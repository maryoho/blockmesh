package metaservice

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"blockmesh/internal/btree"
	"blockmesh/internal/metastore"
	"blockmesh/internal/uapi"
)

var (
	ErrMutationPathUnavailable = errors.New("Pool mutation transaction backend is unavailable")
	ErrNonContiguousMapping = errors.New("logical range is not backed by one contiguous physical extent")
)

// PoolMutationExecutor is the transaction boundary for data allocation and
// discard. Implementations must use the SAN-backed Pool bitmap and persistent
// idempotency records; in-memory deduplication is not sufficient.
//
// AllocateZeroPublish contract:
//   - reserve physical blocks durably under the current OwnerEpoch;
//   - make retries with the same key and identical request return the same
//     result, and reject reuse of that key with a different request;
//   - zero every newly allocated data block and make the zeroing durable
//     before publishing any logical mapping;
//   - atomically publish the bitmap/mapping/root/idempotency after-images, or
//     retain a safe durable reservation that recovery can finish or reclaim.
//
// Discard contract:
//   - remove the logical mapping durably and record the physical blocks in a
//     persistent retire queue;
//   - do not free or physically discard those blocks until Reader Generation,
//     SAN fencing and metadata-I/O drain proofs permit reclamation.
//
// The current P3 APIs do not yet provide this combined data-bitmap,
// idempotency and mapping transaction. A nil executor therefore fails closed.
type PoolMutationExecutor interface {
	AllocateZeroPublish(context.Context, metastore.MetadataRequestSnapshot, uapi.Request) (uapi.Mapping, error)
	Discard(context.Context, metastore.MetadataRequestSnapshot, uapi.Request) error
}

type PoolRuntimeProcessor struct {
	manager *metastore.VolumeRuntimeManager
	mutations PoolMutationExecutor
	mu sync.Mutex
	volumes map[string]*poolTreeState
}

type poolTreeState struct {
	mu sync.Mutex
	tree *btree.Tree
	rootPage uint64
	rootGeneration uint64
	metadata *metastore.VolumeRuntimeMetadata
}

func NewPoolRuntimeProcessor(manager *metastore.VolumeRuntimeManager, mutations PoolMutationExecutor) (*PoolRuntimeProcessor, error) {
	if manager == nil { return nil, errors.New("Pool VolumeRuntimeManager is required") }
	return &PoolRuntimeProcessor{manager: manager, mutations: mutations, volumes: make(map[string]*poolTreeState)}, nil
}

func uuidText(v [16]byte) string {
	s := hex.EncodeToString(v[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", s[:8], s[8:12], s[12:16], s[16:20], s[20:])
}

func (p *PoolRuntimeProcessor) state(volumeID string) *poolTreeState {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.volumes[volumeID]
	if s == nil { s = &poolTreeState{}; p.volumes[volumeID] = s }
	return s
}

func (p *PoolRuntimeProcessor) openTree(s *poolTreeState, snap metastore.MetadataRequestSnapshot) error {
	meta := snap.Metadata
	if meta == nil || meta.Store == nil || meta.Arena == nil || meta.Committer == nil {
		return errors.New("Pool runtime must expose Store, shared PersistentArena and cluster-safe WAL committer")
	}
	if safe, ok := meta.Committer.(metastore.ClusterSafeCommitter); !ok || !safe.ClusterSafe() {
		return metastore.ErrClusterSafeCommitterRequired
	}
	tree, err := btree.OpenPersistentAtRoot(meta.Store, meta.Committer, meta.Arena, snap.RootPage)
	if err != nil { return fmt.Errorf("open pinned Pool mapping root: %w", err) }
	s.tree, s.rootPage, s.rootGeneration, s.metadata = tree, snap.RootPage, snap.RootGeneration, meta
	return nil
}

func (p *PoolRuntimeProcessor) Process(ctx context.Context, req uapi.Request) (Result, error) {
	if p == nil || p.manager == nil { return Result{}, ErrFenced }
	if err := ctx.Err(); err != nil { return Result{}, err }
	if err := req.Validate(); err != nil { return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err) }
	volumeID, nodeID, bootID := uuidText(req.VolumeID), uuidText(req.NodeID), uuidText(req.BootID)
	snap, err := p.manager.ResolveMetadataRequest(volumeID, nodeID, bootID,
		req.OwnerEpoch, req.ReaderEpoch, req.RootGeneration)
	if err != nil {
		switch {
		case errors.Is(err, metastore.ErrRuntimeRequestFenced), errors.Is(err, metastore.ErrVolumeRuntimeStopping):
			return Result{}, errors.Join(ErrFenced, err)
		case errors.Is(err, metastore.ErrRuntimeRequestStaleEpoch):
			return Result{}, errors.Join(ErrStaleEpoch, err)
		case errors.Is(err, metastore.ErrRuntimeRequestInvalid):
			return Result{}, errors.Join(ErrInvalid, err)
		default:
			return Result{}, err
		}
	}
	state := p.state(volumeID)
	state.mu.Lock()
	defer state.mu.Unlock()
	switch req.Operation {
	case uapi.OpLookupBatch:
		if state.tree == nil || state.rootPage != snap.RootPage ||
			state.rootGeneration != snap.RootGeneration || state.metadata != snap.Metadata {
			if err = p.openTree(state, snap); err != nil { return Result{}, err }
		}
		mappings, lookupErr := lookupMappingVector(state.tree, req.LogicalStart, req.BlockCount)
		if lookupErr != nil { return Result{}, lookupErr }
		return Result{Mappings: mappings}, nil

	case uapi.OpAllocateZeroPublish:
		if snap.Spec.Mode != metastore.VolumeRuntimeReadWrite || snap.OwnerEpoch != req.OwnerEpoch {
			return Result{}, ErrFenced
		}
		if p.mutations == nil { return Result{}, errors.Join(ErrRetry, ErrMutationPathUnavailable) }
		mapping, mutationErr := p.mutations.AllocateZeroPublish(ctx, snap, req)
		if mutationErr != nil { return Result{}, mutationErr }
		if mapping.LogicalBlock != req.LogicalStart || mapping.BlockCount != req.BlockCount {
			return Result{}, fmt.Errorf("%w: allocator returned mapping for a different logical range", ErrInvalid)
		}
		if _, encodeErr := mapping.MarshalBinary(); encodeErr != nil { return Result{}, fmt.Errorf("%w: %v", ErrInvalid, encodeErr) }
		// The transaction backend may have published a new COW root.
		if err = snap.Metadata.Store.Refresh(); err != nil { return Result{}, err }
		state.tree = nil
		return Result{Mapping: &mapping}, nil

	case uapi.OpDiscard:
		if snap.Spec.Mode != metastore.VolumeRuntimeReadWrite || snap.OwnerEpoch != req.OwnerEpoch {
			return Result{}, ErrFenced
		}
		if p.mutations == nil { return Result{}, errors.Join(ErrRetry, ErrMutationPathUnavailable) }
		if err = p.mutations.Discard(ctx, snap, req); err != nil { return Result{}, err }
		if err = snap.Metadata.Store.Refresh(); err != nil { return Result{}, err }
		state.tree = nil
		return Result{}, nil

	case uapi.OpFlush:
		if snap.Spec.Mode != metastore.VolumeRuntimeReadWrite || snap.OwnerEpoch != req.OwnerEpoch {
			return Result{}, ErrFenced
		}
		// Every WAL-backed metadata operation is already durable on successful
		// CommitImages return. Sync additionally orders pending device writes
		// before acknowledging the DM FLUSH.
		if err = snap.Metadata.Store.Sync(); err != nil { return Result{}, fmt.Errorf("flush Pool metadata device: %w", err) }
		return Result{}, nil
	default:
		return Result{}, ErrInvalid
	}
}

func lookupMappingVector(tree interface{ Get(uint64) (uint64, error) }, logical uint64, count uint32) ([]uapi.Mapping, error) {
	if tree == nil || count == 0 || count > uapi.MaxBatch || logical > ^uint64(0)-uint64(count) { return nil, ErrInvalid }
	out := make([]uapi.Mapping, 0, count)
	for i := uint32(0); i < count; i++ {
		l := logical + uint64(i)
		physical, err := tree.Get(l)
		if errors.Is(err, btree.ErrKeyNotFound) { return nil, ErrUnmapped }
		if err != nil { return nil, err }
		if n := len(out); n > 0 && out[n-1].PhysicalBlock+uint64(out[n-1].BlockCount) == physical {
			out[n-1].BlockCount++
		} else {
			out = append(out, uapi.Mapping{LogicalBlock:l, PhysicalBlock:physical, BlockCount:1})
		}
	}
	if _, err := uapi.MarshalMappings(out); err != nil { return nil, err }
	return out, nil
}

func lookupContiguous(tree interface{ Get(uint64) (uint64, error) }, logical uint64, count uint32) (uapi.Mapping, error) {
	if tree == nil || count == 0 || count > uapi.MaxBatch { return uapi.Mapping{}, ErrInvalid }
	var first uint64
	for i := uint32(0); i < count; i++ {
		physical, err := tree.Get(logical + uint64(i))
		if errors.Is(err, btree.ErrKeyNotFound) { return uapi.Mapping{}, ErrUnmapped }
		if err != nil { return uapi.Mapping{}, err }
		if i == 0 { first = physical; continue }
		if physical != first+uint64(i) { return uapi.Mapping{}, errors.Join(ErrRetry, ErrNonContiguousMapping) }
	}
	m := uapi.Mapping{LogicalBlock: logical, PhysicalBlock: first, BlockCount: count}
	if _, err := m.MarshalBinary(); err != nil { return uapi.Mapping{}, fmt.Errorf("%w: %v", ErrInvalid, err) }
	return m, nil
}

var _ Processor = (*PoolRuntimeProcessor)(nil)
