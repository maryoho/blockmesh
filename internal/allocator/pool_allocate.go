package allocator

import (
	"context"
	"errors"
	"fmt"
	"math"

	"blockmesh/internal/btree"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
	"blockmesh/internal/wal"
)

var (
	ErrAllocationOutcomeUnknown = errors.New("allocation WAL outcome is unknown; recover WAL and reopen the Volume before retrying")
	ErrLogicalRangeMapped       = errors.New("logical allocation range already contains mapped blocks")
	ErrPoolAllocatorUnavailable = errors.New("Pool allocator requires a persistent B+Tree and cluster-safe WAL")
)

// AllocateRequest describes one idempotent contiguous logical allocation.
// Fingerprint must be computed by the caller from the complete canonical
// request (Volume, operation, logical range, size and relevant options).
type AllocateRequest struct {
	IdempotencyKey [16]byte
	Fingerprint    [32]byte
	LogicalStart   uint64
	BlockCount     uint32
	Operation      uint16
}

// AllocateResult is the durable result of an allocation. Replayed is true
// when the idempotency key resolved to an earlier committed allocation.
type AllocateResult struct {
	LogicalStart uint64
	PhysicalStart uint64
	BlockCount uint32
	OwnerEpoch uint64
	Replayed bool
}

// PoolAllocator coordinates Pool bitmap allocation, data zeroing, the
// idempotency collision chain and B+Tree mapping publication. Its lock and WAL
// dependencies must be distributed implementations for shared-SAN use.
type PoolAllocator struct {
	store *metastore.Store
	tree *btree.Tree
	locks lock.DistributedLocker
	data DataDevice
}

func NewPoolAllocator(store *metastore.Store, tree *btree.Tree, locks lock.DistributedLocker, data DataDevice) (*PoolAllocator, error) {
	if store == nil || tree == nil || locks == nil || data == nil || !locks.Distributed() {
		return nil, ErrPoolAllocatorUnavailable
	}
	if _, err := store.PoolSuperblock(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPoolAllocatorUnavailable, err)
	}
	if !tree.UsesPersistentArena() || !tree.UsesClusterSafeCommitter() {
		return nil, ErrPoolAllocatorUnavailable
	}
	return &PoolAllocator{store: store, tree: tree, locks: locks, data: data}, nil
}

// AllocateZeroPublishTxn performs the complete allocation protocol:
// reserve bitmap range -> reserve immutable idempotency page -> zero data and
// flush the data LUN -> atomically publish bitmap, Mapping, idempotency index,
// record page, lifecycle state and Volume root in one AG WAL transaction.
//
// On an ambiguous WAL error the metadata reservation is intentionally kept
// pinned. The caller must fence/stop this Volume, recover its AG WAL, and reopen
// the persistent tree before accepting another allocation.
func (a *PoolAllocator) AllocateZeroPublishTxn(ctx context.Context, ownerLease lock.Lease, req AllocateRequest) (AllocateResult, error) {
	var result AllocateResult
	if a == nil || a.store == nil || a.tree == nil || a.locks == nil || a.data == nil {
		return result, ErrPoolAllocatorUnavailable
	}
	if ctx == nil || ownerLease == nil || !ownerLease.Valid() {
		return result, errors.New("live Volume Owner lease and context are required")
	}
	if req.IdempotencyKey == ([16]byte{}) || req.Fingerprint == ([32]byte{}) ||
		req.BlockCount == 0 || req.Operation == 0 {
		return result, ErrIdempotencyRecord
	}
	if req.LogicalStart >= uint64(1)<<63 ||
		uint64(req.BlockCount) > (uint64(1)<<63)-req.LogicalStart {
		return result, errors.New("logical range overlaps the reserved idempotency key namespace")
	}

	pool, err := a.store.PoolSuperblock()
	if err != nil { return result, err }
	desc, err := a.store.PoolDescriptor()
	if err != nil { return result, err }
	if ownerLease.Resource() != "volume:"+desc.VolumeID || !ownerLease.Valid() {
		return result, errors.New("Owner lease does not match Volume Descriptor")
	}
	if desc.OwnerEpoch == 0 { return result, errors.New("Volume Descriptor has no active OwnerEpoch") }
	epochLease, ok := ownerLease.(interface{ Epoch() uint64 })
	if !ok || epochLease.Epoch() != desc.OwnerEpoch {
		return result, errors.New("Owner lease epoch does not match the durable Volume Descriptor")
	}
	if req.LogicalStart+uint64(req.BlockCount) > pool.TotalBlocks {
		return result, errors.New("logical allocation range exceeds Pool capacity")
	}
	if desc.Generation == math.MaxUint64 || desc.MappingSequence == math.MaxUint64 {
		return result, errors.New("Volume metadata generation exhausted")
	}
	unlock, err := a.locks.Lock(ctx, fmt.Sprintf("ag:%03d", desc.AG))
	if err != nil { return result, fmt.Errorf("acquire AG %d lock: %w", desc.AG, err) }
	defer unlock()
	if err = ctx.Err(); err != nil { return result, err }
	if !ownerLease.Valid() { return result, errors.New("Volume Owner lease lost before allocation") }

	// Idempotency lookup is serialized by the same AG lock as the eventual
	// index insertion, including hash-collision chain extension.
	hash := IdempotencyHash(req.IdempotencyKey)
	head, lookupErr := a.tree.Get(hash)
	if lookupErr == nil {
		record, found, err := LookupIdempotencyRecord(a.store, head, req.IdempotencyKey, req.Fingerprint)
		if err != nil { return result, err }
		if found {
			return AllocateResult{LogicalStart:record.LogicalStart,PhysicalStart:record.PhysicalStart,
				BlockCount:record.BlockCount,OwnerEpoch:record.OwnerEpoch,Replayed:true}, nil
		}
	} else if !errors.Is(lookupErr, btree.ErrKeyNotFound) {
		return result, fmt.Errorf("lookup idempotency index: %w", lookupErr)
	} else {
		head = 0
	}

	// Refuse accidental remapping. A logical address is published exactly once
	// by this allocator; overwrite/COW semantics belong to a separate operation.
	for i:=uint64(0); i<uint64(req.BlockCount); i++ {
		if _, err := a.tree.Get(req.LogicalStart+i); err == nil {
			return result, fmt.Errorf("%w: logical block %d", ErrLogicalRangeMapped, req.LogicalStart+i)
		} else if !errors.Is(err, btree.ErrKeyNotFound) {
			return result, fmt.Errorf("lookup logical block %d: %w", req.LogicalStart+i, err)
		}
	}

	physical, bitmapImages, err := PlanBitmapAllocation(a.store, pool, desc.AG, req.BlockCount)
	if err != nil { return result, fmt.Errorf("plan AG bitmap allocation: %w", err) }
	reservation, ids, err := a.tree.ReserveExternalPages(1)
	if err != nil { return result, fmt.Errorf("reserve idempotency record page: %w", err) }
	if len(ids) != 1 {
		_ = a.tree.AbortReservedArenaPages(reservation)
		return result, metastore.ErrInvalidPageSet
	}
	abortReservation := func(cause error) (AllocateResult,error) {
		if abortErr := a.tree.AbortReservedArenaPages(reservation); abortErr != nil {
			return result, errors.Join(cause, fmt.Errorf("abort unused idempotency reservation: %w", abortErr))
		}
		return result, cause
	}

	// Check the data LUN geometry before touching it. DataStartSector is
	// relative to the underlying device; Pool logical block zero starts there.
	view, err := newPoolDataView(a.data, pool)
	if err != nil { return abortReservation(err) }
	if err = ZeroBlocks(ctx, view, physical, req.BlockCount); err != nil {
		return abortReservation(fmt.Errorf("zero newly allocated data blocks: %w", err))
	}
	if !ownerLease.Valid() {
		return abortReservation(errors.New("Volume Owner lease lost after data zeroing and before metadata publication"))
	}
	if err = ctx.Err(); err != nil { return abortReservation(err) }

	record := IdempotencyRecord{
		NextPage:head, Key:req.IdempotencyKey, Fingerprint:req.Fingerprint,
		LogicalStart:req.LogicalStart, PhysicalStart:physical, BlockCount:req.BlockCount,
		Operation:req.Operation, OwnerEpoch:desc.OwnerEpoch,
	}
	page, err := BuildIdempotencyRecordPage(ids[0], desc.Generation+1, record)
	if err != nil { return abortReservation(err) }
	updates := make([]btree.Item, 0, int(req.BlockCount)+1)
	for i:=uint64(0); i<uint64(req.BlockCount); i++ {
		updates = append(updates, btree.Item{Key:req.LogicalStart+i,Value:physical+i})
	}
	updates = append(updates, btree.Item{Key:hash,Value:ids[0]})
	if err = a.tree.PutBatchWithReservedArenaPages(updates,reservation,[]metastore.Page{page},bitmapImages); err != nil {
		if isKnownPreCommitAllocationError(err) {
			return result, fmt.Errorf("publish allocation transaction: %w", err)
		}
		return result, fmt.Errorf("%w: publish allocation transaction: %v", ErrAllocationOutcomeUnknown, err)
	}
	return AllocateResult{LogicalStart:req.LogicalStart,PhysicalStart:physical,
		BlockCount:req.BlockCount,OwnerEpoch:desc.OwnerEpoch}, nil
}

func isKnownPreCommitAllocationError(err error) bool {
	return errors.Is(err,wal.ErrJournalFull) ||
		errors.Is(err,metastore.ErrRetireQueueFull) ||
		errors.Is(err,metastore.ErrMetadataTransactionTooLarge) ||
		errors.Is(err,metastore.ErrInvalidPageSet) ||
		errors.Is(err,metastore.ErrArenaFull) ||
		errors.Is(err,btree.ErrCorruptTree)
}

// poolDataView translates Pool-relative byte offsets into the backing data
// device's DataStartSector-relative range.
type poolDataView struct {
	dev DataDevice
	base uint64
	size int64
}

func newPoolDataView(dev DataDevice, pool format.Superblock) (*poolDataView,error) {
	if dev==nil || pool.DataStartSector > math.MaxUint64/512 { return nil,errors.New("invalid Pool data device geometry") }
	base:=pool.DataStartSector*512
	if dev.Size()<0 || base>uint64(dev.Size()) || pool.PoolSize>uint64(dev.Size())-base {
		return nil,fmt.Errorf("Pool data range [%d,%d) exceeds data device size %d",base,base+pool.PoolSize,dev.Size())
	}
	return &poolDataView{dev:dev,base:base,size:int64(pool.PoolSize)},nil
}
func (v *poolDataView) WriteAt(p []byte, off int64)(int,error) {
	if off<0 || uint64(off)>uint64(v.size) || uint64(len(p))>uint64(v.size)-uint64(off) {
		return 0,errors.New("Pool-relative data write outside Pool")
	}
	if v.base>math.MaxUint64-uint64(off) || v.base+uint64(off)>math.MaxInt64 {
		return 0,errors.New("Pool data offset overflows int64")
	}
	return v.dev.WriteAt(p,int64(v.base+uint64(off)))
}
func (v *poolDataView) Sync() error { return v.dev.Sync() }
func (v *poolDataView) Size() int64 { return v.size }

