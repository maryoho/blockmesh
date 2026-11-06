package metastore

import (
	"errors"
	"fmt"
)

// MetadataRequestSnapshot is a validated, short-lived view of an admitted
// Pool runtime. The kernel request remains in flight until its Completion is
// published, so normal runtime teardown must drain it before closing Store.
type MetadataRequestSnapshot struct {
	Spec VolumeRuntimeSpec
	Metadata *VolumeRuntimeMetadata
	RootPage uint64
	RootGeneration uint64
	OwnerEpoch uint64
	ReaderEpoch uint64
}

type runtimeReaderSnapshotter interface {
	RequestSnapshot() (rootPage, rootGeneration uint64, err error)
}

// ResolveMetadataRequest performs the userspace half of request admission.
// It never treats heartbeat expiry as fencing and never upgrades a RO request
// into an Owner request. Lease-loss fail-stop callbacks run after releasing
// the manager mutex to avoid lock inversion with platform teardown hooks.
func (m *VolumeRuntimeManager) ResolveMetadataRequest(volumeID, nodeID, bootID string,
	ownerEpoch, readerEpoch, rootGeneration uint64) (MetadataRequestSnapshot, error) {
	var lostRuntime *volumeRuntime
	var lostSpec VolumeRuntimeSpec
	var lostErr error
	snapshot, err := func() (MetadataRequestSnapshot, error) {
		var out MetadataRequestSnapshot
		if m == nil { return out, errors.New("nil VolumeRuntimeManager") }
		m.mu.Lock()
		defer m.mu.Unlock()
		rt := m.runtimes[volumeID]
		if rt == nil || !rt.enabled || rt.stopping || rt.ownerLost.Load() ||
			rt.state.current() != OwnerStateRequestsAdmitted || rt.metadata == nil || rt.metadata.Store == nil {
			return out, ErrVolumeRuntimeStopping
		}
		if rt.spec.VolumeID != volumeID || rt.spec.NodeID != nodeID || rt.spec.BootID != bootID {
			return out, ErrFenced
		}
		out.Spec, out.Metadata = rt.spec, rt.metadata
		switch rt.spec.Mode {
		case VolumeRuntimeReadWrite:
			if ownerEpoch == 0 || readerEpoch != 0 { return MetadataRequestSnapshot{}, ErrInvalid }
			if rt.owner == nil || rt.owner.Epoch() != ownerEpoch || rt.metadata.OwnerEpoch != ownerEpoch {
				return MetadataRequestSnapshot{}, ErrStaleEpoch
			}
			if leaseErr := m.checkOwnerLease(rt); leaseErr != nil {
				lostRuntime, lostSpec, lostErr = rt, rt.spec, leaseErr
				return MetadataRequestSnapshot{}, errors.Join(ErrFenced, leaseErr)
			}
			desc, descErr := rt.metadata.Store.PoolDescriptor()
			if descErr != nil { return MetadataRequestSnapshot{}, fmt.Errorf("read durable Pool Volume descriptor: %w", descErr) }
			if desc.OwnerEpoch != ownerEpoch { return MetadataRequestSnapshot{}, ErrStaleEpoch }
			sb := rt.metadata.Store.Superblock()
			if rootGeneration != 0 && rootGeneration != sb.MappingSequence { return MetadataRequestSnapshot{}, ErrStaleEpoch }
			out.RootPage, out.RootGeneration, out.OwnerEpoch = sb.RootPage, sb.MappingSequence, ownerEpoch
		case VolumeRuntimeReadOnly:
			if ownerEpoch != 0 || readerEpoch == 0 || rootGeneration == 0 || readerEpoch != rt.spec.ReaderEpoch {
				return MetadataRequestSnapshot{}, ErrInvalid
			}
			reader, ok := rt.reader.(runtimeReaderSnapshotter)
			if !ok { return MetadataRequestSnapshot{}, ErrFenced }
			root, generation, readerErr := reader.RequestSnapshot()
			if readerErr != nil {
				if errors.Is(readerErr, ErrReaderLeaseLost) || errors.Is(readerErr, ErrReaderIdentity) {
					return MetadataRequestSnapshot{}, errors.Join(ErrFenced, readerErr)
				}
				if errors.Is(readerErr, ErrReaderGeneration) {
					return MetadataRequestSnapshot{}, errors.Join(ErrStaleEpoch, readerErr)
				}
				return MetadataRequestSnapshot{}, readerErr
			}
			if generation != rootGeneration { return MetadataRequestSnapshot{}, ErrStaleEpoch }
			out.RootPage, out.RootGeneration, out.ReaderEpoch = root, generation, readerEpoch
		default:
			return MetadataRequestSnapshot{}, ErrInvalid
		}
		return out, nil
	}()
	if lostErr != nil {
		m.handleOwnerLeaseLoss(lostRuntime, lostSpec, lostErr)
	}
	return snapshot, err
}
