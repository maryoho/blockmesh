// Package dynamicruntime wires the BMPOOL03 on-disk objects into a live
// read-write runtime. It deliberately does not use the BMPOOL02 runtime backend.
package dynamicruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"blockmesh/internal/allocator"
	"blockmesh/internal/btree"
	"blockmesh/internal/device"
	"blockmesh/internal/fence"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/volume"
	"blockmesh/internal/wal"
)

var (
	ErrInvalidConfig = errors.New("invalid BMPOOL03 runtime backend configuration")
	ErrOwnerProof = errors.New("BMPOOL03 runtime requires a live, fenced SAN Owner lease")
	ErrViewMismatch = errors.New("BMPOOL03 MetadataView/DataView do not match one Bootstrap generation")
)

// OwnerLease is the proof-bearing lease returned by the P3 OwnerCoordinator.
// The lease must already hold the exclusive distributed Owner resource and
// have completed SCSI PR verification, previous-owner fencing and I/O drain.
type OwnerLease interface {
	lock.Lease
	Epoch() uint64
	VolumeID() string
	Valid() bool
	OwnerRecord() volume.OwnerRecord
	ReservationVerified() bool
	PreviousOwnerFenced() bool
	OldOwnerIODrained() bool
}

// Config contains only deployment-specific dependencies. The device opener
// must return the parent LUN (multipath, direct iSCSI or local block device),
// never a pre-sliced Metadata or Data device.
type Config struct {
	OpenDevice func(context.Context, metastore.VolumeRuntimeSpec) (device.Device, error)
	Locker lock.DistributedLocker
	WALResourceForAG func(poolUUID [16]byte, ag uint32) string
	ReaderLease volume.ReaderLeaseConfig
	ReaderRegistrar fence.ReaderRegistrar
	ReaderCoordinator metastore.ReaderCoordinator
}

// Runtime owns every object opened from one Bootstrap Extent Map generation.
// Close releases the lease only after the logical views have been closed.
type Runtime struct {
	Spec metastore.VolumeRuntimeSpec
	Parent device.Device
	Bootstrap *poolspace.BootstrapStore
	Metadata *poolspace.MetadataView
	Data *poolspace.DataView
	Superblock format.DynamicSuperblock
	Store *metastore.Store
	Descriptor format.DynamicVolumeDescriptor
	RootGeneration uint64
	Committers map[uint32]*wal.DynamicSharedCommitter
	Committer *wal.DynamicSharedCommitter
	Tree *btree.Tree
	Allocator *allocator.PoolAllocator
	Owner OwnerLease
	closeMu sync.Mutex
	closed bool
}

type Backend struct { cfg Config }

func New(cfg Config) (*Backend, error) {
	if cfg.OpenDevice == nil || cfg.Locker == nil || !cfg.Locker.Distributed() || cfg.WALResourceForAG == nil || cfg.ReaderCoordinator == nil {
		return nil, ErrInvalidConfig
	}
	return &Backend{cfg: cfg}, nil
}

// OpenReadWrite transfers ownership of owner to the returned Runtime. On any
// error it closes both the lease and parent device. The owner must be acquired
// by the P3 OwnerCoordinator before calling this method.
func (b *Backend) OpenReadWrite(ctx context.Context, spec metastore.VolumeRuntimeSpec, owner OwnerLease) (_ *Runtime, retErr error) {
	if b == nil || ctx == nil || b.cfg.OpenDevice == nil || b.cfg.Locker == nil || !b.cfg.Locker.Distributed() {
		return nil, ErrInvalidConfig
	}
	if spec.Mode != metastore.VolumeRuntimeReadWrite || spec.VolumeID == "" || spec.NodeID == "" || spec.BootID == "" {
		return nil, errors.New("BMPOOL03 RW activation requires Volume, Node, Boot and RW mode")
	}
	closeOwnerOnError := func(cause error) (*Runtime,error) {
		if owner==nil { return nil,cause }
		if closeErr:=owner.Close();closeErr!=nil {
			return &Runtime{Spec:spec,Owner:owner},errors.Join(cause,fmt.Errorf("retain Owner lease after failed close: %w",closeErr))
		}
		return nil,cause
	}
	if owner == nil || !owner.Valid() || owner.Epoch() == 0 || owner.VolumeID() != spec.VolumeID ||
		owner.Resource() != "volume:"+spec.VolumeID || !owner.ReservationVerified() ||
		!owner.PreviousOwnerFenced() || !owner.OldOwnerIODrained() {
		return closeOwnerOnError(ErrOwnerProof)
	}
	record := owner.OwnerRecord()
	if record.VolumeID != spec.VolumeID || record.NodeID != spec.NodeID || record.BootID != spec.BootID || record.Epoch != owner.Epoch() {
		return closeOwnerOnError(errors.New("P3 OwnerRecord identity does not match BMPOOL03 activation request"))
	}

	parent, err := b.cfg.OpenDevice(ctx, spec)
	if err != nil {
		return closeOwnerOnError(fmt.Errorf("open parent Pool LUN: %w", err))
	}
	rt := &Runtime{Spec: spec, Parent: parent, Owner: owner, Committers: make(map[uint32]*wal.DynamicSharedCommitter)}
	cleanup := func(cause error) (*Runtime, error) {
		closeErr := rt.closeResources()
		if closeErr != nil { return rt, errors.Join(cause, closeErr) }
		return nil, cause
	}
	if err := ctx.Err(); err != nil { return cleanup(err) }

	bootstrap, err := poolspace.OpenBootstrap(parent)
	if err != nil { return cleanup(fmt.Errorf("open BMPOOL03 Bootstrap: %w", err)) }
	rt.Bootstrap = bootstrap
	if err = bootstrap.SetCommitLocker(b.cfg.Locker); err != nil { return cleanup(err) }
	if err = bootstrap.SetOwnerLease(owner); err != nil { return cleanup(fmt.Errorf("bind Bootstrap Owner lease: %w", err)) }
	bootstrapRecord := bootstrap.Record()
	if bootstrapRecord.PoolUUID == ([16]byte{}) || bootstrapRecord.OwnerEpoch > owner.Epoch() {
		return cleanup(errors.New("Bootstrap Pool identity or OwnerEpoch is invalid"))
	}

	metadata, data, viewGeneration, err := bootstrap.OpenViews()
	if err != nil { return cleanup(fmt.Errorf("open BMPOOL03 logical views: %w", err)) }
	rt.Metadata, rt.Data = metadata, data
	if metadata == nil || data == nil || metadata.Generation() != viewGeneration ||
		data.Generation() != viewGeneration || metadata.Generation() != data.Generation() {
		return cleanup(ErrViewMismatch)
	}
	if viewGeneration != bootstrapRecord.Generation {
		return cleanup(fmt.Errorf("%w: Bootstrap=%d views=%d", ErrViewMismatch, bootstrapRecord.Generation, viewGeneration))
	}

	sb, err := allocator.OpenDynamicSuperblockPair(metadata)
	if err != nil { return cleanup(fmt.Errorf("open BMPOOL03 superblock pair: %w", err)) }
	if sb.PoolUUID != bootstrapRecord.PoolUUID {
		return cleanup(errors.New("BMPOOL03 Superblock Pool UUID differs from Bootstrap Pool UUID"))
	}
	if sb.ViewGeneration != viewGeneration {
		return cleanup(fmt.Errorf("%w: Superblock=%d views=%d", ErrViewMismatch, sb.ViewGeneration, viewGeneration))
	}
	if uint64(metadata.Size()) < sb.Geometry.MetadataBytes || uint64(data.Size()) < sb.Geometry.DataBytes {
		return cleanup(errors.New("BMPOOL03 geometry exceeds its logical MetadataView or DataView"))
	}
	if uint64(parent.Size()) != bootstrapRecord.DeviceBytes {
		return cleanup(errors.New("parent LUN capacity differs from persisted Bootstrap device capacity"))
	}
	rt.Superblock = sb

	store, err := metastore.OpenDynamicPoolVolume(metadata, sb, spec.VolumeIndex, spec.VolumeID)
	if err != nil { return cleanup(fmt.Errorf("open BMDVOL03 Volume Descriptor: %w", err)) }
	rt.Store = store
	desc, err := store.DynamicPoolDescriptor()
	if err != nil { return cleanup(err) }
	if desc.OwnerEpoch == 0 || owner.Epoch() <= desc.OwnerEpoch {
		return cleanup(fmt.Errorf("new OwnerEpoch %d must exceed durable BMDVOL03 OwnerEpoch %d", owner.Epoch(), desc.OwnerEpoch))
	}
	if !owner.Valid() { return cleanup(ErrOwnerProof) }

	// Recover every AG journal before changing the descriptor OwnerEpoch or
	// opening the mutable B+Tree. Each committer is fenced by the acquired P3
	// lease; descriptor-epoch comparison is enabled immediately after publish.
	for ag := uint32(0); ag < sb.Geometry.AGCount; ag++ {
		resource := b.cfg.WALResourceForAG(sb.PoolUUID, ag)
		if resource == "" { return cleanup(fmt.Errorf("empty distributed WAL resource for AG %d", ag)) }
		committer, e := wal.NewDynamicSharedCommitter(metadata, sb, ag, b.cfg.Locker, resource)
		if e != nil { return cleanup(fmt.Errorf("create AG %d dynamic WAL committer: %w", ag, e)) }
		if e = wal.GuardDynamicSharedCommitter(committer, owner, spec.VolumeID, nil); e != nil {
			return cleanup(fmt.Errorf("guard AG %d dynamic WAL committer: %w", ag, e))
		}
		if e = committer.Recover(); e != nil { return cleanup(fmt.Errorf("recover BMPOOL03 AG %d WAL: %w", ag, e)) }
		rt.Committers[ag] = committer
	}
	if !owner.Valid() { return cleanup(ErrOwnerProof) }

	volumeCommitter := rt.Committers[desc.AG]
	if volumeCommitter == nil { return cleanup(fmt.Errorf("BMDVOL03 AG %d has no recovered WAL committer", desc.AG)) }
	if err = store.AdvanceOwnerEpoch(owner.Epoch(), volumeCommitter); err != nil {
		return cleanup(fmt.Errorf("durably publish BMDVOL03 OwnerEpoch: %w", err))
	}
	epochReader, err := metastore.NewDynamicOwnerEpochReader(metadata, sb, spec.VolumeIndex, spec.VolumeID)
	if err != nil { return cleanup(err) }
	readEpoch := epochReader.ReadOwnerEpoch
	for ag, committer := range rt.Committers {
		if err = wal.GuardDynamicSharedCommitter(committer, owner, spec.VolumeID, readEpoch); err != nil {
			return cleanup(fmt.Errorf("install descriptor OwnerEpoch guard for AG %d: %w", ag, err))
		}
	}
	if !owner.Valid() { return cleanup(ErrOwnerProof) }

	tree, err := btree.OpenPersistentWithExternalPages(store, volumeCommitter, allocator.ResolveIdempotencyPages, b.cfg.ReaderCoordinator)
	if err != nil { return cleanup(fmt.Errorf("open persistent BMPOOL03 B+Tree/Arena: %w", err)) }
	rt.Tree = tree
	poolAllocator, err := allocator.NewDynamicPoolAllocator(store, tree, b.cfg.Locker, data)
	if err != nil { return cleanup(fmt.Errorf("open BMPOOL03 PoolAllocator: %w", err)) }
	rt.Allocator = poolAllocator
	rt.Committer = volumeCommitter
	rt.Descriptor, err = store.DynamicPoolDescriptor()
	if err != nil { return cleanup(err) }
	if rt.Descriptor.OwnerEpoch != owner.Epoch() || !owner.Valid() {
		return cleanup(errors.New("BMDVOL03 OwnerEpoch publication did not match the live fenced Owner lease"))
	}
	return rt, nil
}

// OpenReadOnly opens BMPOOL03 metadata without acquiring the exclusive
// Owner resource or replaying WAL. A live Reader session pins the committed
// MappingSequence before the Runtime is returned to the manager.
func (b *Backend) OpenReadOnly(ctx context.Context, spec metastore.VolumeRuntimeSpec,
	onLeaseLost func(error)) (_ *Runtime, _ ReadOnlySession, retErr error) {
	if b == nil || ctx == nil || spec.Mode != metastore.VolumeRuntimeReadOnly ||
		spec.VolumeID == "" || spec.NodeID == "" || spec.BootID == "" ||
		spec.ReaderEpoch == 0 || onLeaseLost == nil {
		return nil, nil, errors.New("BMPOOL03 RO activation requires backend, context, identity, ReaderEpoch and fail-stop callback")
	}
	if b.cfg.ReaderRegistrar == nil || b.cfg.ReaderCoordinator == nil {
		return nil, nil, errors.New("BMPOOL03 RO activation requires SCSI PR ReaderRegistrar and distributed ReaderCoordinator")
	}
	leaseConfig := b.cfg.ReaderLease
	sanlock, ok := b.cfg.Locker.(*lock.SanlockLocker)
	if !ok || sanlock == nil {
		return nil, nil, errors.New("BMPOOL03 Reader lease requires the configured distributed SanlockLocker")
	}
	if leaseConfig.Locker != nil && leaseConfig.Locker != sanlock {
		return nil, nil, errors.New("BMPOOL03 Reader and WAL/Owner paths must use the same SanlockLocker instance")
	}
	leaseConfig.Locker = sanlock
	if leaseConfig.ResourceFor == nil {
		return nil, nil, errors.New("BMPOOL03 Reader lease resource resolver is required")
	}

	parent, err := b.cfg.OpenDevice(ctx, spec)
	if err != nil { return nil, nil, fmt.Errorf("open parent Pool LUN for RO: %w", err) }
	rt := &Runtime{Spec: spec, Parent: parent, Committers: make(map[uint32]*wal.DynamicSharedCommitter)}
	fail := func(cause error, reader *metastore.ReaderSession) (*Runtime, ReadOnlySession, error) {
		if reader != nil {
			if e := reader.CloseDrained(context.WithoutCancel(ctx), true); e != nil {
				// Keep the parent LUN and both views pinned until the durable
				// Reader slot, local PR registrations and sanlock lease are closed.
				return rt, reader, errors.Join(cause, fmt.Errorf("close partially joined Reader session: %w", e))
			}
		}
		if e := rt.Close(); e != nil {
			return rt, nil, errors.Join(cause, fmt.Errorf("close partial BMPOOL03 RO Runtime: %w", e))
		}
		return nil, nil, cause
	}
	if err := ctx.Err(); err != nil { return fail(err, nil) }
	bootstrap, err := poolspace.OpenBootstrap(parent)
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 Bootstrap for RO: %w", err), nil) }
	rt.Bootstrap = bootstrap
	record := bootstrap.Record()
	if record.PoolUUID == ([16]byte{}) || uint64(parent.Size()) != record.DeviceBytes {
		return fail(errors.New("BMPOOL03 RO Bootstrap identity or device capacity is invalid"), nil)
	}
	metadata, data, generation, err := bootstrap.OpenViews()
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 RO views: %w", err), nil) }
	rt.Metadata, rt.Data = metadata, data
	if metadata == nil || data == nil || metadata.Generation() != generation ||
		data.Generation() != generation || generation != record.Generation {
		return fail(ErrViewMismatch, nil)
	}
	sb, err := allocator.OpenDynamicSuperblockPair(metadata)
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 RO Superblock: %w", err), nil) }
	if sb.PoolUUID != record.PoolUUID || sb.ViewGeneration != generation ||
		uint64(metadata.Size()) < sb.Geometry.MetadataBytes || uint64(data.Size()) < sb.Geometry.DataBytes {
		return fail(errors.New("BMPOOL03 RO Superblock, views and Bootstrap geometry disagree"), nil)
	}
	rt.Superblock = sb
	store, err := metastore.OpenDynamicPoolVolume(metadata, sb, spec.VolumeIndex, spec.VolumeID)
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 RO Volume Descriptor: %w", err), nil) }
	rt.Store = store
	desc, err := store.DynamicPoolDescriptor()
	if err != nil { return fail(err, nil) }
	if desc.OwnerEpoch == 0 { return fail(errors.New("BMPOOL03 RO Volume has no published OwnerEpoch"), nil) }

	resource := b.cfg.WALResourceForAG(sb.PoolUUID, desc.AG)
	if resource == "" { return fail(errors.New("BMPOOL03 RO AG WAL resource is empty"), nil) }
	committer, err := wal.NewDynamicSharedCommitter(metadata, sb, desc.AG, b.cfg.Locker, resource)
	if err != nil { return fail(fmt.Errorf("create BMPOOL03 RO Reader Table committer: %w", err), nil) }
	rt.Committers[desc.AG] = committer
	rt.Committer = committer
	// A read-only node may be the first process to touch the AG after an
	// interrupted writer. OpenDynamicJournal performs committed-redo recovery;
	// do this under the distributed AG WAL lock before snapshotting/registering
	// the Reader generation.
	if err = committer.Recover(); err != nil {
		return fail(fmt.Errorf("recover BMPOOL03 AG %d WAL before RO registration: %w", desc.AG, err), nil)
	}
	if err = store.Refresh(); err != nil { return fail(fmt.Errorf("refresh BMPOOL03 Volume after WAL recovery: %w", err), nil) }
	desc, err = store.DynamicPoolDescriptor()
	if err != nil { return fail(err, nil) }
	if desc.OwnerEpoch == 0 { return fail(errors.New("BMPOOL03 RO Volume OwnerEpoch is zero after WAL recovery"), nil) }
	start, pages, err := store.ReaderTableRange()
	if err != nil { return fail(fmt.Errorf("resolve BMPOOL03 Reader Table range: %w", err), nil) }
	readerTable, err := metastore.NewReaderTable(store, start, pages, committer, b.cfg.ReaderCoordinator)
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 Reader Table: %w", err), nil) }
	reader, err := metastore.JoinReaderSession(ctx, readerTable, leaseConfig, b.cfg.ReaderRegistrar,
		spec.ReaderSlot, spec.NodeID, spec.BootID, spec.ReaderEpoch, onLeaseLost)
	if err != nil { return fail(fmt.Errorf("join BMPOOL03 Reader session: %w", err), nil) }

	tree, err := btree.OpenPersistentWithExternalPages(store, committer, allocator.ResolveIdempotencyPages, b.cfg.ReaderCoordinator)
	if err != nil { return fail(fmt.Errorf("open BMPOOL03 read-only B+Tree: %w", err), reader) }
	rt.Tree = tree
	// Registration may race a concurrent root publication. Bring the loaded
	// tree and durable Reader pin to the same generation before returning.
	for {
		if err := ctx.Err(); err != nil { return fail(err, reader) }
		treeGeneration := tree.RootGeneration()
		readerGeneration := reader.RootGeneration()
		if treeGeneration < readerGeneration {
			if err := tree.Refresh(); err != nil { return fail(fmt.Errorf("refresh BMPOOL03 RO tree to Reader generation: %w", err), reader) }
			continue
		}
		if treeGeneration == readerGeneration { break }
		if err := reader.AdvanceGeneration(treeGeneration, true); err != nil {
			if errors.Is(err, metastore.ErrReaderGeneration) { 
				if refreshErr := tree.Refresh(); refreshErr != nil { return fail(refreshErr, reader) }
				continue
			}
			return fail(fmt.Errorf("advance BMPOOL03 Reader pin to loaded tree generation: %w", err), reader)
		}
	}
	rt.Descriptor, err = store.DynamicPoolDescriptor()
	if err != nil { return fail(err, reader) }
	rt.RootGeneration = reader.RootGeneration()
	if rt.RootGeneration != tree.RootGeneration() { return fail(errors.New("BMPOOL03 RO Tree and Reader pin generation diverged"), reader) }
	return rt, reader, nil
}

// ReadOnlyOpener binds Reader sanlock lease loss directly to the supplied
// RequestGate. The callback must synchronously close and drain the DM read gate.
func (b *Backend) ReadOnlyOpener(gate RequestGate) ReadOnlyRuntimeOpener {
	return func(ctx context.Context, spec metastore.VolumeRuntimeSpec) (*Runtime, ReadOnlySession, error) {
		if gate == nil { return nil, nil, errors.New("BMPOOL03 RO RequestGate is required") }
		return b.OpenReadOnly(ctx, spec, func(cause error) {
			_ = gate.FailStop(context.Background(), spec, cause)
		})
	}
}

func (r *Runtime) closeResources() error {
	if r == nil { return nil }
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed { return nil }

	// Keep the parent LUN and Owner lease pinned until both logical views have
	// closed successfully. Stop at the first error so a caller can retry the
	// exact unsafe boundary without releasing resources underneath live views.
	if r.Metadata != nil {
		if err := r.Metadata.Close(); err != nil {
			return fmt.Errorf("close BMPOOL03 MetadataView: %w", err)
		}
		r.Metadata = nil
	}
	if r.Data != nil {
		if err := r.Data.Close(); err != nil {
			return fmt.Errorf("close BMPOOL03 DataView: %w", err)
		}
		r.Data = nil
	}
	if r.Owner != nil {
		if err := r.Owner.Close(); err != nil {
			return fmt.Errorf("release BMPOOL03 Owner lease: %w", err)
		}
		r.Owner = nil
	}
	if r.Parent != nil {
		if err := r.Parent.Close(); err != nil {
			return fmt.Errorf("close BMPOOL03 parent LUN: %w", err)
		}
		r.Parent = nil
	}
	r.closed = true
	return nil
}

func (r *Runtime) Close() error { return r.closeResources() }

var _ io.Closer = (*Runtime)(nil)
