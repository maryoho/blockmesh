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
	if cfg.OpenDevice == nil || cfg.Locker == nil || !cfg.Locker.Distributed() || cfg.WALResourceForAG == nil {
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
	if owner == nil || !owner.Valid() || owner.Epoch() == 0 || owner.VolumeID() != spec.VolumeID ||
		owner.Resource() != "volume:"+spec.VolumeID || !owner.ReservationVerified() ||
		!owner.PreviousOwnerFenced() || !owner.OldOwnerIODrained() {
		if owner != nil { _ = owner.Close() }
		return nil, ErrOwnerProof
	}
	record := owner.OwnerRecord()
	if record.VolumeID != spec.VolumeID || record.NodeID != spec.NodeID || record.BootID != spec.BootID || record.Epoch != owner.Epoch() {
		_ = owner.Close()
		return nil, errors.New("P3 OwnerRecord identity does not match BMPOOL03 activation request")
	}

	parent, err := b.cfg.OpenDevice(ctx, spec)
	if err != nil {
		_ = owner.Close()
		return nil, fmt.Errorf("open parent Pool LUN: %w", err)
	}
	rt := &Runtime{Spec: spec, Parent: parent, Owner: owner, Committers: make(map[uint32]*wal.DynamicSharedCommitter)}
	cleanup := func(cause error) (*Runtime, error) {
		closeErr := rt.closeResources()
		return nil, errors.Join(cause, closeErr)
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

	tree, err := btree.OpenPersistentWithExternalPages(store, volumeCommitter, allocator.ResolveIdempotencyPages)
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
