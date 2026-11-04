package metastore

import (
	"context"
	"errors"
	"fmt"

	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
)

// ReaderLeaseProbe must acquire the exact exclusive sanlock resource held by
// the reader process. Success proves the old resource is no longer held and
// keeps it reserved while SAN fencing and the durable Fenced transition run.
type ReaderLeaseProbe interface {
	AcquireExpiredReader(context.Context, string, ReaderRecord) (release func() error, err error)
}

type SanlockReaderLeaseProbe struct {
	Locker *lock.SanlockLocker
	ResourceFor func(volumeID string, reader ReaderRecord) string
}

func (p *SanlockReaderLeaseProbe) AcquireExpiredReader(ctx context.Context, volumeID string, reader ReaderRecord) (func() error, error) {
	if p == nil || p.Locker == nil || p.ResourceFor == nil {
		return nil, errors.New("sanlock reader lease probe is not configured")
	}
	resource := p.ResourceFor(volumeID, reader)
	if resource == "" { return nil, errors.New("reader lease resource is empty") }
	lease, err := p.Locker.Acquire(ctx, resource)
	if err != nil { return nil, fmt.Errorf("reader lease is still held or cannot be proven free: %w", err) }
	return lease.Close, nil
}

// FenceExpiredReader is the recovery-controller path. It first acquires the
// same exclusive sanlock resource as the reader (which must succeed only after
// the reader lease is gone), keeps that probe lease held, performs SCSI PR
// fencing, and only then persists ReaderFenced. All steps are serialized
// against registration, root publication, and reclamation by ReaderCoordinator.
func (t *ReaderTable) FenceExpiredReader(ctx context.Context, slot uint32, nodeID, bootID string, epoch uint64,
	probe ReaderLeaseProbe, fencer fence.Fencer) error {
	if probe == nil || fencer == nil { return ErrReaderNotFenced }
	if t.coordinator == nil { return ErrReaderCoordinatorRequired }
	unlock, err := t.lockReaderSet()
	if err != nil { return err }
	defer unlock()

	t.mu.Lock()
	reader, err := t.readLocked(slot)
	t.mu.Unlock()
	if err != nil { return err }
	if reader.State != ReaderActive && reader.State != ReaderLeaving { return ErrReaderIdentity }
	if reader.NodeID != nodeID || reader.BootID != bootID || reader.Epoch != epoch { return ErrReaderIdentity }

	releaseProbe, err := probe.AcquireExpiredReader(ctx, t.store.Superblock().VolumeID, reader)
	if err != nil { return err }
	defer func() { _ = releaseProbe() }()

	if err = fencer.FenceNode(ctx, nodeID, bootID); err != nil {
		return fmt.Errorf("fence expired reader %s/%s: %w", nodeID, bootID, err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	current, err := t.readLocked(slot)
	if err != nil { return err }
	if current.NodeID != nodeID || current.BootID != bootID || current.Epoch != epoch ||
		(current.State != ReaderActive && current.State != ReaderLeaving) {
		return ErrReaderIdentity
	}
	current.State = ReaderFenced
	return t.writeLocked(current)
}
