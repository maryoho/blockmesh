package metastore

import (
	"context"
	"errors"
	"sync"

	"blockmesh/internal/volume"
)

var ErrReaderLeaseLost = errors.New("reader sanlock lease is no longer valid")

// ReaderSession couples the durable Reader Table slot to a live sanlock
// resource lease. The caller must wire onLeaseLost to stop new I/O and suspend
// the local data path; this callback is deliberately mandatory.
type ReaderSession struct {
	table *ReaderTable
	lease *volume.ReaderLease
	slot uint32
	nodeID string
	bootID string
	epoch uint64
	mu sync.Mutex
	rootGeneration uint64
	closed bool
}

func JoinReaderSession(ctx context.Context, table *ReaderTable, leaseConfig volume.ReaderLeaseConfig,
	slot uint32, nodeID, bootID string, epoch uint64, onLeaseLost func(error)) (*ReaderSession, error) {
	if table == nil { return nil, errors.New("reader table is required") }
	if err := table.store.Refresh(); err != nil { return nil, err }
	rootGeneration := table.store.Superblock().MappingSequence
	lease, err := leaseConfig.AcquireReader(ctx, table.store.Superblock().VolumeID, nodeID, bootID, epoch, onLeaseLost)
	if err != nil { return nil, err }
	if err = table.Register(slot, nodeID, bootID, epoch, rootGeneration); err != nil {
		_ = lease.Close()
		return nil, err
	}
	return &ReaderSession{table:table, lease:lease, slot:slot, nodeID:nodeID, bootID:bootID, epoch:epoch, rootGeneration:rootGeneration}, nil
}

func (s *ReaderSession) checkLeaseLocked() error {
	if s == nil || s.closed { return errors.New("reader session is closed") }
	if s.lease == nil || !s.lease.Valid() { return ErrReaderLeaseLost }
	return nil
}

func (s *ReaderSession) Heartbeat() error {
	if s == nil { return errors.New("nil reader session") }
	s.mu.Lock(); defer s.mu.Unlock()
	if err := s.checkLeaseLocked(); err != nil { return err }
	return s.table.Heartbeat(s.slot,s.nodeID,s.bootID,s.epoch,s.rootGeneration)
}

// AdvanceGeneration must be called after Tree.Refresh and after all I/O that
// can reference the previous root generation has completed.
func (s *ReaderSession) AdvanceGeneration(newGeneration uint64, oldGenerationIOsDrained bool) error {
	if s == nil { return errors.New("nil reader session") }
	s.mu.Lock(); defer s.mu.Unlock()
	if err := s.checkLeaseLocked(); err != nil { return err }
	if err := s.table.AdvanceGeneration(s.slot,s.nodeID,s.bootID,s.epoch,newGeneration,oldGenerationIOsDrained); err != nil { return err }
	s.rootGeneration = newGeneration
	return nil
}

// CloseDrained persists Closed only after the caller has stopped new reads
// and drained every in-flight I/O, then releases the sanlock reader resource.
func (s *ReaderSession) CloseDrained(ioDrained bool) error {
	if s == nil { return nil }
	s.mu.Lock(); defer s.mu.Unlock()
	if s.closed { return nil }
	if err := s.table.CloseDrained(s.slot,s.nodeID,s.bootID,s.epoch,ioDrained); err != nil { return err }
	s.closed = true
	return s.lease.Close()
}
