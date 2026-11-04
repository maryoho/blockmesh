package metastore

import (
	"context"
	"errors"
	"sync"

	"blockmesh/internal/fence"
	"blockmesh/internal/volume"
)

var ErrReaderLeaseLost = errors.New("reader sanlock lease is no longer valid")

// ReaderSession couples a durable Reader Table slot, local SCSI PR
// registration, and a process-lifetime sanlock resource lease.
type ReaderSession struct {
	table *ReaderTable
	lease *volume.ReaderLease
	registrar fence.ReaderRegistrar
	slot uint32
	nodeID string
	bootID string
	epoch uint64
	mu sync.Mutex
	rootGeneration uint64
	closed bool
}

func JoinReaderSession(ctx context.Context, table *ReaderTable, leaseConfig volume.ReaderLeaseConfig,
	registrar fence.ReaderRegistrar, slot uint32, nodeID, bootID string, epoch uint64,
	onLeaseLost func(error)) (*ReaderSession, error) {
	if table == nil || registrar == nil { return nil, errors.New("reader table and SCSI PR registrar are required") }
	if err := table.store.Refresh(); err != nil { return nil, err }
	rootGeneration := table.store.Superblock().MappingSequence
	lease, err := leaseConfig.AcquireReader(ctx, table.store.Superblock().VolumeID, slot, nodeID, bootID, epoch, onLeaseLost)
	if err != nil { return nil, err }
	if err = registrar.RegisterReader(ctx,nodeID,bootID); err != nil {
		cleanupErr:=registrar.UnregisterReader(ctx,nodeID,bootID)
		if cleanupErr==nil { _ = lease.Close() }
		return nil, errors.Join(err,cleanupErr)
	}
	if err = table.Register(slot, nodeID, bootID, epoch, rootGeneration); err != nil {
		cleanupErr:=registrar.UnregisterReader(ctx,nodeID,bootID)
		if cleanupErr==nil { _ = lease.Close() }
		return nil, errors.Join(err,cleanupErr)
	}
	return &ReaderSession{table:table,lease:lease,registrar:registrar,slot:slot,nodeID:nodeID,bootID:bootID,epoch:epoch,rootGeneration:rootGeneration},nil
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

// CloseDrained unregisters the local PR keys, persists Closed, and releases
// the sanlock slot lease. The caller must stop new reads and drain all I/O.
func (s *ReaderSession) CloseDrained(ctx context.Context, ioDrained bool) error {
	if s == nil { return nil }
	s.mu.Lock(); defer s.mu.Unlock()
	if s.closed { return nil }
	if !ioDrained { return ErrUnsafeReclaim }
	if err:=s.registrar.UnregisterReader(ctx,s.nodeID,s.bootID);err!=nil{return err}
	if err := s.table.CloseDrained(s.slot,s.nodeID,s.bootID,s.epoch,true); err != nil { return err }
	s.closed = true
	return s.lease.Close()
}
