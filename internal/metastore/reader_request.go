// RequestSnapshot preserves the legacy ReaderSession interface. New code
// that has a request context should use RequestSnapshotContext.
func (s *ReaderSession) RequestSnapshot() (uint64,uint64,error) {
	return s.RequestSnapshotContext(context.Background())
}

package metastore

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (t *ReaderTable) ValidateActive(slot uint32, nodeID, bootID string, epoch, rootGeneration uint64) error {
	if t == nil { return errors.New("nil Reader Table") }
	unlock, err := t.lockReaderSet()
	if err != nil { return err }
	defer unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if err = t.store.Refresh(); err != nil { return err }
	r, err := t.readLocked(slot)
	if err != nil { return err }
	if r.State != ReaderActive || r.NodeID != nodeID || r.BootID != bootID || r.Epoch != epoch {
		return ErrReaderIdentity
	}
	if r.RootGeneration != rootGeneration {
		return fmt.Errorf("%w: durable=%d request=%d", ErrReaderGeneration, r.RootGeneration, rootGeneration)
	}
	return nil
}

// AdvanceRootGeneration requires the caller to refresh the B+Tree and drain
// all old-generation I/O before moving the durable Reader Table forward.
func (s *ReaderSession) AdvanceRootGeneration(rootPage, newGeneration uint64, oldGenerationIOsDrained bool) error {
	if s == nil { return errors.New("nil Reader Session") }
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(); err != nil { return err }
	if err := s.table.store.Refresh(); err != nil { return err }
	sb := s.table.store.Superblock()
	if sb.MappingSequence != newGeneration || sb.RootPage != rootPage { return ErrReaderGeneration }
	if rootPage != NoPage && (rootPage < sb.ArenaStart || rootPage-sb.ArenaStart >= sb.ArenaPages) {
		return ErrOutOfRange
	}
	if err := s.table.AdvanceGenerationAtRoot(s.slot, s.nodeID, s.bootID, s.epoch, rootPage, newGeneration, oldGenerationIOsDrained); err != nil {
		return err
	}
	s.rootPage, s.rootGeneration = rootPage, newGeneration
	return nil
}

 // AdvanceGenerationAtRoot couples the reader's durable generation update to
 // the exact root descriptor while holding the same distributed reader-set
 // barrier used by root publication and retire collection.
func (t *ReaderTable) AdvanceGenerationAtRoot(slot uint32, nodeID, bootID string, epoch, rootPage, newGeneration uint64, oldGenerationIOsDrained bool) error {
	if t == nil { return errors.New("nil Reader Table") }
	unlock, err := t.lockReaderSet()
	if err != nil { return err }
	defer unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if err = t.store.Refresh(); err != nil { return err }
	sb := t.store.Superblock()
	if sb.MappingSequence != newGeneration || sb.RootPage != rootPage { return ErrReaderGeneration }
	r, err := t.readLocked(slot)
	if err != nil { return err }
	if r.State != ReaderActive || r.NodeID != nodeID || r.BootID != bootID || r.Epoch != epoch { return ErrReaderIdentity }
	if !oldGenerationIOsDrained || newGeneration <= r.RootGeneration { return ErrReaderGeneration }
	if r.HeartbeatSequence == ^uint64(0) { return errors.New("reader heartbeat sequence exhausted") }
	r.RootGeneration = newGeneration
	r.HeartbeatSequence++
	r.HeartbeatUnixNano = time.Now().UnixNano()
	return t.writeLocked(r)
}
