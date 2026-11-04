package metastore

import (
	"errors"
	"fmt"
	"sync"
)

var ErrUnsafeReclaim = errors.New("metadata pages are not safe to reclaim")

// RetireBatch records pages reachable from a root that was superseded by the
// publication at RetireGeneration. Pages are process-local until WAL persists
// these records.
type RetireBatch struct {
	RetireGeneration uint64
	PageIDs          []uint64
}

// ReclaimProof is supplied by the reader/lease and I/O lifecycle layer.
// SafeGeneration must be strictly greater than every batch being collected.
// FencingConfirmed must mean every expired/failed reader has been fenced;
// heartbeat expiry alone is not sufficient.
type ReclaimProof struct {
	SafeGeneration   uint64
	IOsDrained       bool
	FencingConfirmed bool
}

type RetireQueue struct {
	mu      sync.Mutex
	batches []RetireBatch
}

func NewRetireQueue() *RetireQueue { return &RetireQueue{} }

// Retire adds a batch and copies pageIDs so callers may safely reuse buffers.
func (q *RetireQueue) Retire(generation uint64, pageIDs []uint64) error {
	if q == nil {
		return errors.New("nil retire queue")
	}
	if len(pageIDs) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(pageIDs))
	ids := make([]uint64, 0, len(pageIDs))
	for _, id := range pageIDs {
		if id == NoPage {
			return fmt.Errorf("%w: NoPage in retire batch", ErrInvalidPageSet)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("%w: duplicate retired page %d", ErrInvalidPageSet, id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.batches = append(q.batches, RetireBatch{RetireGeneration: generation, PageIDs: ids})
	return nil
}

func (q *RetireQueue) Pending() []RetireBatch {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]RetireBatch, len(q.batches))
	for i, b := range q.batches {
		out[i] = RetireBatch{RetireGeneration: b.RetireGeneration, PageIDs: append([]uint64(nil), b.PageIDs...)}
	}
	return out
}

// Collect releases only batches whose reader generation, I/O drain and fencing
// proof are all satisfied. ReleasePages independently protects the current root.
func (q *RetireQueue) Collect(arena *Arena, proof ReclaimProof) (int, error) {
	if q == nil || arena == nil {
		return 0, errors.New("nil retire queue or arena")
	}
	if !proof.IOsDrained || !proof.FencingConfirmed {
		return 0, ErrUnsafeReclaim
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	kept := make([]RetireBatch, 0, len(q.batches))
	released := 0
	for i, batch := range q.batches {
		if proof.SafeGeneration <= batch.RetireGeneration {
			kept = append(kept, batch)
			continue
		}
		if err := arena.ReleasePages(batch.PageIDs); err != nil {
			kept = append(kept, q.batches[i:]...)
			q.batches = kept
			return released, err
		}
		released += len(batch.PageIDs)
	}
	q.batches = kept
	return released, nil
}
