package metastore

import (
	"errors"
	"testing"
)

func TestRetireQueueRequiresGenerationIODrainAndFencing(t *testing.T) {
	s, a := newTestArena(t, 12, 2)
	r, err := a.Reserve(2)
	if err != nil { t.Fatal(err) }
	ids := r.Pages()
	if err = r.Commit(); err != nil { t.Fatal(err) }
	if err = s.Publish(ids[0], 1); err != nil { t.Fatal(err) }

	q := NewRetireQueue()
	if err = q.Retire(0, ids); err != nil { t.Fatal(err) }
	if _, err = q.Collect(a, ReclaimProof{SafeGeneration: 2, IOsDrained: true}); !errors.Is(err, ErrUnsafeReclaim) {
		t.Fatalf("missing fencing proof: %v", err)
	}
	if n, err := q.Collect(a, ReclaimProof{SafeGeneration: 0, IOsDrained: true, FencingConfirmed: true}); err != nil || n != 0 {
		t.Fatalf("generation pin should retain pages: released=%d err=%v", n, err)
	}
	if got := a.Stats(); got.LivePages != 2 { t.Fatalf("pages released while pinned: %+v", got) }
	n, err := q.Collect(a, ReclaimProof{SafeGeneration: 1, IOsDrained: true, FencingConfirmed: true})
	if err != nil || n != 1 { t.Fatalf("collect: released=%d err=%v", n, err) }
	if got := a.Stats(); got.LivePages != 1 || got.FreePages != 9 { t.Fatalf("unexpected arena stats: %+v", got) }
	if len(q.Pending()) != 0 { t.Fatal("retire queue should be empty") }
}

func TestRetireQueueKeepsBatchWhenIOHasNotDrained(t *testing.T) {
	_, a := newTestArena(t, 8, 2)
	r, err := a.Reserve(1)
	if err != nil { t.Fatal(err) }
	if err = r.Commit(); err != nil { t.Fatal(err) }
	q := NewRetireQueue()
	if err = q.Retire(3, r.Pages()); err != nil { t.Fatal(err) }
	if _, err = q.Collect(a, ReclaimProof{SafeGeneration: 4, FencingConfirmed: true}); !errors.Is(err, ErrUnsafeReclaim) {
		t.Fatalf("expected unsafe reclaim, got %v", err)
	}
	if len(q.Pending()) != 1 { t.Fatal("unsafe batch was discarded") }
}
