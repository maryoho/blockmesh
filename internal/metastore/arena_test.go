package metastore

import (
	"errors"
	"sync"
	"testing"
)

func newTestArena(t *testing.T, pages uint64, live ...uint64) (*Store, *Arena) {
	t.Helper()
	d := newMemdev(pages)
	s, err := Format(d, Layout{VolumeID: "arena-test", TotalPages: pages, ArenaStart: 2, ArenaPages: pages - 2})
	if err != nil { t.Fatal(err) }
	a, err := NewArena(s, live)
	if err != nil { t.Fatal(err) }
	return s, a
}

func TestArenaReserveCommitAndStats(t *testing.T) {
	_, a := newTestArena(t, 12, 2, 3)
	r, err := a.Reserve(3)
	if err != nil { t.Fatal(err) }
	ids := r.Pages()
	if len(ids) != 3 { t.Fatalf("reserved %d pages", len(ids)) }
	seen := map[uint64]bool{2: true, 3: true}
	for _, id := range ids {
		if seen[id] { t.Fatalf("duplicate or live page allocated: %d", id) }
		seen[id] = true
	}
	if got := a.Stats(); got.LivePages != 2 || got.ReservedPages != 3 || got.FreePages != 5 {
		t.Fatalf("unexpected stats: %+v", got)
	}
	if err = r.Commit(); err != nil { t.Fatal(err) }
	if got := a.Stats(); got.LivePages != 5 || got.ReservedPages != 0 || got.FreePages != 5 {
		t.Fatalf("unexpected committed stats: %+v", got)
	}
	if err = r.Commit(); !errors.Is(err, ErrReservationDone) { t.Fatalf("second commit: %v", err) }
}

func TestArenaAbortMakesPagesReusable(t *testing.T) {
	_, a := newTestArena(t, 10, 2)
	r, err := a.Reserve(4)
	if err != nil { t.Fatal(err) }
	ids := r.Pages()
	if err = r.Abort(); err != nil { t.Fatal(err) }
	if got := a.Stats(); got.ReservedPages != 0 || got.LivePages != 1 || got.FreePages != 7 {
		t.Fatalf("unexpected stats after abort: %+v", got)
	}
	r2, err := a.Reserve(4)
	if err != nil { t.Fatal(err) }
	if got := r2.Pages(); !samePageIDs(got, ids) { t.Fatalf("aborted pages not reused: got %v want %v", got, ids) }
}

func TestArenaFullAndRelease(t *testing.T) {
	s, a := newTestArena(t, 8, 2)
	r, err := a.Reserve(5)
	if err != nil { t.Fatal(err) }
	if _, err = a.Reserve(1); !errors.Is(err, ErrArenaFull) { t.Fatalf("expected arena full, got %v", err) }
	if err = r.Commit(); err != nil { t.Fatal(err) }
	ids := r.Pages()
	if err = a.ReleasePages(ids); err != nil { t.Fatal(err) }
	if got := a.Stats(); got.LivePages != 1 || got.ReservedPages != 0 || got.FreePages != 6 {
		t.Fatalf("unexpected stats after release: %+v", got)
	}
	if err = s.Publish(ids[0], 1); err != nil { t.Fatal(err) }
	if err = a.ReleasePages([]uint64{ids[0]}); err == nil { t.Fatal("expected current root release to fail") }
}

func TestArenaRejectsInvalidLiveSetAndRelease(t *testing.T) {
	s, _ := newTestArena(t, 8)
	if _, err := NewArena(s, []uint64{1}); !errors.Is(err, ErrInvalidPageSet) { t.Fatalf("expected invalid live page error, got %v", err) }
	a, err := NewArena(s, nil)
	if err != nil { t.Fatal(err) }
	if err = a.ReleasePages([]uint64{2}); !errors.Is(err, ErrInvalidPageSet) { t.Fatalf("expected non-live release error, got %v", err) }
	if err = a.ReleasePages([]uint64{3, 3}); !errors.Is(err, ErrInvalidPageSet) { t.Fatalf("expected duplicate release error, got %v", err) }
}

func TestArenaConcurrentReservationsAreUnique(t *testing.T) {
	_, a := newTestArena(t, 258)
	const workers, each = 32, 4
	all := make(chan []uint64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := a.Reserve(each)
			if err != nil { errs <- err; return }
			all <- r.Pages()
			if err = r.Commit(); err != nil { errs <- err }
		}()
	}
	wg.Wait()
	close(all)
	close(errs)
	for err := range errs { t.Fatal(err) }
	seen := make(map[uint64]bool)
	for ids := range all {
		for _, id := range ids {
			if seen[id] { t.Fatalf("page %d allocated twice", id) }
			seen[id] = true
		}
	}
	if len(seen) != workers*each { t.Fatalf("allocated %d pages, want %d", len(seen), workers*each) }
}

func samePageIDs(a, b []uint64) bool {
	if len(a) != len(b) { return false }
	for i := range a { if a[i] != b[i] { return false } }
	return true
}

func TestArenaFailedReservationRollsBackPartialPages(t *testing.T) {
	_, a := newTestArena(t, 8, 2, 3, 4, 5, 6)
	if _, err := a.Reserve(2); !errors.Is(err, ErrArenaFull) {
		t.Fatalf("expected arena full, got %v", err)
	}
	if got := a.Stats(); got.LivePages != 5 || got.ReservedPages != 0 || got.FreePages != 1 {
		t.Fatalf("failed reservation leaked pages: %+v", got)
	}
	r, err := a.Reserve(1)
	if err != nil { t.Fatalf("page was not returned after failed reservation: %v", err) }
	if err = r.Abort(); err != nil { t.Fatal(err) }
}

func TestArenaRebuildsFromPublishedRootReachability(t *testing.T) {
	d := newMemdev(16)
	s, err := Format(d, Layout{VolumeID: "rebuild", TotalPages: 16, ArenaStart: 2, ArenaPages: 14})
	if err != nil { t.Fatal(err) }
	if err = s.WritePage(Page{ID: 2, Type: 1, Payload: []byte("root")}); err != nil { t.Fatal(err) }
	if err = s.WritePage(Page{ID: 3, Type: 2, Payload: []byte("child")}); err != nil { t.Fatal(err) }
	if err = s.Sync(); err != nil { t.Fatal(err) }
	if err = s.Publish(2, 1); err != nil { t.Fatal(err) }
	reopened, err := Open(d, "rebuild")
	if err != nil { t.Fatal(err) }
	a, err := NewArena(reopened, []uint64{2, 3})
	if err != nil { t.Fatal(err) }
	r, err := a.Reserve(1)
	if err != nil { t.Fatal(err) }
	if got := r.Pages()[0]; got == 2 || got == 3 { t.Fatalf("reachable page %d was reused", got) }
}
