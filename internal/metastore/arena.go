package metastore

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

var (
	ErrArenaFull       = errors.New("metadata arena is full")
	ErrInvalidPageSet  = errors.New("invalid metadata page set")
	ErrReservationDone = errors.New("metadata page reservation is already completed")
)

type pageSet map[uint64]struct{}

type ArenaStats struct {
	TotalPages    uint64
	LivePages     uint64
	ReservedPages uint64
	FreePages     uint64
}

// Arena is a process-local allocator for the Store metadata arena. Its state
// must be rebuilt from the published metadata graph after a process restart.
type Arena struct {
	mu       sync.Mutex
	store    *Store
	start    uint64
	pages    uint64
	cursor   uint64
	nextID   uint64
	live     pageSet
	reserved map[uint64]uint64
	free     []uint64
}

// Reservation exclusively owns pages until Commit or Abort.
type Reservation struct {
	arena *Arena
	id    uint64
	pages []uint64
	done  bool // protected by arena.mu
}

// NewArena creates an allocator. livePages must include every page reachable
// from the published root. The current root is added automatically.
func NewArena(store *Store, livePages []uint64) (*Arena, error) {
	if store == nil {
		return nil, errors.New("nil metastore")
	}
	sb := store.Superblock()
	a := &Arena{
		store: store, start: sb.ArenaStart, pages: sb.ArenaPages,
		live: make(pageSet, len(livePages)+1), reserved: make(map[uint64]uint64),
	}
	for _, id := range livePages {
		if id < a.start || id-a.start >= a.pages {
			return nil, fmt.Errorf("%w: page %d is outside arena", ErrInvalidPageSet, id)
		}
		if _, exists := a.live[id]; exists {
			return nil, fmt.Errorf("%w: duplicate live page %d", ErrInvalidPageSet, id)
		}
		a.live[id] = struct{}{}
	}
	if sb.RootPage != NoPage {
		a.live[sb.RootPage] = struct{}{}
	}
	return a, nil
}

// Reserve returns count unique, not necessarily contiguous, absolute page IDs.
func (a *Arena) Reserve(count uint64) (*Reservation, error) {
	if count == 0 || count > a.pages || count > uint64(^uint(0)>>1) {
		return nil, ErrInvalidPageSet
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nextID == math.MaxUint64 {
		return nil, errors.New("metadata reservation ID exhausted")
	}
	a.nextID++
	r := &Reservation{arena: a, id: a.nextID, pages: make([]uint64, 0, int(count))}
	for uint64(len(r.pages)) < count {
		id, ok := a.takeFreeLocked()
		if !ok {
			a.rollbackLocked(r)
			return nil, ErrArenaFull
		}
		a.reserved[id] = r.id
		r.pages = append(r.pages, id)
	}
	return r, nil
}

func (r *Reservation) Pages() []uint64 {
	if r == nil || r.arena == nil {
		return nil
	}
	r.arena.mu.Lock()
	defer r.arena.mu.Unlock()
	return append([]uint64(nil), r.pages...)
}

// Commit transitions Reserved pages to Live. Call only after page writes,
// Store.Sync, and successful Store.Publish of a root referencing these pages.
func (r *Reservation) Commit() error {
	if r == nil || r.arena == nil {
		return ErrReservationDone
	}
	a := r.arena
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.done {
		return ErrReservationDone
	}
	for _, id := range r.pages {
		if owner, ok := a.reserved[id]; !ok || owner != r.id {
			return fmt.Errorf("%w: reservation lost page %d", ErrInvalidPageSet, id)
		}
		if _, exists := a.live[id]; exists {
			return fmt.Errorf("%w: page %d is already live", ErrInvalidPageSet, id)
		}
	}
	for _, id := range r.pages {
		delete(a.reserved, id)
		a.live[id] = struct{}{}
	}
	r.done = true
	return nil
}

// Abort returns reserved pages to the free list. Never abort after publishing
// a root that references any page in this reservation.
func (r *Reservation) Abort() error {
	if r == nil || r.arena == nil {
		return ErrReservationDone
	}
	a := r.arena
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.done {
		return ErrReservationDone
	}
	for _, id := range r.pages {
		if owner, ok := a.reserved[id]; !ok || owner != r.id {
			return fmt.Errorf("%w: reservation lost page %d", ErrInvalidPageSet, id)
		}
	}
	a.rollbackLocked(r)
	r.done = true
	return nil
}

// ReleasePages makes live pages reusable. The caller must prove that no reader
// or in-flight I/O can still reference them. The current published root is
// protected. Root publication and release must be serialized by the caller.
func (a *Arena) ReleasePages(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	root := a.store.Superblock().RootPage
	seen := make(pageSet, len(ids))
	for _, id := range ids {
		if id < a.start || id-a.start >= a.pages {
			return fmt.Errorf("%w: page %d is outside arena", ErrInvalidPageSet, id)
		}
		if id == root {
			return fmt.Errorf("%w: cannot release current root page %d", ErrInvalidPageSet, id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: duplicate page %d", ErrInvalidPageSet, id)
		}
		seen[id] = struct{}{}
		if _, ok := a.live[id]; !ok {
			return fmt.Errorf("%w: page %d is not live", ErrInvalidPageSet, id)
		}
		if _, ok := a.reserved[id]; ok {
			return fmt.Errorf("%w: page %d is reserved", ErrInvalidPageSet, id)
		}
	}
	for _, id := range ids {
		delete(a.live, id)
		a.free = append(a.free, id)
	}
	return nil
}

func (a *Arena) Stats() ArenaStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	live, reserved := uint64(len(a.live)), uint64(len(a.reserved))
	return ArenaStats{TotalPages: a.pages, LivePages: live, ReservedPages: reserved, FreePages: a.pages - live - reserved}
}

func (a *Arena) takeFreeLocked() (uint64, bool) {
	for len(a.free) > 0 {
		last := len(a.free) - 1
		id := a.free[last]
		a.free = a.free[:last]
		if _, live := a.live[id]; live {
			continue
		}
		if _, reserved := a.reserved[id]; reserved {
			continue
		}
		return id, true
	}
	for scanned := uint64(0); scanned < a.pages; scanned++ {
		id := a.start + a.cursor
		a.cursor++
		if a.cursor == a.pages {
			a.cursor = 0
		}
		if _, live := a.live[id]; live {
			continue
		}
		if _, reserved := a.reserved[id]; reserved {
			continue
		}
		return id, true
	}
	return 0, false
}

func (a *Arena) rollbackLocked(r *Reservation) {
	for _, id := range r.pages {
		delete(a.reserved, id)
		a.free = append(a.free, id)
	}
}
