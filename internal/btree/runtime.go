package btree

import (
	"errors"
	"fmt"
	"math"

	"blockmesh/internal/metastore"
)

// OpenPersistentAtRoot opens a COW B+Tree at an explicitly pinned root while
// reusing the Pool runtime's PersistentArena. This is required for online RO
// readers: their root may be older than the current Volume Descriptor root,
// but its pages remain pinned by the durable Reader Generation.
func OpenPersistentAtRoot(store *metastore.Store, journal metastore.Committer,
	arena *metastore.PersistentArena, rootPage uint64) (*Tree, error) {
	if store == nil || journal == nil || arena == nil {
		return nil, errors.New("Store, WAL committer and shared PersistentArena are required")
	}
	sb := store.Superblock()
	if sb.LifecyclePages == 0 {
		return nil, metastore.ErrLifecycleUnformatted
	}
	if rootPage != metastore.NoPage &&
		(rootPage < sb.ArenaStart || rootPage-sb.ArenaStart >= sb.ArenaPages) {
		return nil, metastore.ErrOutOfRange
	}
	t := &Tree{
		store: store,
		root: rootPage,
		retire: metastore.NewRetireQueue(),
		journal: journal,
		persistentArena: arena,
	}
	if rootPage != metastore.NoPage {
		var live []uint64
		items, err := loadNode(store, rootPage, math.MaxUint16, make(map[uint64]struct{}), &live)
		if err != nil {
			return nil, fmt.Errorf("load pinned B+Tree root %d: %w", rootPage, err)
		}
		t.items = items
		t.pages = append([]uint64(nil), live...)
	}
	return t, nil
}
