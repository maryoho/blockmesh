// Package btree implements a fixed-page copy-on-write B+Tree for metadata.
package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"blockmesh/internal/metastore"
)

const (
	leafPageType        uint16 = 1
	internalPageType    uint16 = 2
	nodeHeaderSize             = 12
	nodeFormatVersionV1 uint16 = 1
	nodeFormatVersion   uint16 = 2
	maxLeafEntries             = (metastore.PagePayloadMax - nodeHeaderSize) / 16
	maxInternalKeys            = (metastore.PagePayloadMax - 20) / 16
	maxInternalChildren        = maxInternalKeys + 1
)

var (
	ErrKeyNotFound = errors.New("B+Tree key not found")
	ErrCorruptTree = errors.New("corrupt B+Tree")
)

type Item struct {
	Key   uint64
	Value uint64
}

type nodeRef struct {
	id     uint64
	minKey uint64
	level  uint16
}

type Tree struct {
	mu    sync.RWMutex
	store *metastore.Store
	arena *metastore.Arena
	items []Item // sorted in-memory read cache; disk pages remain authoritative
	pages []uint64
	root  uint64
}

// Open loads and validates the published tree, then reconstructs the
// process-local arena allocation state from its reachable pages.
func Open(store *metastore.Store) (*Tree, error) {
	if store == nil {
		return nil, errors.New("nil metastore")
	}
	sb := store.Superblock()
	t := &Tree{store: store, root: sb.RootPage}
	var live []uint64
	if sb.RootPage != metastore.NoPage {
		seen := make(map[uint64]struct{})
		items, err := loadNode(store, sb.RootPage, ^uint16(0), seen, &live)
		if err != nil {
			return nil, err
		}
		t.items = items
		t.pages = append([]uint64(nil), live...)
	}
	arena, err := metastore.NewArena(store, live)
	if err != nil {
		return nil, err
	}
	t.arena = arena
	return t, nil
}

func loadNode(store *metastore.Store, id uint64, expectedLevel uint16, seen map[uint64]struct{}, live *[]uint64) ([]Item, error) {
	if _, ok := seen[id]; ok {
		return nil, fmt.Errorf("%w: page cycle or duplicate child %d", ErrCorruptTree, id)
	}
	seen[id] = struct{}{}
	p, err := store.ReadPage(id)
	if err != nil {
		return nil, fmt.Errorf("read B+Tree page %d: %w", id, err)
	}
	*live = append(*live, id)
	if len(p.Payload) < nodeHeaderSize || string(p.Payload[:4]) != "BMTN" {
		return nil, fmt.Errorf("%w: invalid node header at page %d", ErrCorruptTree, id)
	}
	nodeVersion := binary.LittleEndian.Uint16(p.Payload[4:6])
	if nodeVersion != nodeFormatVersionV1 && nodeVersion != nodeFormatVersion {
		return nil, fmt.Errorf("%w: unsupported node version %d at page %d", ErrCorruptTree, nodeVersion, id)
	}
	count := int(binary.LittleEndian.Uint16(p.Payload[6:8]))
	level := binary.LittleEndian.Uint16(p.Payload[8:10])
	switch p.Type {
	case leafPageType:
		if level != 0 || (expectedLevel != math.MaxUint16 && expectedLevel != level) || len(p.Payload) != nodeHeaderSize+count*16 || count == 0 || count > maxLeafEntries {
			return nil, fmt.Errorf("%w: invalid leaf page %d", ErrCorruptTree, id)
		}
		out := make([]Item, count)
		for i := range out {
			off := nodeHeaderSize + i*16
			out[i] = Item{Key: binary.LittleEndian.Uint64(p.Payload[off : off+8]), Value: binary.LittleEndian.Uint64(p.Payload[off+8 : off+16])}
			if i > 0 && out[i-1].Key >= out[i].Key {
				return nil, fmt.Errorf("%w: unsorted leaf page %d", ErrCorruptTree, id)
			}
		}
		return out, nil
	case internalPageType:
		if level == 0 || (expectedLevel != math.MaxUint16 && expectedLevel != level) || count > maxInternalKeys || (count == 0 && nodeVersion < nodeFormatVersion) || len(p.Payload) != nodeHeaderSize+count*8+(count+1)*8 {
			return nil, fmt.Errorf("%w: invalid internal page %d", ErrCorruptTree, id)
		}
		keys := make([]uint64, count)
		off := nodeHeaderSize
		for i := range keys {
			keys[i] = binary.LittleEndian.Uint64(p.Payload[off : off+8])
			if i > 0 && keys[i-1] >= keys[i] {
				return nil, fmt.Errorf("%w: unsorted separators in page %d", ErrCorruptTree, id)
			}
			off += 8
		}
		children := make([]uint64, count+1)
		for i := range children {
			children[i] = binary.LittleEndian.Uint64(p.Payload[off : off+8])
			off += 8
		}
		var out []Item
		for i, child := range children {
			part, childErr := loadNode(store, child, level-1, seen, live)
			if childErr != nil {
				return nil, childErr
			}
			if len(part) == 0 || (i > 0 && part[0].Key != keys[i-1]) {
				return nil, fmt.Errorf("%w: separator mismatch in page %d", ErrCorruptTree, id)
			}
			if len(out) > 0 && out[len(out)-1].Key >= part[0].Key {
				return nil, fmt.Errorf("%w: overlapping child ranges in page %d", ErrCorruptTree, id)
			}
			out = append(out, part...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unknown page type %d", ErrCorruptTree, p.Type)
	}
}

// Get returns the value associated with key.
func (t *Tree) Get(key uint64) (uint64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i := sort.Search(len(t.items), func(i int) bool { return t.items[i].Key >= key })
	if i == len(t.items) || t.items[i].Key != key {
		return 0, ErrKeyNotFound
	}
	return t.items[i].Value, nil
}

// Put inserts or replaces key. A successful update publishes a new root only
// after every new page image has been written and synchronized.
func (t *Tree) Put(key, value uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	next := append([]Item(nil), t.items...)
	i := sort.Search(len(next), func(i int) bool { return next[i].Key >= key })
	if i < len(next) && next[i].Key == key && next[i].Value == value {
		return nil
	}
	if i < len(next) && next[i].Key == key {
		next[i].Value = value
	} else {
		next = append(next, Item{})
		copy(next[i+1:], next[i:])
		next[i] = Item{Key: key, Value: value}
	}

	var root *cowNode
	if t.root == metastore.NoPage {
		root = &cowNode{level: 0, leaf: true, dirty: true, items: []Item{{Key: key, Value: value}}}
	} else {
		var err error
		root, err = loadCowNode(t.store, t.root)
		if err != nil {
			return err
		}
		roots, changed, err := insertCOW(t.store, root, key, value)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if len(roots) == 1 {
			root = roots[0]
		} else {
			children := make([]cowChild, len(roots))
			for i, child := range roots {
				children[i] = cowChild{id: child.id, minKey: child.minKey(), level: child.level, node: child}
			}
			root = &cowNode{level: roots[0].level + 1, dirty: true, children: children}
		}
	}
	return t.publishCOW(root, next)
}

// Delete removes key and returns ErrKeyNotFound when it does not exist.
func (t *Tree) Delete(key uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	i := sort.Search(len(t.items), func(i int) bool { return t.items[i].Key >= key })
	if i == len(t.items) || t.items[i].Key != key {
		return ErrKeyNotFound
	}
	next := make([]Item, 0, len(t.items)-1)
	next = append(next, t.items[:i]...)
	next = append(next, t.items[i+1:]...)

	root, err := loadCowNode(t.store, t.root)
	if err != nil {
		return err
	}
	root, changed, err := deleteCOW(t.store, root, key)
	if err != nil {
		return err
	}
	if !changed {
		return ErrKeyNotFound
	}
	// A root with one child can be replaced by that child directly. Unary
	// internal nodes below the root are retained to keep all leaves at the
	// same depth; their zero-separator encoding is valid.
	for root != nil && !root.leaf && len(root.children) == 1 {
		child := root.children[0].node
		if child == nil {
			child, err = loadCowNode(t.store, root.children[0].id)
			if err != nil {
				return err
			}
		}
		root = child
	}
	return t.publishCOW(root, next)
}

// Snapshot returns a stable, sorted copy of all key/value pairs.
func (t *Tree) Snapshot() []Item {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]Item(nil), t.items...)
}

func (t *Tree) RootPage() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root
}

func (t *Tree) ArenaStats() metastore.ArenaStats { return t.arena.Stats() }







func encodeLeaf(items []Item) []byte {
	b := make([]byte, nodeHeaderSize+len(items)*16)
	copy(b[:4], "BMTN")
	binary.LittleEndian.PutUint16(b[4:6], nodeFormatVersion)
	binary.LittleEndian.PutUint16(b[6:8], uint16(len(items)))
	binary.LittleEndian.PutUint16(b[8:10], 0)
	for i, item := range items {
		off := nodeHeaderSize + i*16
		binary.LittleEndian.PutUint64(b[off:off+8], item.Key)
		binary.LittleEndian.PutUint64(b[off+8:off+16], item.Value)
	}
	return b
}

func encodeInternal(children []nodeRef, level uint16) []byte {
	keys := len(children) - 1
	b := make([]byte, nodeHeaderSize+keys*8+len(children)*8)
	copy(b[:4], "BMTN")
	binary.LittleEndian.PutUint16(b[4:6], nodeFormatVersion)
	binary.LittleEndian.PutUint16(b[6:8], uint16(keys))
	binary.LittleEndian.PutUint16(b[8:10], level)
	off := nodeHeaderSize
	for i := 1; i < len(children); i++ {
		binary.LittleEndian.PutUint64(b[off:off+8], children[i].minKey)
		off += 8
	}
	for _, child := range children {
		binary.LittleEndian.PutUint64(b[off:off+8], child.id)
		off += 8
	}
	return b
}
