package btree

import (
	"encoding/binary"
	"fmt"
	"sort"

	"blockmesh/internal/metastore"
)

type cowChild struct {
	id     uint64
	minKey uint64
	level  uint16
	node   *cowNode
}

type cowNode struct {
	id       uint64
	level    uint16
	leaf     bool
	items    []Item
	children []cowChild
	dirty    bool
}

func loadCowNode(store *metastore.Store, id uint64) (*cowNode, error) {
	p, err := store.ReadPage(id)
	if err != nil {
		return nil, fmt.Errorf("read B+Tree page %d: %w", id, err)
	}
	if len(p.Payload) < nodeHeaderSize || string(p.Payload[:4]) != "BMTN" {
		return nil, fmt.Errorf("%w: invalid node header at page %d", ErrCorruptTree, id)
	}
	nodeVersion := binary.LittleEndian.Uint16(p.Payload[4:6])
	if nodeVersion != nodeFormatVersionV1 && nodeVersion != nodeFormatVersion {
		return nil, fmt.Errorf("%w: unsupported node version %d at page %d", ErrCorruptTree, nodeVersion, id)
	}
	count := int(binary.LittleEndian.Uint16(p.Payload[6:8]))
	level := binary.LittleEndian.Uint16(p.Payload[8:10])
	n := &cowNode{id: id, level: level}
	switch p.Type {
	case leafPageType:
		if level != 0 || count == 0 || count > maxLeafEntries ||
			len(p.Payload) != nodeHeaderSize+count*16 {
			return nil, fmt.Errorf("%w: invalid leaf page %d", ErrCorruptTree, id)
		}
		n.leaf = true
		n.items = make([]Item, count)
		for i := range n.items {
			off := nodeHeaderSize + i*16
			n.items[i] = Item{
				Key: binary.LittleEndian.Uint64(p.Payload[off : off+8]),
				Value: binary.LittleEndian.Uint64(p.Payload[off+8 : off+16]),
			}
			if i > 0 && n.items[i-1].Key >= n.items[i].Key {
				return nil, fmt.Errorf("%w: unsorted leaf page %d", ErrCorruptTree, id)
			}
		}
	case internalPageType:
		if level == 0 || count > maxInternalKeys || (count == 0 && nodeVersion < nodeFormatVersion) ||
			len(p.Payload) != nodeHeaderSize+count*8+(count+1)*8 {
			return nil, fmt.Errorf("%w: invalid internal page %d", ErrCorruptTree, id)
		}
		off := nodeHeaderSize
		separators := make([]uint64, count)
		for i := range separators {
			separators[i] = binary.LittleEndian.Uint64(p.Payload[off : off+8])
			if i > 0 && separators[i-1] >= separators[i] {
				return nil, fmt.Errorf("%w: unsorted separators in page %d", ErrCorruptTree, id)
			}
			off += 8
		}
		ids := make([]uint64, count+1)
		for i := range ids {
			ids[i] = binary.LittleEndian.Uint64(p.Payload[off : off+8])
			off += 8
		}
		n.children = make([]cowChild, len(ids))
		for i, childID := range ids {
			n.children[i] = cowChild{id: childID, level: level - 1}
			if i > 0 {
				n.children[i].minKey = separators[i-1]
			}
		}
		// The first child's minimum is not stored in the parent page. Read only
		// its leftmost path so a changed minimum can be propagated on COW.
		minKey, err := readSubtreeMinKey(store, ids[0], level-1)
		if err != nil {
			return nil, err
		}
		n.children[0].minKey = minKey
	default:
		return nil, fmt.Errorf("%w: unknown page type %d", ErrCorruptTree, p.Type)
	}
	return n, nil
}

func readSubtreeMinKey(store *metastore.Store, id uint64, expectedLevel uint16) (uint64, error) {
	p, err := store.ReadPage(id)
	if err != nil {
		return 0, err
	}
	if len(p.Payload) < nodeHeaderSize || string(p.Payload[:4]) != "BMTN" ||
		binary.LittleEndian.Uint16(p.Payload[8:10]) != expectedLevel {
		return 0, fmt.Errorf("%w: invalid leftmost path at page %d", ErrCorruptTree, id)
	}
	nodeVersion := binary.LittleEndian.Uint16(p.Payload[4:6])
	if nodeVersion != nodeFormatVersionV1 && nodeVersion != nodeFormatVersion {
		return 0, fmt.Errorf("%w: invalid leftmost path at page %d", ErrCorruptTree, id)
	}
	count := int(binary.LittleEndian.Uint16(p.Payload[6:8]))
	switch p.Type {
	case leafPageType:
		if expectedLevel != 0 || count == 0 || len(p.Payload) != nodeHeaderSize+count*16 {
			return 0, fmt.Errorf("%w: invalid leftmost leaf %d", ErrCorruptTree, id)
		}
		return binary.LittleEndian.Uint64(p.Payload[nodeHeaderSize : nodeHeaderSize+8]), nil
	case internalPageType:
		if expectedLevel == 0 || count > maxInternalKeys || (count == 0 && nodeVersion < nodeFormatVersion) ||
			len(p.Payload) != nodeHeaderSize+count*8+(count+1)*8 {
			return 0, fmt.Errorf("%w: invalid leftmost internal node %d", ErrCorruptTree, id)
		}
		childOff := nodeHeaderSize + count*8
		childID := binary.LittleEndian.Uint64(p.Payload[childOff : childOff+8])
		return readSubtreeMinKey(store, childID, expectedLevel-1)
	default:
		return 0, fmt.Errorf("%w: invalid page type %d", ErrCorruptTree, id)
	}
}

func (n *cowNode) minKey() uint64 {
	if n.leaf {
		if len(n.items) == 0 {
			return 0
		}
		return n.items[0].Key
	}
	if len(n.children) == 0 {
		return 0
	}
	if n.children[0].node != nil {
		return n.children[0].node.minKey()
	}
	return n.children[0].minKey
}

func childIndex(children []cowChild, key uint64) int {
	return sort.Search(len(children)-1, func(i int) bool {
		return key < children[i+1].minKey
	})
}

func insertCOW(store *metastore.Store, n *cowNode, key, value uint64) ([]*cowNode, bool, error) {
	if n.leaf {
		i := sort.Search(len(n.items), func(i int) bool { return n.items[i].Key >= key })
		if i < len(n.items) && n.items[i].Key == key {
			if n.items[i].Value == value {
				return []*cowNode{n}, false, nil
			}
			n.items[i].Value = value
		} else {
			n.items = append(n.items, Item{})
			copy(n.items[i+1:], n.items[i:])
			n.items[i] = Item{Key: key, Value: value}
		}
		n.dirty = true
		if len(n.items) <= maxLeafEntries {
			return []*cowNode{n}, true, nil
		}
		mid := len(n.items) / 2
		left := &cowNode{level: 0, leaf: true, dirty: true, items: append([]Item(nil), n.items[:mid]...)}
		right := &cowNode{level: 0, leaf: true, dirty: true, items: append([]Item(nil), n.items[mid:]...)}
		return []*cowNode{left, right}, true, nil
	}

	idx := childIndex(n.children, key)
	child := n.children[idx].node
	if child == nil {
		var err error
		child, err = loadCowNode(store, n.children[idx].id)
		if err != nil {
			return nil, false, err
		}
	}
	replacements, changed, err := insertCOW(store, child, key, value)
	if err != nil || !changed {
		return []*cowNode{n}, changed, err
	}
	newChildren := make([]cowChild, 0, len(n.children)+1)
	newChildren = append(newChildren, n.children[:idx]...)
	for _, replacement := range replacements {
		newChildren = append(newChildren, cowChild{
			id: replacement.id, minKey: replacement.minKey(),
			level: replacement.level, node: replacement,
		})
	}
	newChildren = append(newChildren, n.children[idx+1:]...)
	n.children = newChildren
	n.dirty = true
	if len(n.children) <= maxInternalChildren {
		return []*cowNode{n}, true, nil
	}
	mid := len(n.children) / 2
	left := &cowNode{level: n.level, dirty: true, children: append([]cowChild(nil), n.children[:mid]...)}
	right := &cowNode{level: n.level, dirty: true, children: append([]cowChild(nil), n.children[mid:]...)}
	return []*cowNode{left, right}, true, nil
}

func deleteCOW(store *metastore.Store, n *cowNode, key uint64) (*cowNode, bool, error) {
	if n.leaf {
		i := sort.Search(len(n.items), func(i int) bool { return n.items[i].Key >= key })
		if i == len(n.items) || n.items[i].Key != key {
			return n, false, nil
		}
		n.items = append(n.items[:i], n.items[i+1:]...)
		n.dirty = true
		if len(n.items) == 0 {
			return nil, true, nil
		}
		return n, true, nil
	}

	idx := childIndex(n.children, key)
	child := n.children[idx].node
	if child == nil {
		var err error
		child, err = loadCowNode(store, n.children[idx].id)
		if err != nil {
			return nil, false, err
		}
	}
	updated, changed, err := deleteCOW(store, child, key)
	if err != nil || !changed {
		return n, changed, err
	}
	if updated == nil {
		n.children = append(n.children[:idx], n.children[idx+1:]...)
	} else {
		n.children[idx] = cowChild{
			id: updated.id, minKey: updated.minKey(),
			level: updated.level, node: updated,
		}
	}
	n.dirty = true
	if len(n.children) == 0 {
		return nil, true, nil
	}
	return n, true, nil
}

func countDirty(n *cowNode) int {
	if n == nil || !n.dirty {
		return 0
	}
	total := 1
	for i := range n.children {
		if n.children[i].node != nil {
			total += countDirty(n.children[i].node)
		}
	}
	return total
}

func persistCOW(store *metastore.Store, n *cowNode, ids []uint64, cursor *int, sequence uint64) (uint64, error) {
	if !n.dirty {
		return n.id, nil
	}
	if !n.leaf {
		for i := range n.children {
			child := &n.children[i]
			if child.node != nil && child.node.dirty {
				id, err := persistCOW(store, child.node, ids, cursor, sequence)
				if err != nil {
					return 0, err
				}
				child.id = id
				child.minKey = child.node.minKey()
			}
		}
	}
	if *cursor >= len(ids) {
		return 0, fmt.Errorf("COW reservation exhausted")
	}
	id := ids[*cursor]
	*cursor = *cursor + 1
	var payload []byte
	var pageType uint16
	if n.leaf {
		payload = encodeLeaf(n.items)
		pageType = leafPageType
	} else {
		refs := make([]nodeRef, len(n.children))
		for i, child := range n.children {
			refs[i] = nodeRef{id: child.id, minKey: child.minKey, level: child.level}
		}
		payload = encodeInternal(refs, n.level)
		pageType = internalPageType
	}
	if err := store.WritePage(metastore.Page{ID: id, Type: pageType, Generation: sequence, Payload: payload}); err != nil {
		return 0, fmt.Errorf("write COW page %d: %w", id, err)
	}
	n.id = id
	return id, nil
}

func (t *Tree) publishCOW(root *cowNode, items []Item) error {
	sb := t.store.Superblock()
	if sb.MappingSequence == ^uint64(0) {
		return fmt.Errorf("B+Tree mapping sequence exhausted")
	}
	sequence := sb.MappingSequence + 1
	if root == nil {
		if err := t.store.Publish(metastore.NoPage, sequence); err != nil {
			return fmt.Errorf("publish empty B+Tree root: %w", err)
		}
		t.items, t.pages, t.root = nil, nil, metastore.NoPage
		return nil
	}

	pageCount := countDirty(root)
	if pageCount == 0 {
		// Deleting the final key in one root child may collapse the root to
		// an unchanged sibling. Publishing that existing page needs no COW
		// page allocation, but still advances the mapping sequence.
		if err := t.store.Publish(root.id, sequence); err != nil {
			return fmt.Errorf("publish collapsed B+Tree root: %w", err)
		}
		t.items = append([]Item(nil), items...)
		t.root = root.id
		return nil
	}
	reservation, err := t.arena.Reserve(uint64(pageCount))
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = reservation.Abort()
		}
	}()
	ids := reservation.Pages()
	cursor := 0
	rootID, err := persistCOW(t.store, root, ids, &cursor, sequence)
	if err != nil {
		return err
	}
	if cursor != len(ids) {
		return fmt.Errorf("internal error: used %d of %d reserved COW pages", cursor, len(ids))
	}
	if err = t.store.Sync(); err != nil {
		return fmt.Errorf("sync COW pages: %w", err)
	}
	if err = t.store.Publish(rootID, sequence); err != nil {
		// The inactive superblock may have reached stable storage despite a
		// reported flush error. Keep these IDs reserved until reopen/recovery.
		committed = true
		return fmt.Errorf("publish COW root (reservation retained for recovery): %w", err)
	}
	if err = reservation.Commit(); err != nil {
		committed = true
		return fmt.Errorf("root published but arena commit failed: %w", err)
	}
	committed = true
	t.items = append([]Item(nil), items...)
	t.root = rootID
	t.pages = append(t.pages, ids...)
	return nil
}
