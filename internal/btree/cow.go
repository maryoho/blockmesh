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
				Key:   binary.LittleEndian.Uint64(p.Payload[off : off+8]),
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
		n.children[idx] = refForNode(updated)
		if isUnderfull(updated) && len(n.children) > 1 {
			if err := rebalanceChild(store, n, idx); err != nil {
				return nil, false, err
			}
		}
	}
	n.dirty = true
	if len(n.children) == 0 {
		return nil, true, nil
	}
	return n, true, nil
}

func refForNode(n *cowNode) cowChild {
	return cowChild{id: n.id, minKey: n.minKey(), level: n.level, node: n}
}

func minLeafEntries() int { return (maxLeafEntries + 1) / 2 }
func minInternalChildren() int { return (maxInternalChildren + 1) / 2 }

func isUnderfull(n *cowNode) bool {
	if n.leaf {
		return len(n.items) < minLeafEntries()
	}
	return len(n.children) < minInternalChildren()
}

// rebalanceChild combines an underfull child with an adjacent sibling. If the
// combined contents fit in one page they merge; otherwise both pages are
// redistributed evenly. Parent separators are regenerated during encoding.
func rebalanceChild(store *metastore.Store, parent *cowNode, idx int) error {
	leftIdx, rightIdx := idx, idx+1
	if idx > 0 {
		leftIdx, rightIdx = idx-1, idx
	}
	left := parent.children[leftIdx].node
	if left == nil {
		var err error
		left, err = loadCowNode(store, parent.children[leftIdx].id)
		if err != nil { return err }
	}
	right := parent.children[rightIdx].node
	if right == nil {
		var err error
		right, err = loadCowNode(store, parent.children[rightIdx].id)
		if err != nil { return err }
	}
	if left.level != right.level || left.leaf != right.leaf {
		return fmt.Errorf("%w: sibling level mismatch", ErrCorruptTree)
	}
	if left.leaf {
		combined := make([]Item, 0, len(left.items)+len(right.items))
		combined = append(combined, left.items...)
		combined = append(combined, right.items...)
		if len(combined) <= maxLeafEntries {
			merged := &cowNode{level: 0, leaf: true, dirty: true, items: combined}
			parent.children[leftIdx] = refForNode(merged)
			parent.children = append(parent.children[:rightIdx], parent.children[rightIdx+1:]...)
			return nil
		}
		mid := len(combined) / 2
		left.items = append([]Item(nil), combined[:mid]...)
		right.items = append([]Item(nil), combined[mid:]...)
		left.dirty, right.dirty = true, true
		parent.children[leftIdx], parent.children[rightIdx] = refForNode(left), refForNode(right)
		return nil
	}
	combined := make([]cowChild, 0, len(left.children)+len(right.children))
	combined = append(combined, left.children...)
	combined = append(combined, right.children...)
	if len(combined) <= maxInternalChildren {
		merged := &cowNode{level: left.level, dirty: true, children: combined}
		parent.children[leftIdx] = refForNode(merged)
		parent.children = append(parent.children[:rightIdx], parent.children[rightIdx+1:]...)
		return nil
	}
	mid := len(combined) / 2
	left.children = append([]cowChild(nil), combined[:mid]...)
	right.children = append([]cowChild(nil), combined[mid:]...)
	left.dirty, right.dirty = true, true
	parent.children[leftIdx], parent.children[rightIdx] = refForNode(left), refForNode(right)
	return nil
}

// collectCowPages enumerates the page graph represented by a COW root.
// It is called after persistCOW has assigned IDs to all dirty nodes.
func collectCowPages(root *cowNode) []uint64 {
	if root == nil { return nil }
	seen := make(map[uint64]struct{})
	var pages []uint64
	var walk func(*cowNode)
	add := func(id uint64) {
		if id == 0 || id == metastore.NoPage { return }
		if _, ok := seen[id]; ok { return }
		seen[id] = struct{}{}
		pages = append(pages, id)
	}
	walk = func(n *cowNode) {
		if n == nil { return }
		if !n.dirty {
			add(n.id)
			return
		}
		if n.leaf {
			add(n.id)
			return
		}
		for _, child := range n.children {
			if child.node != nil { walk(child.node) } else { add(child.id) }
		}
		add(n.id)
	}
	walk(root)
	return pages
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
	oldPages := append([]uint64(nil), t.pages...)
	var newPages []uint64
	if root != nil {
		newPages = collectCowPages(root)
	}
	if root == nil {
		if err := t.store.Publish(metastore.NoPage, sequence); err != nil {
			return fmt.Errorf("publish empty B+Tree root: %w", err)
		}
		if err := t.retireSuperseded(oldPages, nil, sb.MappingSequence); err != nil {
			return fmt.Errorf("record retired B+Tree pages after empty-root publication: %w", err)
		}
		t.items, t.pages, t.root = nil, nil, metastore.NoPage
		return nil
	}

	pageCount := countDirty(root)
	if pageCount == 0 {
		if err := t.store.Publish(root.id, sequence); err != nil {
			return fmt.Errorf("publish collapsed B+Tree root: %w", err)
		}
		if err := t.retireSuperseded(oldPages, newPages, sb.MappingSequence); err != nil {
			return fmt.Errorf("record retired B+Tree pages after root publication: %w", err)
		}
		t.items = append([]Item(nil), items...)
		t.pages = newPages
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
		committed = true
		return fmt.Errorf("publish COW root (reservation retained for recovery): %w", err)
	}
	if err = reservation.Commit(); err != nil {
		committed = true
		return fmt.Errorf("root published but arena commit failed: %w", err)
	}
	committed = true

	// persistCOW has assigned durable IDs to all dirty nodes; collect again so
	// the current page set contains the newly allocated IDs.
	newPages = collectCowPages(root)
	if err := t.retireSuperseded(oldPages, newPages, sb.MappingSequence); err != nil {
		return fmt.Errorf("record retired B+Tree pages after root publication: %w", err)
	}
	t.items = append([]Item(nil), items...)
	t.root = rootID
	t.pages = newPages
	return nil
}

func (t *Tree) retireSuperseded(oldPages, newPages []uint64, generation uint64) error {
	current := make(map[uint64]struct{}, len(newPages))
	for _, id := range newPages { current[id] = struct{}{} }
	var retired []uint64
	for _, id := range oldPages {
		if _, ok := current[id]; !ok { retired = append(retired, id) }
	}
	return t.retire.Retire(generation, retired)
}
