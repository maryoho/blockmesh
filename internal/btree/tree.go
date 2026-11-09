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
	ErrPersistentArenaRequired = errors.New("persistent lifecycle metadata requires OpenPersistent")
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
	pages  []uint64
	retire  *metastore.RetireQueue
	journal metastore.Committer
	persistentArena *metastore.PersistentArena
	readerCoordinator metastore.ReaderCoordinator
	externalPageResolver ExternalPageResolver
	externalPages []uint64
	root   uint64
	rootGeneration uint64
}

// Open loads and validates the published tree, then reconstructs the
// process-local arena allocation state from its reachable pages.
func Open(store *metastore.Store, journals ...metastore.Committer) (*Tree, error) {
	return openTree(store, false, journals...)
}

func openTree(store *metastore.Store, allowPersistent bool, journals ...metastore.Committer) (*Tree, error) {
	if len(journals) > 1 {
		return nil, errors.New("at most one WAL committer may be supplied")
	}
	if store == nil {
		return nil, errors.New("nil metastore")
	}
	sb := store.Superblock()
	if sb.LifecyclePages != 0 && !allowPersistent { return nil, ErrPersistentArenaRequired }
	if sb.LifecyclePages == 0 && allowPersistent { return nil, metastore.ErrLifecycleUnformatted }
	t := &Tree{store: store, root: sb.RootPage, rootGeneration: sb.MappingSequence, retire: metastore.NewRetireQueue()}
	if len(journals) == 1 {
		if journals[0] == nil {
			return nil, errors.New("nil WAL committer")
		}
		t.journal = journals[0]
	}
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
	if !allowPersistent {
		arena, err := metastore.NewArena(store, live)
		if err != nil { return nil, err }
		t.arena = arena
	}
	return t, nil
}

// OpenPersistent opens a tree whose Arena and retire queue are persisted
// in the lifecycle region configured in metastore.Layout. The caller must
// recover the WAL before calling this function.
// ExternalPageResolver discovers every non-B+Tree Arena page referenced by
// indexed records. Returned IDs are pinned Live during Arena reconstruction.
type ExternalPageResolver func(*metastore.Store, []Item) ([]uint64, error)

func OpenPersistent(store *metastore.Store, journal metastore.Committer, coordinators ...metastore.ReaderCoordinator) (*Tree, error) {
	return OpenPersistentWithExternalPages(store, journal, nil, coordinators...)
}

// OpenPersistentWithExternalPages must be used when high-bit namespace keys
// point to immutable external Arena pages. The resolver validates all records
// and collision-chain links and returns every referenced page ID.
func OpenPersistentWithExternalPages(store *metastore.Store, journal metastore.Committer, resolver ExternalPageResolver, coordinators ...metastore.ReaderCoordinator) (*Tree, error) {
	if journal == nil { return nil, errors.New("nil WAL committer") }
	t, err := openTree(store, true, journal)
	if err != nil { return nil, err }
	hasExternal := false
	dataRetireFloor := ^uint64(0)
	if dynamicSB, dynamicErr := store.DynamicPoolSuperblock(); dynamicErr == nil {
		dataRetireFloor = dynamicSB.Geometry.TotalBlocks
	}
	for _, item := range t.items {
		if item.Key>>63 != 0 || item.Key >= dataRetireFloor { hasExternal = true; break }
	}
	if hasExternal && resolver == nil { return nil, errors.New("external-page index keys require an ExternalPageResolver") }
	if resolver != nil {
		t.externalPages, err = resolver(store, append([]Item(nil), t.items...))
		if err != nil { return nil, fmt.Errorf("resolve external Arena pages: %w", err) }
		t.externalPageResolver = resolver
	}
	live := append(append([]uint64(nil), t.pages...), t.externalPages...)
	persistent, err := metastore.NewPersistentArena(store, live, journal, coordinators...)
	if err != nil { return nil, fmt.Errorf("open persistent Arena: %w", err) }
	t.persistentArena = persistent
	if len(coordinators)==1 { t.readerCoordinator=coordinators[0] }
	return t, nil
}

// RootGeneration returns the MappingSequence of the root currently loaded
// into this Tree. It is used to register and advance durable Reader pins.
func (t *Tree) RootGeneration() uint64 {
	if t == nil { return 0 }
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rootGeneration
}

// Refresh reloads the currently published root for an online-consistent
// reader. Call it before advancing that reader's durable Root Generation.
// Refresh is intended for read-only trees; outstanding local reservations
// prevent replacing the allocator state.
func (t *Tree) Refresh() error {
	if t==nil { return errors.New("nil B+Tree") }
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentArena==nil { return ErrPersistentArenaRequired }
	if stats:=t.persistentArena.Stats();stats.ReservedPages!=0 {
		return errors.New("cannot refresh B+Tree with outstanding metadata reservations")
	}
	if t.readerCoordinator!=nil {
		unlock,err:=t.readerCoordinator.LockReaderSet(t.store.Superblock().VolumeID)
		if err!=nil{return err}
		defer unlock()
	}
	if err:=t.store.Refresh();err!=nil{return err}
	fresh,err:=openTree(t.store,true,t.journal)
	if err!=nil{return fmt.Errorf("reload published B+Tree root: %w",err)}
	hasExternal:=false
	dataRetireFloor:=^uint64(0)
	if dynamicSB,dynamicErr:=t.store.DynamicPoolSuperblock();dynamicErr==nil { dataRetireFloor=dynamicSB.Geometry.TotalBlocks }
	for _,item:=range fresh.items {
		if item.Key>>63!=0 || item.Key>=dataRetireFloor {hasExternal=true;break}
	}
	if hasExternal&&t.externalPageResolver==nil {return errors.New("external-page index keys require an ExternalPageResolver")}
	var external []uint64
	if t.externalPageResolver!=nil {
		external,err=t.externalPageResolver(t.store,append([]Item(nil),fresh.items...))
		if err!=nil{return fmt.Errorf("resolve external Arena pages after refresh: %w",err)}
	}
	live:=append(append([]uint64(nil),fresh.pages...),external...)
	persistent,err:=metastore.NewPersistentArena(t.store,live,t.journal,t.readerCoordinator)
	if err!=nil{return fmt.Errorf("rebuild persistent Arena after refresh: %w",err)}
	t.root=fresh.root
	t.rootGeneration=fresh.rootGeneration
	t.items=append([]Item(nil),fresh.items...)
	t.pages=append([]uint64(nil),fresh.pages...)
	t.externalPages=external
	t.persistentArena=persistent
	return nil
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
// Put inserts or replaces one key.
func (t *Tree) Put(key, value uint64) error {
	return t.PutBatchWithImages([]Item{{Key:key, Value:value}}, nil)
}

// PutBatchWithImages applies all key/value updates and caller-supplied fixed-LBA
// after-images in one root publication/WAL transaction. It is intended for
// Pool metadata transactions (e.g. mapping + idempotency index + Data Bitmap).
// Every extra image must be a full aligned 4KiB page and must not overlap the
// Volume Descriptor/root image or any COW page.
func (t *Tree) PutBatchWithImages(updates []Item, extra []metastore.PageImage) error {
	return t.PutBatchWithArenaPages(updates,nil,extra)
}

// PutBatchWithArenaPages reserves COW pages and immutable external Arena pages
// from one Reservation, then publishes their images, index updates, lifecycle
// state and root in one WAL transaction.
func (t *Tree) PutBatchWithArenaPages(updates []Item, arenaPages []metastore.Page, extra []metastore.PageImage) error {
	return t.putBatchWithArenaPages(updates,arenaPages,extra,nil)
}

// ReserveExternalPages reserves immutable record pages before their IDs are
// embedded in the B+Tree index. The returned Reservation must be passed to
// PutBatchWithReservedArenaPages; callers must not Commit or Abort it directly.
func (t *Tree) ReserveExternalPages(count uint64) (*metastore.Reservation, []uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentArena==nil {return nil,nil,metastore.ErrPersistentArenaRequired}
	r,err:=t.persistentArena.Reserve(count)
	if err!=nil{return nil,nil,err}
	return r,r.Pages(),nil
}

// PutBatchWithReservedArenaPages publishes pages whose IDs were obtained from
// ReserveExternalPages. The same reservation is extended for all COW pages,
// so external records, B+Tree nodes, index values and the root share one WAL.
func (t *Tree) PutBatchWithReservedArenaPages(updates []Item, reservation *metastore.Reservation, arenaPages []metastore.Page, extra []metastore.PageImage) error {
	if reservation==nil||len(arenaPages)==0 {return metastore.ErrInvalidPageSet}
	return t.putBatchWithArenaPages(updates,arenaPages,extra,reservation)
}

func (t *Tree) putBatchWithArenaPages(updates []Item, arenaPages []metastore.Page, extra []metastore.PageImage, reservation *metastore.Reservation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(updates)==0 && len(extra)==0 && len(arenaPages)==0 { return nil }
	if len(extra)>0 && t.journal==nil { return errors.New("atomic extra images require a WAL-backed B+Tree") }
	next:=append([]Item(nil),t.items...)
	var root *cowNode
	if t.root!=metastore.NoPage {
		var err error
		root,err=loadCowNode(t.store,t.root);if err!=nil{return err}
	}
	changed:=false
	for _,u:=range updates {
		i:=sort.Search(len(next),func(i int)bool{return next[i].Key>=u.Key})
		if i<len(next)&&next[i].Key==u.Key&&next[i].Value==u.Value { continue }
		if i<len(next)&&next[i].Key==u.Key { next[i].Value=u.Value } else {
			next=append(next,Item{});copy(next[i+1:],next[i:]);next[i]=u
		}
		if root==nil {
			root=&cowNode{level:0,leaf:true,dirty:true,items:[]Item{{Key:u.Key,Value:u.Value}}}
			changed=true
			continue
		}
		roots,didChange,err:=insertCOW(t.store,root,u.Key,u.Value)
		if err!=nil{return err}
		if !didChange {continue}
		changed=true
		if len(roots)==1 { root=roots[0] } else {
			children:=make([]cowChild,len(roots))
			for i,child:=range roots { children[i]=cowChild{id:child.id,minKey:child.minKey(),level:child.level,node:child} }
			root=&cowNode{level:roots[0].level+1,dirty:true,children:children}
		}
	}
	if !changed && len(extra)==0 && len(arenaPages)==0 { return nil }
	if root==nil && len(extra)>0 { root=nil }
	return t.publishCOWWithReservedPages(root,next,arenaPages,extra,reservation)
}


// DeleteBatchWithImagesRetireExternal removes index entries and commits
// fixed-LBA after-images in the same COW/WAL root transaction. External Arena
// page IDs removed from the index are retired with the old root, not leaked or
// returned to the allocator before Reader generations have advanced.
func (t *Tree) DeleteBatchWithImagesRetireExternal(keys []uint64, extra []metastore.PageImage, retireExternal []uint64) error {
	if t == nil || t.persistentArena == nil || t.journal == nil {
		return metastore.ErrPersistentArenaRequired
	}
	if len(keys) == 0 || len(retireExternal) == 0 {
		return metastore.ErrInvalidPageSet
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	next := append([]Item(nil), t.items...)
	var root *cowNode
	if t.root != metastore.NoPage {
		var err error
		root, err = loadCowNode(t.store, t.root)
		if err != nil { return err }
	}
	for _, key := range keys {
		i := sort.Search(len(next), func(i int) bool { return next[i].Key >= key })
		if i == len(next) || next[i].Key != key { return ErrKeyNotFound }
		if root == nil { return ErrCorruptTree }
		var changed bool
		var err error
		root, changed, err = deleteCOW(t.store, root, key)
		if err != nil { return err }
		if !changed { return ErrCorruptTree }
		next = append(next[:i], next[i+1:]...)
		for root != nil && !root.leaf && len(root.children) == 1 {
			child := root.children[0].node
			if child == nil {
				child, err = loadCowNode(t.store, root.children[0].id)
				if err != nil { return err }
			}
			root = child
		}
	}
	return t.publishCOWWithReservedPages(root, next, nil, extra, nil, retireExternal)
}

 // DeleteBatchWithReservedArenaPages removes a set of logical mappings and
 // publishes caller-supplied index updates, immutable Arena pages and the new
 // root in one COW/WAL transaction. It is used by idempotent discard so the
 // mapping deletion and replay record cannot become visible separately.
func (t *Tree) DeleteBatchWithReservedArenaPages(keys []uint64, updates []Item,
	reservation *metastore.Reservation, arenaPages []metastore.Page, extra []metastore.PageImage) error {
	if t == nil || reservation == nil || len(arenaPages) == 0 {
		return metastore.ErrInvalidPageSet
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	next := append([]Item(nil), t.items...)
	var root *cowNode
	if t.root != metastore.NoPage {
		var err error
		root, err = loadCowNode(t.store, t.root)
		if err != nil { return err }
	}
	for _, key := range keys {
		i := sort.Search(len(next), func(i int) bool { return next[i].Key >= key })
		if i == len(next) || next[i].Key != key { continue }
		if root == nil { return ErrCorruptTree }
		var changed bool
		var err error
		root, changed, err = deleteCOW(t.store, root, key)
		if err != nil { return err }
		if !changed { return ErrCorruptTree }
		next = append(next[:i], next[i+1:]...)
		for root != nil && !root.leaf && len(root.children) == 1 {
			child := root.children[0].node
			if child == nil {
				child, err = loadCowNode(t.store, root.children[0].id)
				if err != nil { return err }
			}
			root = child
		}
	}
	for _, update := range updates {
		i := sort.Search(len(next), func(i int) bool { return next[i].Key >= update.Key })
		if i < len(next) && next[i].Key == update.Key {
			if next[i].Value == update.Value { continue }
			next[i].Value = update.Value
		} else {
			next = append(next, Item{})
			copy(next[i+1:], next[i:])
			next[i] = update
		}
		if root == nil {
			root = &cowNode{level: 0, leaf: true, dirty: true, items: []Item{{Key:update.Key, Value:update.Value}}}
			continue
		}
		roots, changed, err := insertCOW(t.store, root, update.Key, update.Value)
		if err != nil { return err }
		if !changed { continue }
		if len(roots) == 1 { root = roots[0] } else {
			children := make([]cowChild, len(roots))
			for i, child := range roots {
				children[i] = cowChild{id:child.id, minKey:child.minKey(), level:child.level, node:child}
			}
			root = &cowNode{level:roots[0].level+1, dirty:true, children:children}
		}
	}
	return t.publishCOWWithReservedPages(root, next, arenaPages, extra, reservation)
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

func (t *Tree) ArenaStats() metastore.ArenaStats {
	if t.persistentArena != nil { return t.persistentArena.Stats() }
	return t.arena.Stats()
}

// RetiredPages returns a snapshot of process-local retirement batches.
func (t *Tree) RetiredPages() []metastore.RetireBatch {
	if t.persistentArena != nil { return t.persistentArena.Pending() }
	return t.retire.Pending()
}

// CollectRetired applies an externally established reader/fencing proof.
// It serializes reclamation with root publication in this Tree instance.
func (t *Tree) CollectRetired(proof metastore.ReclaimProof) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentArena != nil { return t.persistentArena.Collect(proof) }
	return t.retire.Collect(t.arena, proof)
}

// CollectRetiredWithReaders derives the safe generation from the persistent
// Reader Table and uses the configured distributed reader-set barrier.
func (t *Tree) CollectRetiredWithReaders(readers *metastore.ReaderTable, ioDrained bool) (int,error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentArena==nil { return 0,metastore.ErrPersistentArenaRequired }
	return t.persistentArena.CollectWithReaders(readers,ioDrained)
}

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

// UsesPersistentArena reports whether this tree was opened with durable Arena
// lifecycle management.
func (t *Tree) UsesPersistentArena() bool {
	if t == nil { return false }
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.persistentArena != nil
}

// UsesClusterSafeCommitter reports whether root publication is protected by
// the distributed WAL commit protocol required for shared-SAN operation.
func (t *Tree) UsesClusterSafeCommitter() bool {
	if t == nil { return false }
	t.mu.RLock()
	defer t.mu.RUnlock()
	c, ok := t.journal.(metastore.ClusterSafeCommitter)
	return ok && c.ClusterSafe()
}

// AbortReservedArenaPages releases a reservation that has not been submitted
// to a WAL transaction. It must never be used after an ambiguous commit error.
func (t *Tree) AbortReservedArenaPages(r *metastore.Reservation) error {
	if t == nil || r == nil { return metastore.ErrReservationDone }
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentArena == nil { return metastore.ErrPersistentArenaRequired }
	return r.Abort()
}
