package btree

import (
	"errors"
	"io"
	"sync"
	"testing"

	"blockmesh/internal/metastore"
)

type memoryDevice struct {
	mu sync.Mutex
	b  []byte
	writes int
}

func newMemoryDevice(pages int) *memoryDevice {
	return &memoryDevice{b: make([]byte, pages*int(metastore.PageSize))}
}
func (d *memoryDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off >= int64(len(d.b)) {
		return 0, io.EOF
	}
	n := copy(p, d.b[off:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (d *memoryDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(d.b)) {
		return 0, io.ErrShortWrite
	}
	d.writes++
	return copy(d.b[off:], p), nil
}
func (d *memoryDevice) Sync() error { return nil }

func newTree(t *testing.T, pages int) (*metastore.Store, *Tree, *memoryDevice) {
	t.Helper()
	dev := newMemoryDevice(pages)
	store, err := metastore.Format(dev, metastore.Layout{VolumeID: "btree-test", TotalPages: uint64(pages), ArenaStart: 2, ArenaPages: uint64(pages - 2)})
	if err != nil {
		t.Fatal(err)
	}
	tree, err := Open(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, tree, dev
}

func TestPutGetReplaceDeleteAndReopen(t *testing.T) {
	store, tree, dev := newTree(t, 1024)
	for i := uint64(0); i < 270; i++ {
		if err := tree.Put(i*3, i+1000); err != nil {
			t.Fatalf("Put(%d): %v", i*3, err)
		}
	}
	for i := uint64(0); i < 270; i++ {
		got, err := tree.Get(i * 3)
		if err != nil || got != i+1000 {
			t.Fatalf("Get(%d) = %d, %v", i*3, got, err)
		}
	}
	if err := tree.Put(9, 9999); err != nil {
		t.Fatal(err)
	}
	if got, err := tree.Get(9); err != nil || got != 9999 {
		t.Fatalf("replace: got %d, %v", got, err)
	}
	if err := tree.Delete(9); err != nil {
		t.Fatal(err)
	}
	if _, err := tree.Get(9); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted key lookup: %v", err)
	}

	reopenedStore, err := metastore.Open(dev, "btree-test")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 270; i++ {
		if i == 3 {
			continue
		}
		got, err := reopened.Get(i * 3)
		want := i + 1000
		if err != nil || got != want {
			t.Fatalf("reopened Get(%d) = %d, %v; want %d", i*3, got, err, want)
		}
	}
	if got := len(reopened.Snapshot()); got != 269 {
		t.Fatalf("snapshot has %d items, want 269", got)
	}
	_ = store
}

func TestDeleteLastKeyPublishesEmptyRoot(t *testing.T) {
	_, tree, dev := newTree(t, 32)
	if err := tree.Put(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := tree.Delete(1); err != nil {
		t.Fatal(err)
	}
	if tree.RootPage() != metastore.NoPage {
		t.Fatalf("root=%d, want NoPage", tree.RootPage())
	}
	reopenedStore, err := metastore.Open(dev, "btree-test")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Snapshot()) != 0 {
		t.Fatal("expected empty tree after reopen")
	}
}

func TestOpenRejectsCorruptRootPage(t *testing.T) {
	_, tree, dev := newTree(t, 32)
	if err := tree.Put(10, 20); err != nil {
		t.Fatal(err)
	}
	root := tree.RootPage()
	dev.mu.Lock()
	dev.b[root*metastore.PageSize+metastore.PageHeaderSize] ^= 0xff
	dev.mu.Unlock()
	store, err := metastore.Open(dev, "btree-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(store); err == nil {
		t.Fatal("expected corrupt root page error")
	}
}


func TestPathCopyUpdateWritesOnlySearchPath(t *testing.T) {
	store, tree, dev := newTree(t, 2048)
	for i := uint64(0); i < 270; i++ {
		if err := tree.Put(i*3, i+1); err != nil {
			t.Fatalf("Put(%d): %v", i*3, err)
		}
	}
	oldRoot := tree.RootPage()
	dev.mu.Lock()
	dev.writes = 0
	dev.mu.Unlock()

	if err := tree.Put(450, 99999); err != nil {
		t.Fatal(err)
	}
	dev.mu.Lock()
	writes := dev.writes
	dev.mu.Unlock()
	// Two B+Tree pages (leaf + root) and one superblock publication.
	if writes > 3 {
		t.Fatalf("single-key update wrote %d times; want at most 3 writes", writes)
	}
	if got, err := tree.Get(450); err != nil || got != 99999 {
		t.Fatalf("Get(450) = %d, %v", got, err)
	}
	seen := make(map[uint64]struct{})
	var live []uint64
	oldItems, err := loadNode(store, oldRoot, ^uint16(0), seen, &live)
	if err != nil {
		t.Fatalf("old COW root is no longer readable: %v", err)
	}
	oldIndex := sort.Search(len(oldItems), func(i int) bool { return oldItems[i].Key >= 450 })
	if oldIndex == len(oldItems) || oldItems[oldIndex].Value != 151 {
		t.Fatalf("old root does not retain pre-update value: %#v", oldItems)
	}
}

func TestDeleteAllKeysAcrossLeafBoundary(t *testing.T) {
	_, tree, dev := newTree(t, 2048)
	const count = 270
	for i := uint64(0); i < count; i++ {
		if err := tree.Put(i, i+10); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	for i := uint64(0); i < count; i++ {
		if err := tree.Delete(i); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}
	if tree.RootPage() != metastore.NoPage {
		t.Fatalf("root=%d, want NoPage", tree.RootPage())
	}
	reopenedStore, err := metastore.Open(dev, "btree-test")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Snapshot()) != 0 {
		t.Fatalf("expected empty tree, got %d items", len(reopened.Snapshot()))
	}
}
