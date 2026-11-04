package btree

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/wal"
)

type memoryDevice struct {
	mu     sync.Mutex
	b      []byte
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

func TestDeleteRebalancesLeafByRedistributionAndMerge(t *testing.T) {
	_, tree, _ := newTree(t, 2048)
	for i := uint64(0); i < 270; i++ {
		if err := tree.Put(i, i+1); err != nil { t.Fatalf("Put(%d): %v", i, err) }
	}
	// The initial split produces two leaves. Removing 18 keys from the
	// left leaf first forces redistribution, then a merge when the combined
	// leaf contents fit in one page.
	for i := uint64(0); i < 18; i++ {
		if err := tree.Delete(i); err != nil { t.Fatalf("Delete(%d): %v", i, err) }
	}
	root, err := tree.store.ReadPage(tree.RootPage())
	if err != nil { t.Fatal(err) }
	if root.Type != leafPageType {
		t.Fatalf("root type=%d, want leaf after sibling merge and root collapse", root.Type)
	}
	if got := len(tree.Snapshot()); got != 252 { t.Fatalf("snapshot has %d items, want 252", got) }
	for i := uint64(18); i < 270; i++ {
		if got, err := tree.Get(i); err != nil || got != i+1 {
			t.Fatalf("Get(%d) = %d, %v", i, got, err)
		}
	}
}

func TestDeleteRandomizedAgainstMap(t *testing.T) {
	_, tree, dev := newTree(t, 4096)
	const count = 420
	rng := rand.New(rand.NewSource(20261104))
	order := rng.Perm(count)
	want := make(map[uint64]uint64, count)
	for _, raw := range order {
		k := uint64(raw)
		v := k*17 + 3
		if err := tree.Put(k, v); err != nil { t.Fatalf("Put(%d): %v", k, err) }
		want[k] = v
	}
	order = rng.Perm(count)
	for _, raw := range order {
		k := uint64(raw)
		if err := tree.Delete(k); err != nil { t.Fatalf("Delete(%d): %v", k, err) }
		delete(want, k)
		got := tree.Snapshot()
		if len(got) != len(want) { t.Fatalf("after deleting %d: snapshot len=%d want=%d", k, len(got), len(want)) }
		for _, item := range got {
			if v, ok := want[item.Key]; !ok || v != item.Value {
				t.Fatalf("unexpected item after deleting %d: %+v", k, item)
			}
		}
	}
	if tree.RootPage() != metastore.NoPage { t.Fatalf("root=%d, want NoPage", tree.RootPage()) }
	reopenedStore, err := metastore.Open(dev, "btree-test")
	if err != nil { t.Fatal(err) }
	reopened, err := Open(reopenedStore)
	if err != nil { t.Fatal(err) }
	if len(reopened.Snapshot()) != 0 { t.Fatalf("reopened tree has %d entries", len(reopened.Snapshot())) }
}

func TestDeleteQueuesSupersededPagesWithoutReleasingThem(t *testing.T) {
	_, tree, _ := newTree(t, 128)
	if err := tree.Put(1, 10); err != nil { t.Fatal(err) }
	oldRoot := tree.RootPage()
	if err := tree.Put(2, 20); err != nil { t.Fatal(err) }
	pending := tree.RetiredPages()
	if len(pending) != 1 || len(pending[0].PageIDs) != 1 || pending[0].PageIDs[0] != oldRoot {
		t.Fatalf("unexpected retire queue: %+v oldRoot=%d", pending, oldRoot)
	}
	before := tree.ArenaStats()
	if n, err := tree.CollectRetired(metastore.ReclaimProof{SafeGeneration: 100, IOsDrained: true}); !errors.Is(err, metastore.ErrUnsafeReclaim) || n != 0 {
		t.Fatalf("collection without fencing proof: released=%d err=%v", n, err)
	}
	if after := tree.ArenaStats(); after != before { t.Fatalf("arena changed without proof: before=%+v after=%+v", before, after) }
}


func TestDeleteRetiresSupersededRoot(t *testing.T) {
	_, tree, _ := newTree(t, 128)
	if err := tree.Put(1, 10); err != nil { t.Fatal(err) }
	if err := tree.Put(2, 20); err != nil { t.Fatal(err) }
	oldRoot := tree.RootPage()
	before := len(tree.RetiredPages())
	if err := tree.Delete(1); err != nil { t.Fatal(err) }
	pending := tree.RetiredPages()
	if len(pending) != before+1 {
		t.Fatalf("retire batches=%d, want %d", len(pending), before+1)
	}
	last := pending[len(pending)-1]
	if last.RetireGeneration != 2 || len(last.PageIDs) != 1 || last.PageIDs[0] != oldRoot {
		t.Fatalf("unexpected delete retirement batch: %+v oldRoot=%d", last, oldRoot)
	}
	if got, err := tree.Get(2); err != nil || got != 20 {
		t.Fatalf("surviving key lookup: got=%d err=%v", got, err)
	}
}


func TestWALBackedCOWPublishesPagesAndRootAtomically(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "btree-wal-*.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	dev, err := device.Open(f.Name(), true)
	if err != nil { t.Fatal(err) }
	defer dev.Close()

	store, err := metastore.Format(dev, metastore.Layout{
		VolumeID: "btree-wal-test",
		TotalPages: format.MetadataSize / metastore.PageSize,
		ArenaStart: 1_700_000,
		ArenaPages: 1024,
	})
	if err != nil { t.Fatal(err) }
	sb, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	journal, err := wal.New(dev, sb, 0)
	if err != nil { t.Fatal(err) }
	tree, err := Open(store, journal)
	if err != nil { t.Fatal(err) }

	// Slot 0 is the inactive superblock (Format initially publishes slot 1).
	// Preserve it so we can model a crash after the WAL Commit but before any
	// of its after-images reached their target locations.
	oldInactive := make([]byte, metastore.PageSize)
	if n, readErr := dev.ReadAt(oldInactive, 0); readErr != nil || n != len(oldInactive) {
		t.Fatalf("read old inactive superblock: n=%d err=%v", n, readErr)
	}
	if err = tree.Put(42, 4242); err != nil { t.Fatal(err) }
	newRoot := tree.RootPage()
	if newRoot == metastore.NoPage { t.Fatal("WAL-backed Put did not publish a root") }

	// Roll back the applied root descriptor and COW page while leaving the
	// durable WAL records intact. Reopening the Journal must replay both.
	if n, writeErr := dev.WriteAt(make([]byte, metastore.PageSize), int64(newRoot*metastore.PageSize)); writeErr != nil || n != metastore.PageSize {
		t.Fatalf("clear COW root page: n=%d err=%v", n, writeErr)
	}
	if n, writeErr := dev.WriteAt(oldInactive, 0); writeErr != nil || n != len(oldInactive) {
		t.Fatalf("restore old root descriptor: n=%d err=%v", n, writeErr)
	}
	if err = dev.Sync(); err != nil { t.Fatal(err) }

	if _, err = wal.New(dev, sb, 0); err != nil {
		t.Fatalf("WAL reopen must replay COW pages and root descriptor: %v", err)
	}
	reopenedStore, err := metastore.Open(dev, "btree-wal-test")
	if err != nil { t.Fatal(err) }
	reopened, err := Open(reopenedStore)
	if err != nil { t.Fatal(err) }
	if got, getErr := reopened.Get(42); getErr != nil || got != 4242 {
		t.Fatalf("recovered COW value = %d, %v; want 4242", got, getErr)
	}
	if reopened.RootPage() != newRoot {
		t.Fatalf("recovered root=%d, want %d", reopened.RootPage(), newRoot)
	}
}
