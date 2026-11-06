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


type memoryCommitter struct{ dev *memoryDevice }
func (c memoryCommitter) CommitImages(images []metastore.PageImage) error {
 for _,image:=range images {
  if _,err:=c.dev.WriteAt(image.Data,int64(image.Offset));err!=nil{return err}
 }
 return c.dev.Sync()
}

func TestPersistentTreePersistsArenaAndRetireQueue(t *testing.T) {
 dev:=newMemoryDevice(128)
 store,err:=metastore.Format(dev,metastore.Layout{
  VolumeID:"persistent-btree",TotalPages:128,ArenaStart:2,ArenaPages:124,
  LifecycleStart:126,LifecyclePages:2,
 })
 if err!=nil{t.Fatal(err)}
 if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
 committer:=memoryCommitter{dev:dev}
 if _,err=Open(store,committer);!errors.Is(err,ErrPersistentArenaRequired){t.Fatalf("legacy Open must reject persistent lifecycle layout: %v",err)}
 tree,err:=OpenPersistent(store,committer);if err!=nil{t.Fatal(err)}
 if err=tree.Put(1,10);err!=nil{t.Fatal(err)}
 oldRoot:=tree.RootPage()
 if err=tree.Put(1,11);err!=nil{t.Fatal(err)}
 if tree.RootPage()==oldRoot{t.Fatal("COW update did not replace root")}
 if got:=tree.RetiredPages();len(got)!=1||got[0].RetireGeneration!=1||len(got[0].PageIDs)!=1||got[0].PageIDs[0]!=oldRoot{
  t.Fatalf("unexpected persistent retire queue: %+v",got)
 }
 reopenedStore,err:=metastore.Open(dev,"persistent-btree");if err!=nil{t.Fatal(err)}
 reopened,err:=OpenPersistent(reopenedStore,committer);if err!=nil{t.Fatal(err)}
 if got,err:=reopened.Get(1);err!=nil||got!=11{t.Fatalf("reopened Get = %d, %v",got,err)}
 if got:=reopened.ArenaStats();got.LivePages!=2{t.Fatalf("retired root not pinned after reopen: %+v",got)}
 if _,err=reopened.CollectRetired(metastore.ReclaimProof{SafeGeneration:2,IOsDrained:true,FencingConfirmed:true});err!=nil{t.Fatal(err)}
 if len(reopened.RetiredPages())!=0{t.Fatal("persistent retire queue not cleared")}
}


func TestPersistentArenaLifecycleReplaysAtomicallyWithWAL(t *testing.T) {
 f,err:=os.CreateTemp(t.TempDir(),"btree-persistent-wal-*.img");if err!=nil{t.Fatal(err)};defer f.Close()
 if err=f.Truncate(int64(format.MetadataSize));err!=nil{t.Fatal(err)}
 dev,err:=device.Open(f.Name(),true);if err!=nil{t.Fatal(err)};defer dev.Close()
 lifecycleStart:=format.ReservedOffset/format.PageSize
 store,err:=metastore.Format(dev,metastore.Layout{
  VolumeID:"persistent-wal",TotalPages:format.MetadataSize/format.PageSize,
  ArenaStart:1_700_000,ArenaPages:1024,LifecycleStart:lifecycleStart,LifecyclePages:2,
 })
 if err!=nil{t.Fatal(err)}
 if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
 poolSB,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
 journal,err:=wal.New(dev,poolSB,0);if err!=nil{t.Fatal(err)}
 tree,err:=OpenPersistent(store,journal);if err!=nil{t.Fatal(err)}
 if err=tree.Put(7,70);err!=nil{t.Fatal(err)}
 oldRoot:=tree.RootPage()

 oldInactive:=make([]byte,metastore.PageSize)
 if n,e:=dev.ReadAt(oldInactive,int64(metastore.Superblock1*metastore.PageSize));e!=nil||n!=len(oldInactive){t.Fatalf("read inactive superblock n=%d err=%v",n,e)}
 stateBefore:=make([]byte,metastore.PageSize);queueBefore:=make([]byte,metastore.PageSize)
 stateOff:=int64(lifecycleStart*format.PageSize);queueOff:=stateOff+int64(metastore.PageSize)
 if n,e:=dev.ReadAt(stateBefore,stateOff);e!=nil||n!=len(stateBefore){t.Fatalf("read lifecycle state n=%d err=%v",n,e)}
 if n,e:=dev.ReadAt(queueBefore,queueOff);e!=nil||n!=len(queueBefore){t.Fatalf("read retire queue n=%d err=%v",n,e)}

 if err=tree.Put(7,71);err!=nil{t.Fatal(err)}
 newRoot:=tree.RootPage();if newRoot==oldRoot{t.Fatal("expected COW root replacement")}
 // Model a crash after WAL Commit became durable but before any target
 // after-image (COW page, lifecycle pages, root superblock) was applied.
 images:=map[int64][]byte{
  int64(newRoot*metastore.PageSize):make([]byte,metastore.PageSize),
  int64(metastore.Superblock1*metastore.PageSize):oldInactive,
  stateOff:stateBefore,
  queueOff:queueBefore,
 }
 for off,data:=range images {
  if n,e:=dev.WriteAt(data,off);e!=nil||n!=len(data){t.Fatalf("restore pre-apply image at %d: n=%d err=%v",off,n,e)}
 }
 if err=dev.Sync();err!=nil{t.Fatal(err)}
 if _,err=wal.New(dev,poolSB,0);err!=nil{t.Fatalf("WAL recovery failed: %v",err)}
 reopenedStore,err:=metastore.Open(dev,"persistent-wal");if err!=nil{t.Fatal(err)}
 recoveredJournal,err:=wal.New(dev,poolSB,0);if err!=nil{t.Fatal(err)}
 recovered,err:=OpenPersistent(reopenedStore,recoveredJournal);if err!=nil{t.Fatal(err)}
 if got,e:=recovered.Get(7);e!=nil||got!=71{t.Fatalf("recovered value=%d err=%v",got,e)}
 pending:=recovered.RetiredPages()
 if len(pending)!=1||pending[0].RetireGeneration!=1||len(pending[0].PageIDs)!=1||pending[0].PageIDs[0]!=oldRoot{
  t.Fatalf("WAL did not atomically recover retire queue: %+v",pending)
 }
}
func TestPutBatchWithImagesCommitsExtrasAndRootTogether(t *testing.T) {
	path:=t.TempDir()+"/pool.img"
	f,err:=os.Create(path);if err!=nil{t.Fatal(err)}
	if err=f.Truncate(int64(format.MetadataSize));err!=nil{t.Fatal(err)}
	if err=f.Close();err!=nil{t.Fatal(err)}
	dev,err:=device.Open(path,true);if err!=nil{t.Fatal(err)};defer dev.Close()
	pool,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	if _,err=dev.WriteAt(pool.Encode(),int64(format.SuperAOffset));err!=nil{t.Fatal(err)}
	pool.Generation=2
	if _,err=dev.WriteAt(pool.Encode(),int64(format.SuperBOffset));err!=nil{t.Fatal(err)}
	pair,err:=format.VolumeDescriptorPairOffset(pool,0);if err!=nil{t.Fatal(err)}
	slotBytes:=(pool.MetadataSize-pool.ReservedOffset)/uint64(format.MaxVolumeDescriptors)/format.PageSize*format.PageSize
	arenaStart:=pool.ReservedOffset/format.PageSize+format.OwnerRecordPages
	desc:=format.VolumeDescriptor{VolumeID:"batch-pool",AG:0,Generation:1,VolumeSequence:1,MappingRootOffset:pool.MappingOffset,BitmapSequence:1,OwnerEpoch:1,ArenaStart:arenaStart,ArenaPages:128,RootPage:^uint64(0),LifecycleStart:arenaStart+128,LifecyclePages:8}
	_ = slotBytes
	image,err:=desc.Encode();if err!=nil{t.Fatal(err)}
	for slot:=uint64(0);slot<2;slot++ { if _,err=dev.WriteAt(image,int64(pair+slot*format.PageSize));err!=nil{t.Fatal(err)} }
	if err=dev.Sync();err!=nil{t.Fatal(err)}
	store,err:=metastore.OpenPoolVolume(dev,pool,0,"batch-pool");if err!=nil{t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
	journal,err:=wal.New(dev,pool,0);if err!=nil{t.Fatal(err)}
	tree,err:=OpenPersistent(store,journal);if err!=nil{t.Fatal(err)}
	extraOffset:=pool.BitmapOffset+format.PageSize
	extra:=make([]byte,metastore.PageSize);copy(extra[:8],"BMEXTRA1")
	if err=tree.PutBatchWithImages([]Item{{Key:10,Value:100},{Key:20,Value:200}},[]metastore.PageImage{{Offset:extraOffset,Data:extra}});err!=nil{t.Fatal(err)}
	if got,err:=tree.Get(10);err!=nil||got!=100{t.Fatalf("Get(10)=%d err=%v",got,err)}
	if got,err:=tree.Get(20);err!=nil||got!=200{t.Fatalf("Get(20)=%d err=%v",got,err)}
	read:=make([]byte,metastore.PageSize)
	if n,err:=dev.ReadAt(read,int64(extraOffset));err!=nil||n!=len(read){t.Fatalf("read extra image n=%d err=%v",n,err)}
	if string(read[:8])!="BMEXTRA1"{t.Fatalf("extra image not committed: %q",read[:8])}
	reopened,err:=metastore.OpenPoolVolume(dev,pool,0,"batch-pool");if err!=nil{t.Fatal(err)}
	recoveredJournal,err:=wal.New(dev,pool,0);if err!=nil{t.Fatal(err)}
	reopenedTree,err:=OpenPersistent(reopened,recoveredJournal);if err!=nil{t.Fatal(err)}
	if got,err:=reopenedTree.Get(20);err!=nil||got!=200{t.Fatalf("reopen Get(20)=%d err=%v",got,err)}
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

func TestCOWDoesNotRetireDescendantsOfUnchangedInternalSubtree(t *testing.T) {
	store, _, _ := newTree(t, 128)
	write := func(id uint64, typ uint16, payload []byte) {
		t.Helper()
		if err := store.WritePage(metastore.Page{ID:id, Type:typ, Generation:1, Payload:payload}); err != nil {
			t.Fatalf("write page %d: %v", id, err)
		}
	}
	write(2, leafPageType, encodeLeaf([]Item{{Key:1, Value:10}}))
	write(3, leafPageType, encodeLeaf([]Item{{Key:10, Value:100}}))
	write(4, leafPageType, encodeLeaf([]Item{{Key:20, Value:200}}))
	write(5, leafPageType, encodeLeaf([]Item{{Key:30, Value:300}}))
	write(6, internalPageType, encodeInternal([]nodeRef{{id:2, minKey:1, level:0}, {id:3, minKey:10, level:0}}, 1))
	write(7, internalPageType, encodeInternal([]nodeRef{{id:4, minKey:20, level:0}, {id:5, minKey:30, level:0}}, 1))
	write(8, internalPageType, encodeInternal([]nodeRef{{id:6, minKey:1, level:1}, {id:7, minKey:20, level:1}}, 2))
	if err := store.Sync(); err != nil { t.Fatal(err) }
	if err := store.Publish(8, 1); err != nil { t.Fatal(err) }

	tree, err := Open(store)
	if err != nil { t.Fatal(err) }
	if err = tree.Put(2, 22); err != nil { t.Fatal(err) }
	if got, getErr := tree.Get(2); getErr != nil || got != 22 {
		t.Fatalf("updated key lookup: got=%d err=%v", got, getErr)
	}
	// The untouched right-hand internal subtree was not loaded. Its child
	// leaves must not be mistaken for unreachable pages by the retire queue.
	if pending := tree.RetiredPages(); len(pending) != 0 {
		t.Fatalf("opaque subtree pages were incorrectly retired: %+v", pending)
	}
}

func TestPersistentTreeRefreshSeesCommittedRootFromAnotherStore(t *testing.T) {
	dev:=newMemoryDevice(128)
	store,err:=metastore.Format(dev,metastore.Layout{
		VolumeID:"persistent-refresh",TotalPages:128,ArenaStart:2,ArenaPages:124,
		LifecycleStart:126,LifecyclePages:2,
	})
	if err!=nil{t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
	committer:=memoryCommitter{dev:dev}
	reader,err:=OpenPersistent(store,committer);if err!=nil{t.Fatal(err)}
	writerStore,err:=metastore.Open(dev,"persistent-refresh");if err!=nil{t.Fatal(err)}
	writer,err:=OpenPersistent(writerStore,committer);if err!=nil{t.Fatal(err)}
	if err=writer.Put(7,70);err!=nil{t.Fatal(err)}
	if _,err=reader.Get(7);!errors.Is(err,ErrKeyNotFound){t.Fatalf("reader unexpectedly saw unrefreshed root: %v",err)}
	if err=reader.Refresh();err!=nil{t.Fatal(err)}
	got,err:=reader.Get(7);if err!=nil{t.Fatal(err)}
	if got!=70{t.Fatalf("refreshed value=%d want 70",got)}
}

func TestPersistentTreePublishesExternalArenaPageWithIndexAtomically(t *testing.T) {
	dev:=newMemoryDevice(128)
	store,err:=metastore.Format(dev,metastore.Layout{
		VolumeID:"external-arena-pages",TotalPages:128,ArenaStart:2,ArenaPages:124,
		LifecycleStart:126,LifecyclePages:2,
	})
	if err!=nil {t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil {t.Fatal(err)}
	committer:=memoryCommitter{dev:dev}
	tree,err:=OpenPersistent(store,committer);if err!=nil {t.Fatal(err)}
	reservation,ids,err:=tree.ReserveExternalPages(1);if err!=nil {t.Fatal(err)}
	recordID:=ids[0]
	page:=metastore.Page{ID:recordID,Type:0x100,Generation:1,Payload:[]byte("external-record")}
	if err=tree.PutBatchWithReservedArenaPages([]Item{{Key:(uint64(1)<<63)|123,Value:recordID}},reservation,[]metastore.Page{page},nil);err!=nil {
		t.Fatalf("atomic external page publish: %v",err)
	}
	if stats:=tree.ArenaStats();stats.ReservedPages!=0||stats.LivePages!=2 {
		t.Fatalf("external page and COW page not transitioned to Live: %+v",stats)
	}
	if got,readErr:=tree.store.ReadPage(recordID);readErr!=nil||got.Type!=0x100||string(got.Payload)!="external-record" {
		raw:=make([]byte,metastore.PageSize);_,_=dev.ReadAt(raw,int64(recordID*metastore.PageSize))
		t.Fatalf("external page unreadable immediately after commit: page=%+v err=%v raw=%x",got,readErr,raw[:48])
	}
	// A later COW update must retain the external page as Live.
	if err=tree.Put(7,70);err!=nil {t.Fatal(err)}
	if stats:=tree.ArenaStats();stats.LivePages<3 {t.Fatalf("external page was lost during later COW: %+v",stats)}

	reopenedStore,err:=metastore.Open(dev,"external-arena-pages");if err!=nil {t.Fatal(err)}
	resolver:=func(s *metastore.Store,items []Item)([]uint64,error){
		var ids []uint64
		for _,item:=range items {if item.Key>>63!=0 {ids=append(ids,item.Value)}}
		return ids,nil
	}
	reopened,err:=OpenPersistentWithExternalPages(reopenedStore,committer,resolver);if err!=nil {t.Fatal(err)}
	if got,err:=reopened.store.ReadPage(recordID);err!=nil||got.Type!=0x100||string(got.Payload)!="external-record" {
		t.Fatalf("recovered external page=%+v err=%v",got,err)
	}
	if stats:=reopened.ArenaStats();stats.LivePages<3 {t.Fatalf("recovered Arena did not pin external page: %+v",stats)}
	if _,err=OpenPersistent(reopenedStore,committer);err==nil {
		t.Fatal("opening external-page index without resolver must fail closed")
	}
}
