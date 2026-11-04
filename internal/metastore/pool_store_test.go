package metastore_test

import (
	"errors"
	"os"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

type poolImageCommitter struct {
	dev device.Device
	fail bool
	images []metastore.PageImage
}

func (c *poolImageCommitter) ClusterSafe() bool { return true }

func (c *poolImageCommitter) CommitImages(images []metastore.PageImage) error {
	c.images = append([]metastore.PageImage(nil), images...)
	if c.fail { return errors.New("injected WAL failure") }
	for _, image := range images {
		if _, err := c.dev.WriteAt(image.Data, int64(image.Offset)); err != nil { return err }
	}
	return c.dev.Sync()
}

func TestOpenPoolVolumeAndPublishRootThroughDescriptor(t *testing.T) {
	path := t.TempDir() + "/pool.img"
	f, err := os.Create(path)
	if err != nil { t.Fatal(err) }
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	dev, err := device.Open(path, true)
	if err != nil { t.Fatal(err) }
	defer dev.Close()

	pool, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	if _, err = dev.WriteAt(pool.Encode(), int64(format.SuperAOffset)); err != nil { t.Fatal(err) }
	pool.Generation = 2
	if _, err = dev.WriteAt(pool.Encode(), int64(format.SuperBOffset)); err != nil { t.Fatal(err) }
	if err = dev.Sync(); err != nil { t.Fatal(err) }

	pair, err := format.VolumeDescriptorPairOffset(pool, 3)
	if err != nil { t.Fatal(err) }
	slotBytes := (pool.MetadataSize-pool.ReservedOffset)/uint64(format.MaxVolumeDescriptors)/format.PageSize*format.PageSize
	arenaStart := pool.ReservedOffset/format.PageSize + 3*(slotBytes/format.PageSize)
	desc := format.VolumeDescriptor{
		VolumeID: "pool-volume-3", AG: 0, Generation: 1, VolumeSequence: 1,
		MappingRootOffset: pool.MappingOffset, BitmapSequence: 1, OwnerEpoch: 9,
		ArenaStart: arenaStart, ArenaPages: 256,
		RootPage: ^uint64(0), LifecycleStart: arenaStart + 256, LifecyclePages: 8,
	}
	image, err := desc.Encode()
	if err != nil { t.Fatal(err) }
	if err = desc.ValidatePool(pool); err != nil { t.Fatal(err) }
	for slot := uint64(0); slot < 2; slot++ {
		if _, err = dev.WriteAt(image, int64(pair+slot*format.PageSize)); err != nil { t.Fatal(err) }
	}
	if err = dev.Sync(); err != nil { t.Fatal(err) }
	store, err := metastore.OpenPoolVolume(dev, pool, 3, "pool-volume-3")
	if err != nil { t.Fatal(err) }

	// The Pool-backed Store uses absolute Pool page IDs. The Volume's Arena
	// starts at the descriptor's ArenaStart, not at a private device LBA 2.
	if err = metastore.FormatLifecycle(store); err != nil { t.Fatal(err) }
	committer := &poolImageCommitter{dev: dev, fail: true}
	readerStart, readerPages, err := store.ReaderTableRange()
	if err != nil { t.Fatal(err) }
	if readerPages != 128 { t.Fatalf("Reader Table pages=%d, want 128", readerPages) }
	if _, err = metastore.NewReaderTable(store, desc.ArenaStart, 1, committer); !errors.Is(err, metastore.ErrReaderSlot) {
		t.Fatalf("ReaderTable accepted a page inside the Arena: %v", err)
	}
	if _, err = metastore.NewReaderTable(store, readerStart, readerPages, committer); err != nil {
		t.Fatalf("ReaderTable rejected its Volume-private range: %v", err)
	}
	arena, err := metastore.NewPersistentArena(store, nil, committer)
	if err != nil { t.Fatal(err) }
	reservation, err := arena.Reserve(1)
	if err != nil { t.Fatal(err) }
	root := reservation.Pages()[0]
	page := metastore.Page{ID: root, Type: 7, Generation: 2, Payload: []byte("pool-backed-cow-root")}
	if err = arena.CommitPagesAndPublish(reservation, []metastore.Page{page}, []uint64{root}, root, 1, 0, nil); err == nil {
		t.Fatal("expected injected WAL failure")
	}
	if got := store.Superblock().Generation; got != 1 {
		t.Fatalf("failed commit changed in-memory generation: %d", got)
	}

	committer.fail = false
	if err = arena.CommitPagesAndPublish(reservation, []metastore.Page{page}, []uint64{root}, root, 1, 0, nil); err != nil {
		t.Fatal(err)
	}
	if len(committer.images) != 10 {
		t.Fatalf("expected COW page + 8 lifecycle pages + final Volume Descriptor, got %d images", len(committer.images))
	}
	if stats := arena.Stats(); stats.LivePages != 1 || stats.ReservedPages != 0 {
		t.Fatalf("committed reservation did not become Live: %+v", stats)
	}
	if err = reservation.Abort(); !errors.Is(err, metastore.ErrReservationDone) {
		t.Fatalf("committed reservation remained abortable: %v", err)
	}
	last := committer.images[len(committer.images)-1]
	if last.Offset != descPairOffset(t, pool, 3)+format.PageSize {
		t.Fatalf("root descriptor was not appended as final inactive copy: offset=%d", last.Offset)
	}
	if string(last.Data[:8]) != format.VolumeDescriptorMagic {
		t.Fatal("final WAL after-image is not BMDVOL02")
	}

	reopened, err := metastore.OpenPoolVolume(dev, pool, 3, "pool-volume-3")
	if err != nil { t.Fatal(err) }
	sb := reopened.Superblock()
	if sb.Generation != 2 || sb.RootPage != root || sb.MappingSequence != 1 {
		t.Fatalf("unexpected reopened Volume state: %+v", sb)
	}
	got, err := reopened.ReadPage(root)
	if err != nil { t.Fatal(err) }
	if string(got.Payload) != string(page.Payload) {
		t.Fatalf("unexpected COW page payload %q", got.Payload)
	}
	reopenedArena, err := metastore.NewPersistentArena(reopened, nil, &poolImageCommitter{dev: dev})
	if err != nil { t.Fatalf("reopen Pool-backed PersistentArena: %v", err) }
	if stats := reopenedArena.Stats(); stats.LivePages != 1 {
		t.Fatalf("reopened PersistentArena live pages=%d, want 1", stats.LivePages)
	}
	readerCommitter := &poolImageCommitter{dev: dev}
	readerTable, err := metastore.NewReaderTable(reopened, readerStart, 1, readerCommitter)
	if err != nil { t.Fatal(err) }
	if err = readerTable.Register(0, "node-reader", "boot-1", 1, 1); err != nil { t.Fatal(err) }
	readers, err := readerTable.Snapshot()
	if err != nil || len(readers) != 1 || readers[0].State != metastore.ReaderActive {
		t.Fatalf("Pool-backed ReaderTable snapshot=%+v err=%v", readers, err)
	}

	// Neither root publication nor COW page writes may alter Pool Superblocks.
	for _, off := range []uint64{format.SuperAOffset, format.SuperBOffset} {
		b := make([]byte, format.PageSize)
		if _, err = dev.ReadAt(b, int64(off)); err != nil { t.Fatal(err) }
		if _, err = format.DecodeSuperblock(b); err != nil {
			t.Fatalf("Pool Superblock at %d was modified: %v", off, err)
		}
	}
}

func descPairOffset(t *testing.T, pool format.Superblock, index uint32) uint64 {
	t.Helper()
	off, err := format.VolumeDescriptorPairOffset(pool, index)
	if err != nil { t.Fatal(err) }
	return off
}


// TestPoolVolumeOwnerEpochIsWALPublishedAndMonotonic verifies that owner
// takeover publishes its fencing epoch through the inactive descriptor copy.
func TestPoolVolumeOwnerEpochIsWALPublishedAndMonotonic(t *testing.T) {
	path := t.TempDir() + "/pool-owner-epoch.img"
	f, err := os.Create(path)
	if err != nil { t.Fatal(err) }
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }
	dev, err := device.Open(path, true)
	if err != nil { t.Fatal(err) }
	defer dev.Close()

	pool, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	if _, err = dev.WriteAt(pool.Encode(), int64(format.SuperAOffset)); err != nil { t.Fatal(err) }
	pool.Generation = 2
	if _, err = dev.WriteAt(pool.Encode(), int64(format.SuperBOffset)); err != nil { t.Fatal(err) }
	if err = dev.Sync(); err != nil { t.Fatal(err) }

	index := uint32(5)
	pair, err := format.VolumeDescriptorPairOffset(pool, index)
	if err != nil { t.Fatal(err) }
	slotBytes := (pool.MetadataSize-pool.ReservedOffset)/uint64(format.MaxVolumeDescriptors)/format.PageSize*format.PageSize
	arenaStart := pool.ReservedOffset/format.PageSize + uint64(index)*(slotBytes/format.PageSize)
	desc := format.VolumeDescriptor{
		VolumeID:"owner-epoch-volume", AG:0, Generation:1, VolumeSequence:1,
		MappingRootOffset:pool.MappingOffset, BitmapSequence:1, OwnerEpoch:7,
		ArenaStart:arenaStart, ArenaPages:256, RootPage:^uint64(0),
		LifecycleStart:arenaStart+256, LifecyclePages:8,
	}
	image, err := desc.Encode()
	if err != nil { t.Fatal(err) }
	for slot:=uint64(0);slot<2;slot++ {
		if _,err=dev.WriteAt(image,int64(pair+slot*format.PageSize));err!=nil{t.Fatal(err)}
	}
	if err=dev.Sync();err!=nil{t.Fatal(err)}

	store,err:=metastore.OpenPoolVolume(dev,pool,index,desc.VolumeID)
	if err!=nil{t.Fatal(err)}
	committer:=&poolImageCommitter{dev:dev}
	if got:=store.OwnerEpoch();got!=7{t.Fatalf("initial owner epoch=%d, want 7",got)}
	if err=store.AdvanceOwnerEpoch(8,committer);err!=nil{t.Fatal(err)}
	if got:=store.OwnerEpoch();got!=8{t.Fatalf("published owner epoch=%d, want 8",got)}
	if len(committer.images)!=1 || committer.images[0].Offset!=pair+format.PageSize {
		t.Fatalf("owner epoch must be one inactive descriptor after-image: %+v",committer.images)
	}
	if err=store.AdvanceOwnerEpoch(8,committer);err==nil{t.Fatal("same owner epoch was accepted")}
	if err=store.AdvanceOwnerEpoch(7,committer);err==nil{t.Fatal("stale owner epoch was accepted")}

	reopened,err:=metastore.OpenPoolVolume(dev,pool,index,desc.VolumeID)
	if err!=nil{t.Fatal(err)}
	if got:=reopened.OwnerEpoch();got!=8{t.Fatalf("reopened owner epoch=%d, want 8",got)}
}
