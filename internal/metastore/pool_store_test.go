package metastore_test

import (
	"errors"
	"os"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metadata"
	"blockmesh/internal/metastore"
)

type poolImageCommitter struct {
	dev device.Device
	fail bool
	images []metastore.PageImage
}

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

	desc, err := metadata.InitVolume(dev, pool, 3, "pool-volume-3", 9, 8)
	if err != nil { t.Fatal(err) }
	store, err := metastore.OpenPoolVolume(dev, pool, 3, "pool-volume-3")
	if err != nil { t.Fatal(err) }

	// The Pool-backed Store uses absolute Pool page IDs. The Volume's Arena
	// starts at the descriptor's ArenaStart, not at a private device LBA 2.
	root := desc.ArenaStart
	page := metastore.Page{ID: root, Type: 7, Generation: 2, Payload: []byte("pool-backed-cow-root")}
	committer := &poolImageCommitter{dev: dev, fail: true}
	if err = store.CommitPagesAndPublish([]metastore.Page{page}, root, 1, committer); err == nil {
		t.Fatal("expected injected WAL failure")
	}
	if got := store.Superblock().Generation; got != 1 {
		t.Fatalf("failed commit changed in-memory generation: %d", got)
	}

	committer.fail = false
	if err = store.CommitPagesAndPublish([]metastore.Page{page}, root, 1, committer); err != nil {
		t.Fatal(err)
	}
	if len(committer.images) != 2 {
		t.Fatalf("expected COW page plus final Volume Descriptor, got %d images", len(committer.images))
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
