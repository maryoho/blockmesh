package allocator

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"bblockmesh/internal/format"
	"bblockmesh/internal/lock"
	"bblockmesh/internal/metadata"
)

func TestAllocateOneUpdatesBitmapAndMapping(t *testing.T) {
	path := t.TempDir() + "/metadata.img"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(int64(format.MetadataSize)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err = metadata.Init(path, 0, true); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.OpenWritable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	locks, err := lock.NewFileLocker(t.TempDir() + "/locks")
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(store, locks)
	if err != nil {
		t.Fatal(err)
	}

	mappingOffset := store.SB.MappingOffset
	volumeLease, err := locks.Acquire(context.Background(), "volume:vol-test")
	if err != nil {
		t.Fatal(err)
	}
	defer volumeLease.Close()
	physical, err := a.AllocateOne(context.Background(), volumeLease, "vol-test", 0, mappingOffset,
		func(old []byte, physical uint64) ([]byte, error) {
			next := append([]byte(nil), old...)
			copy(next[:8], []byte("BMMAP001"))
			binary.LittleEndian.PutUint64(next[8:16], 123) // logical block
			binary.LittleEndian.PutUint64(next[16:24], physical)
			return next, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if physical != 0 {
		t.Fatalf("first allocation should be block 0, got %d", physical)
	}
	allocated, free, err := store.BitmapStats(0)
	if err != nil {
		t.Fatal(err)
	}
	if allocated != 1 || free != format.AGBlocks-1 {
		t.Fatalf("unexpected bitmap stats: allocated=%d free=%d", allocated, free)
	}
	page, err := store.ReadPage(mappingOffset)
	if err != nil {
		t.Fatal(err)
	}
	if string(page[:8]) != "BMMAP001" || binary.LittleEndian.Uint64(page[16:24]) != physical {
		t.Fatal("mapping page not committed")
	}
}

func TestFindFree(t *testing.T) {
	bm := []byte{0xff, 0xfb}
	got, ok := findFree(bm)
	if !ok || got != 10 {
		t.Fatalf("got %d, %v; want 10, true", got, ok)
	}
}

func TestConcurrentAllocationsSameAGAreUnique(t *testing.T) {
	path := t.TempDir() + "/metadata.img"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(int64(format.MetadataSize)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err = metadata.Init(path, 0, true); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.OpenWritable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	locks, err := lock.NewFileLocker(t.TempDir() + "/locks")
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(store, locks)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		p   uint64
		err error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			off := store.SB.MappingOffset + uint64(i)*format.PageSize
			volumeID := fmt.Sprintf("vol-%d", i)
			volumeLease, le := locks.Acquire(context.Background(), "volume:"+volumeID)
			if le != nil {
				results <- result{err: le}
				return
			}
			defer volumeLease.Close()
			p, e := a.AllocateOne(context.Background(), volumeLease, volumeID, 0, off,
				func(old []byte, physical uint64) ([]byte, error) {
					next := append([]byte(nil), old...)
					binary.LittleEndian.PutUint64(next[:8], physical)
					return next, nil
				})
			results <- result{p: p, err: e}
		}()
	}
	r1, r2 := <-results, <-results
	if r1.err != nil {
		t.Fatal(r1.err)
	}
	if r2.err != nil {
		t.Fatal(r2.err)
	}
	if r1.p == r2.p {
		t.Fatalf("duplicate physical allocation: %d", r1.p)
	}
	allocated, _, err := store.BitmapStats(0)
	if err != nil {
		t.Fatal(err)
	}
	if allocated != 2 {
		t.Fatalf("expected 2 allocated blocks, got %d", allocated)
	}
}
