package metadata

import (
	"os"
	"path/filepath"
	"testing"

	"bblockmesh/internal/format"
)

func TestInitInspectBitmap(t *testing.T) {
	p := filepath.Join(t.TempDir(), "metadata.img")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(int64(format.MetadataSize)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err = Init(p, 4096, false); err != nil {
		t.Fatal(err)
	}
	s, err := OpenWritable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, free, err := s.BitmapStats(0)
	if err != nil {
		t.Fatal(err)
	}
	if a != 0 || free != format.AGBlocks {
		t.Fatalf("initial bitmap: allocated=%d free=%d", a, free)
	}
	if err = s.SetBitmapBit(0, 12345, true); err != nil {
		t.Fatal(err)
	}
	a, free, err = s.BitmapStats(0)
	if err != nil {
		t.Fatal(err)
	}
	if a != 1 || free != format.AGBlocks-1 {
		t.Fatalf("after allocation: allocated=%d free=%d", a, free)
	}
	if err = s.SetBitmapBit(0, 12345, false); err != nil {
		t.Fatal(err)
	}
	a, free, err = s.BitmapStats(0)
	if err != nil {
		t.Fatal(err)
	}
	if a != 0 || free != format.AGBlocks {
		t.Fatalf("after free: allocated=%d free=%d", a, free)
	}
}
