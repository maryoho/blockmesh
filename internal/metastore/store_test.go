package metastore

import (
	"errors"
	"io"
	"sync"
	"testing"
)

type memdev struct {
	mu    sync.Mutex
	b     []byte
	syncs int
}

func newMemdev(pages uint64) *memdev { return &memdev{b: make([]byte, int(pages)*PageSize)} }
func (d *memdev) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off >= int64(len(d.b)) { return 0, io.EOF }
	n := copy(p, d.b[off:])
	if n < len(p) { return n, io.EOF }
	return n, nil
}
func (d *memdev) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(d.b)) { return 0, io.ErrShortWrite }
	return copy(d.b[off:], p), nil
}
func (d *memdev) Sync() error { d.mu.Lock(); defer d.mu.Unlock(); d.syncs++; return nil }
func (d *memdev) corrupt(off int) { d.mu.Lock(); defer d.mu.Unlock(); d.b[off] ^= 0xff }

func TestFormatOpenAndPublish(t *testing.T) {
	d := newMemdev(128)
	s, err := Format(d, Layout{VolumeID: "vol-1", TotalPages: 128, ArenaStart: 2, ArenaPages: 126})
	if err != nil { t.Fatal(err) }
	if got := s.Superblock().Generation; got != 1 { t.Fatalf("generation=%d", got) }
	if err = s.WritePage(Page{ID: 2, Type: 7, Generation: 2, Payload: []byte("metadata-page")}); err != nil { t.Fatal(err) }
	if err = s.Sync(); err != nil { t.Fatal(err) }
	if err = s.Publish(2, 1); err != nil { t.Fatal(err) }
	s2, err := Open(d, "vol-1")
	if err != nil { t.Fatal(err) }
	sb := s2.Superblock()
	if sb.Generation != 2 || sb.RootPage != 2 || sb.MappingSequence != 1 { t.Fatalf("bad superblock: %+v", sb) }
	p, err := s2.ReadPage(2)
	if err != nil { t.Fatal(err) }
	if string(p.Payload) != "metadata-page" || p.Type != 7 || p.Generation != 2 { t.Fatalf("bad page: %+v", p) }
}

func TestOpenFallsBackToPreviousSuperblock(t *testing.T) {
	d := newMemdev(32)
	s, err := Format(d, Layout{VolumeID: "v", TotalPages: 32, ArenaStart: 2, ArenaPages: 30})
	if err != nil { t.Fatal(err) }
	if err = s.Publish(NoPage, 1); err != nil { t.Fatal(err) }
	d.corrupt(0) // newest copy is slot 0; slot 1 generation 1 remains valid
	s2, err := Open(d, "v")
	if err != nil { t.Fatal(err) }
	if got := s2.Superblock().Generation; got != 1 { t.Fatalf("expected fallback generation 1, got %d", got) }
}

func TestPageChecksumAndBounds(t *testing.T) {
	d := newMemdev(16)
	s, err := Format(d, Layout{VolumeID: "v", TotalPages: 16, ArenaStart: 2, ArenaPages: 14})
	if err != nil { t.Fatal(err) }
	if err = s.WritePage(Page{ID: 3, Type: 1, Payload: []byte("x")}); err != nil { t.Fatal(err) }
	d.corrupt(3*PageSize + PageHeaderSize)
	if _, err = s.ReadPage(3); !errors.Is(err, ErrCorrupt) { t.Fatalf("expected corruption, got %v", err) }
	if err = s.WritePage(Page{ID: 1, Type: 1}); !errors.Is(err, ErrOutOfRange) { t.Fatalf("expected bounds error, got %v", err) }
	if err = s.Publish(1, 1); !errors.Is(err, ErrOutOfRange) { t.Fatalf("expected invalid root error, got %v", err) }
}

func TestFormatRejectsInvalidGeometry(t *testing.T) {
	d := newMemdev(16)
	if _, err := Format(d, Layout{VolumeID: "v", TotalPages: 16, ArenaStart: 1, ArenaPages: 15}); !errors.Is(err, ErrGeometry) {
		t.Fatalf("expected geometry error, got %v", err)
	}
}

func TestFormatInvalidatesStaleSuperblocks(t *testing.T) {
	d := newMemdev(32)
	stale, err := encodeSuperblock(Superblock{VolumeID: "v", Generation: 99, TotalPages: 32, ArenaStart: 2, ArenaPages: 30, RootPage: NoPage})
	if err != nil { t.Fatal(err) }
	if _, err = d.WriteAt(stale, 0); err != nil { t.Fatal(err) }
	if _, err = d.WriteAt(stale, PageSize); err != nil { t.Fatal(err) }
	if _, err = Format(d, Layout{VolumeID: "v", TotalPages: 32, ArenaStart: 2, ArenaPages: 30}); err != nil { t.Fatal(err) }
	s, err := Open(d, "v")
	if err != nil { t.Fatal(err) }
	if got := s.Superblock().Generation; got != 1 { t.Fatalf("stale generation survived format: %d", got) }
}

func TestPublishRequiresIncreasingMappingSequence(t *testing.T) {
	d := newMemdev(16)
	s, err := Format(d, Layout{VolumeID: "v", TotalPages: 16, ArenaStart: 2, ArenaPages: 14})
	if err != nil { t.Fatal(err) }
	if err = s.Publish(NoPage, 0); err == nil { t.Fatal("expected non-increasing sequence rejection") }
}
