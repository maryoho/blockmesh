package wal

import (
	"os"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

func TestCommitAndRecoverRedo(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-*.img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil {
		t.Fatal(err)
	}
	d, err := device.Open(f.Name(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	sb, err := format.NewSuperblock(0)
	if err != nil {
		t.Fatal(err)
	}
	j, err := New(d, sb, 0)
	if err != nil {
		t.Fatal(err)
	}
	target := sb.MappingOffset
	page := make([]byte, format.PageSize)
	copy(page, []byte("mapping-v1"))
	if err = j.Commit([]PageUpdate{{Offset: target, Image: page}}); err != nil {
		t.Fatal(err)
	}
	// Simulate a committed-but-not-applied transaction by restoring the old target.
	if _, err = d.WriteAt(make([]byte, format.PageSize), int64(target)); err != nil {
		t.Fatal(err)
	}
	if err = d.Sync(); err != nil {
		t.Fatal(err)
	}
	recovered, err := j.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("expected committed WAL to replay")
	}
	got := make([]byte, format.PageSize)
	if _, err = d.ReadAt(got, int64(target)); err != nil {
		t.Fatal(err)
	}
	if string(got[:10]) != "mapping-v1" {
		t.Fatalf("bad recovered page: %q", got[:10])
	}
}
