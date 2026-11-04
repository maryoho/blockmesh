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


func TestOpenAutomaticallyReplaysCommittedTransactions(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-reopen-*.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	d, err := device.Open(f.Name(), true)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	sb, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	j, err := New(d, sb, 0)
	if err != nil { t.Fatal(err) }
	target := sb.MappingOffset
	image := make([]byte, format.PageSize)
	copy(image, []byte("durable-commit"))
	if err = j.Commit([]PageUpdate{{Offset: target, Image: image}}); err != nil { t.Fatal(err) }

	// Model a crash after Commit became durable but before the target page was
	// applied (or after a torn/incorrect application).
	if n, writeErr := d.WriteAt(make([]byte, format.PageSize), int64(target)); writeErr != nil || n != int(format.PageSize) {
		t.Fatalf("clear target: n=%d err=%v", n, writeErr)
	}
	if err = d.Sync(); err != nil { t.Fatal(err) }

	// Opening the journal must replay before returning it to the caller.
	if _, err = New(d, sb, 0); err != nil { t.Fatalf("New must recover before returning: %v", err) }
	got := make([]byte, format.PageSize)
	if n, readErr := d.ReadAt(got, int64(target)); readErr != nil || n != len(got) {
		t.Fatalf("read target: n=%d err=%v", n, readErr)
	}
	if string(got[:14]) != "durable-commit" {
		t.Fatalf("committed page was not replayed: %q", got[:14])
	}
}

func TestOpenIgnoresIntentWithoutCommit(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-uncommitted-*.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	d, err := device.Open(f.Name(), true)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	sb, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	target := sb.MappingOffset
	image := make([]byte, format.PageSize)
	copy(image, []byte("not-committed"))
	payload, err := encodeUpdates([]PageUpdate{{Offset: target, Image: image}})
	if err != nil { t.Fatal(err) }
	record, err := encodeJournalRecord(journalIntent, 1, payload)
	if err != nil { t.Fatal(err) }
	if n, writeErr := d.WriteAt(record, int64(sb.WALOffset)); writeErr != nil || n != len(record) {
		t.Fatalf("write intent: n=%d err=%v", n, writeErr)
	}
	if err = d.Sync(); err != nil { t.Fatal(err) }

	if _, err = New(d, sb, 0); err != nil { t.Fatal(err) }
	got := make([]byte, format.PageSize)
	if n, readErr := d.ReadAt(got, int64(target)); readErr != nil || n != len(got) {
		t.Fatalf("read target: n=%d err=%v", n, readErr)
	}
	for i, b := range got {
		if b != 0 { t.Fatalf("uncommitted intent modified target at byte %d", i) }
	}
}
