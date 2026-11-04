package wal

import (
	"errors"
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
	j, err := New(d, sb, 0) // initialize the dual checkpoint descriptors
	if err != nil { t.Fatal(err) }
	target := sb.MappingOffset
	image := make([]byte, format.PageSize)
	copy(image, []byte("not-committed"))
	payload, err := encodeUpdates([]PageUpdate{{Offset: target, Image: image}})
	if err != nil { t.Fatal(err) }
	record, err := encodeJournalRecord(journalIntent, 1, payload)
	if err != nil { t.Fatal(err) }
	if n, writeErr := d.WriteAt(record, int64(j.base)); writeErr != nil || n != len(record) {
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


func TestCheckpointSwitchesBanksAndRecoversAfterReopen(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-checkpoint-*.img")
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
	image1 := make([]byte, format.PageSize)
	copy(image1, []byte("before-checkpoint"))
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset, Image: image1}}); err != nil { t.Fatal(err) }
	oldBank := j.activeBank
	oldGeneration := j.checkpointGeneration
	if err = j.Checkpoint(); err != nil { t.Fatal(err) }
	if j.activeBank == oldBank || j.checkpointGeneration != oldGeneration+1 || j.checkpointTxID != 1 || j.end != 0 {
		t.Fatalf("checkpoint state not advanced: bank=%d generation=%d tx=%d end=%d", j.activeBank, j.checkpointGeneration, j.checkpointTxID, j.end)
	}
	image2 := make([]byte, format.PageSize)
	copy(image2, []byte("after-checkpoint"))
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset+format.PageSize, Image: image2}}); err != nil { t.Fatal(err) }

	// A fresh Journal must select the newer descriptor and replay only its
	// active bank, retaining both checkpointed and post-checkpoint data.
	reopened, err := New(d, sb, 0)
	if err != nil { t.Fatal(err) }
	if reopened.activeBank != j.activeBank || reopened.checkpointTxID != 1 {
		t.Fatalf("reopen selected wrong checkpoint: bank=%d tx=%d", reopened.activeBank, reopened.checkpointTxID)
	}
	got := make([]byte, format.PageSize)
	if n, readErr := d.ReadAt(got, int64(sb.MappingOffset)); readErr != nil || n != len(got) { t.Fatalf("read first page: %d %v", n, readErr) }
	if string(got[:17]) != "before-checkpoint" { t.Fatalf("checkpointed data lost: %q", got[:17]) }
	if n, readErr := d.ReadAt(got, int64(sb.MappingOffset+format.PageSize)); readErr != nil || n != len(got) { t.Fatalf("read second page: %d %v", n, readErr) }
	if string(got[:16]) != "after-checkpoint" { t.Fatalf("post-checkpoint data lost: %q", got[:16]) }
}

func TestCheckpointDescriptorMirrorSurvivesSingleSlotCorruption(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-checkpoint-torn-*.img")
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
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset, Image: make([]byte, format.PageSize)}}); err != nil { t.Fatal(err) }
	if err = j.Checkpoint(); err != nil { t.Fatal(err) }
	// Both descriptor slots are mirrored before Checkpoint returns. Corrupt
	// one copy and verify the other still selects the current active bank.
	newestOffset, err := checkpointOffset(sb, 0, j.checkpointSlot)
	if err != nil { t.Fatal(err) }
	bad := make([]byte, format.PageSize)
	if n, writeErr := d.WriteAt(bad, int64(newestOffset)); writeErr != nil || n != len(bad) { t.Fatalf("corrupt descriptor: %d %v", n, writeErr) }
	if err = d.Sync(); err != nil { t.Fatal(err) }
	reopened, err := New(d, sb, 0)
	if err != nil { t.Fatal(err) }
	if reopened.activeBank != j.activeBank || reopened.checkpointTxID != j.checkpointTxID {
		t.Fatal("remaining descriptor copy did not preserve current checkpoint")
	}
}


func TestJournalFullCanCheckpointAndRetry(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-full-checkpoint-*.img")
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
	j.end = j.size - 1 // leave insufficient room for the next record
	image := make([]byte, format.PageSize)
	copy(image, []byte("retry-after-cp"))
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset, Image: image}}); err != ErrJournalFull {
		t.Fatalf("expected ErrJournalFull, got %v", err)
	}
	if j.poisoned { t.Fatal("clean WAL-full condition must not poison the Journal") }
	if err = j.Checkpoint(); err != nil { t.Fatalf("checkpoint after full: %v", err) }
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset, Image: image}}); err != nil {
		t.Fatalf("commit after checkpoint: %v", err)
	}
}


func TestOpenRepairsDescriptorAfterInterruptedMirror(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-checkpoint-repair-*.img")
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
	if err = j.Commit([]PageUpdate{{Offset: sb.MappingOffset, Image: make([]byte, format.PageSize)}}); err != nil { t.Fatal(err) }
	if err = j.Checkpoint(); err != nil { t.Fatal(err) }

	// Model power loss after the new descriptor was durable but before its
	// mirror was written: restore the other slot to the previous generation.
	stale := checkpointDescriptor{Generation: 1, CheckpointTxID: 0, ActiveBank: 0, AG: 0}
	staleOffset, err := checkpointOffset(sb, 0, uint32(1)-j.checkpointSlot)
	if err != nil { t.Fatal(err) }
	if n, writeErr := d.WriteAt(encodeCheckpoint(stale), int64(staleOffset)); writeErr != nil || n != int(format.PageSize) {
		t.Fatalf("write stale descriptor: %d %v", n, writeErr)
	}
	if err = d.Sync(); err != nil { t.Fatal(err) }

	reopened, err := New(d, sb, 0)
	if err != nil { t.Fatal(err) }
	otherOffset, err := checkpointOffset(sb, 0, uint32(1)-reopened.checkpointSlot)
	if err != nil { t.Fatal(err) }
	var a, b [format.PageSize]byte
	if n, readErr := d.ReadAt(a[:], int64(checkpointOffsetMust(t, sb, 0, reopened.checkpointSlot))); readErr != nil || n != len(a) {
		t.Fatalf("read selected descriptor: %d %v", n, readErr)
	}
	if n, readErr := d.ReadAt(b[:], int64(otherOffset)); readErr != nil || n != len(b) {
		t.Fatalf("read repaired descriptor: %d %v", n, readErr)
	}
	if string(a[:]) != string(b[:]) { t.Fatal("open did not repair the stale descriptor mirror") }
}

func checkpointOffsetMust(t *testing.T, sb format.Superblock, ag, slot uint32) uint64 {
	t.Helper()
	off, err := checkpointOffset(sb, ag, slot)
	if err != nil { t.Fatal(err) }
	return off
}


func TestMissingDescriptorsNeverAssumeInactiveBankIsEmpty(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-missing-checkpoint-*.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	d, err := device.Open(f.Name(), true)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	sb, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	// Simulate a WAL whose active bank was bank 1 but whose descriptors were
	// lost. Bank 0 is blank, so probing only the first bank would lose data.
	marker := []byte("live-bank-one")
	bank1Offset := sb.WALOffset + walBankSize + journalHeaderSize
	if n, writeErr := d.WriteAt(marker, int64(bank1Offset)); writeErr != nil || n != len(marker) {
		t.Fatalf("write bank1 marker: %d %v", n, writeErr)
	}
	if err = d.Sync(); err != nil { t.Fatal(err) }
	if _, err = New(d, sb, 0); !errors.Is(err, ErrCheckpointMissing) {
		t.Fatalf("expected ErrCheckpointMissing, got %v", err)
	}
}
