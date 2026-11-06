package wal

import (
	"errors"
	"os"
	"sync"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

// crashDevice injects a one-shot I/O failure at an exact device boundary.
// after=true performs the underlying operation first, then returns the error;
// this models an operation whose completion is uncertain to the caller.
type crashDevice struct {
	mu sync.Mutex
	inner device.Device
	failOp string
	failAt int
	after bool
	writes int
	syncs int
	fired bool
}

func (d *crashDevice) arm(op string, at int, after bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failOp, d.failAt, d.after = op, at, after
	d.writes, d.syncs, d.fired = 0, 0, false
}
func (d *crashDevice) disarm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failOp, d.failAt, d.fired = "", 0, false
}
func (d *crashDevice) shouldFail(op string, n int) (bool, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fired || d.failOp != op || d.failAt != n { return false, false }
	d.fired = true
	return true, d.after
}
func (d *crashDevice) ReadAt(p []byte, off int64) (int, error) {
	return d.inner.ReadAt(p, off)
}
func (d *crashDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	d.writes++
	n := d.writes
	d.mu.Unlock()
	fail, after := d.shouldFail("write", n)
	if fail && !after { return 0, errors.New("injected crash: write") }
	written, err := d.inner.WriteAt(p, off)
	if fail { return written, errors.New("injected crash: write completion uncertain") }
	return written, err
}
func (d *crashDevice) Sync() error {
	d.mu.Lock()
	d.syncs++
	n := d.syncs
	d.mu.Unlock()
	fail, after := d.shouldFail("sync", n)
	if fail && !after { return errors.New("injected crash: sync") }
	err := d.inner.Sync()
	if fail { return errors.New("injected crash: sync completion uncertain") }
	return err
}
func (d *crashDevice) Size() int64 { return d.inner.Size() }
func (d *crashDevice) Close() error { return d.inner.Close() }

type crashPoint struct {
	name string
	op string
	at int
	after bool
	committed bool
}

func TestJournalCrashInjectionRecoveryMatrix(t *testing.T) {
	points := []crashPoint{
		{name:"before-intent-write", op:"write", at:1, committed:false},
		{name:"after-intent-write", op:"write", at:1, after:true, committed:false},
		{name:"intent-sync-uncertain", op:"sync", at:1, after:true, committed:false},
		{name:"before-commit-write", op:"write", at:2, committed:false},
		{name:"commit-sync-durable-but-error", op:"sync", at:2, after:true, committed:true},
		{name:"before-first-after-image", op:"write", at:3, committed:true},
		{name:"after-images-sync-durable-but-error", op:"sync", at:3, after:true, committed:true},
	}

	for _, point := range points {
		t.Run(point.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "wal-crash-*.img")
			if err != nil { t.Fatal(err) }
			if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
			inner, err := device.Open(f.Name(), true)
			if err != nil { t.Fatal(err) }
			defer inner.Close()
			if err = f.Close(); err != nil { t.Fatal(err) }

			sb, err := format.NewSuperblock(0)
			if err != nil { t.Fatal(err) }
			fd := &crashDevice{inner:inner}
			j, err := New(fd, sb, 0)
			if err != nil { t.Fatal(err) }

			target := sb.MappingOffset
			image := make([]byte, format.PageSize)
			copy(image, []byte("allocation-mapping"))
			fd.arm(point.op, point.at, point.after)
			commitErr := j.Commit([]PageUpdate{{Offset:target, Image:image}})
			if commitErr == nil { t.Fatal("injected failure was not observed") }
			fd.disarm()

			// Reopening the journal is the recovery boundary. New must replay
			// every durable Commit before returning a usable Journal.
			reopened, err := New(fd, sb, 0)
			if err != nil { t.Fatalf("reopen/recover: %v", err) }
			_ = reopened
			got := make([]byte, format.PageSize)
			if n, readErr := fd.ReadAt(got, int64(target)); readErr != nil || n != len(got) {
				t.Fatalf("read target: n=%d err=%v", n, readErr)
			}
			if point.committed {
				if string(got[:18]) != "allocation-mapping" {
					t.Fatalf("committed allocation mapping not replayed: %q", got[:18])
				}
			} else {
				for i, b := range got {
					if b != 0 { t.Fatalf("uncommitted transaction changed target byte %d to %#x", i, b) }
				}
			}
		})
	}
}
