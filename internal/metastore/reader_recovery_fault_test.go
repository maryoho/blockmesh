package metastore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type p339FaultCommitter struct {
	d Device
	mu sync.Mutex
	beforeErr error
	afterErr error
}

func (c *p339FaultCommitter) ClusterSafe() bool { return true }
func (c *p339FaultCommitter) failBefore(err error) { c.mu.Lock(); defer c.mu.Unlock(); c.beforeErr = err }
func (c *p339FaultCommitter) failAfter(err error) { c.mu.Lock(); defer c.mu.Unlock(); c.afterErr = err }

func (c *p339FaultCommitter) CommitImages(images []PageImage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.beforeErr != nil { err := c.beforeErr; c.beforeErr = nil; return err }
	for _, image := range images {
		if err := writeFullAt(c.d, image.Data, int64(image.Offset)); err != nil { return err }
	}
	if err := c.d.Sync(); err != nil { return err }
	if c.afterErr != nil { err := c.afterErr; c.afterErr = nil; return err }
	return nil
}

func p339Fixture(t *testing.T) (*memdev, *ReaderTable, *testReaderCoordinator, *p339FaultCommitter, time.Time) {
	t.Helper()
	d := newMemdev(32)
	store, err := Format(d, Layout{VolumeID: "p339-reader-recovery", TotalPages: 32, ArenaStart: 2, ArenaPages: 12, LifecycleStart: 16, LifecyclePages: 2})
	if err != nil { t.Fatal(err) }
	if err = store.Publish(NoPage, 1); err != nil { t.Fatal(err) }
	if err = FormatLifecycle(store); err != nil { t.Fatal(err) }
	coordinator := &testReaderCoordinator{}
	committer := &p339FaultCommitter{d: d}
	table, err := NewReaderTable(store, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }
	if err = table.Register(0, "node-fenced", "boot-fenced", 4, 1); err != nil { t.Fatal(err) }
	now := time.Unix(2_000_000_000, 0)
	table.mu.Lock()
	record, err := table.readLocked(0)
	if err == nil { record.HeartbeatUnixNano = now.Add(-time.Hour).UnixNano(); err = table.writeLocked(record) }
	table.mu.Unlock()
	if err != nil { t.Fatal(err) }
	return d, table, coordinator, committer, now
}

func p339Controller(table *ReaderTable, probe ReaderLeaseProbe, fencer testFenceFunc, now time.Time) *ReaderRecoveryController {
	return &ReaderRecoveryController{Table: table, Probe: probe, Fencer: fencer,
		Config: ReaderRecoveryConfig{HeartbeatTimeout: 30 * time.Second, MaxClockSkew: 2 * time.Second, Clock: func() time.Time { return now }}}
}

func p339ReadRecord(t *testing.T, table *ReaderTable) ReaderRecord {
	t.Helper()
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.store.Refresh(); err != nil { t.Fatal(err) }
	record, err := table.readLocked(0)
	if err != nil { t.Fatal(err) }
	return record
}

func TestP339RecoveryRestartsAfterFenceBeforeDurableReaderState(t *testing.T) {
	d, _, coordinator, committer, now := p339Fixture(t)
	committer.failBefore(errors.New("injected WAL failure before durable image"))
	fenceCalls := 0
	fencer := testFenceFunc(func(context.Context, string, string) error { fenceCalls++; return nil })
	report, err := p339Controller(tableForP339(t, d, coordinator, committer), &testReaderLeaseProbe{}, fencer, now).RecoverOnce(context.Background())
	if err == nil || report.Failed != 1 || fenceCalls != 1 { t.Fatalf("expected uncertain recovery failure: report=%+v err=%v calls=%d", report, err, fenceCalls) }

	reopened, err := Open(d, "p339-reader-recovery")
	if err != nil { t.Fatal(err) }
	restartedTable, err := NewReaderTable(reopened, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }
	if got := p339ReadRecord(t, restartedTable); got.State != ReaderActive { t.Fatalf("failed durable transition must remain pinned: %+v", got) }

	report, err = p339Controller(restartedTable, &testReaderLeaseProbe{}, fencer, now).RecoverOnce(context.Background())
	if err != nil || report.Fenced != 1 || fenceCalls != 2 { t.Fatalf("restart must retry fencing: report=%+v err=%v calls=%d", report, err, fenceCalls) }
	if got := p339ReadRecord(t, restartedTable); got.State != ReaderFenced { t.Fatalf("reader not durably fenced: %+v", got) }
}

func TestP339RecoveryRestartsAfterDurableReaderStateBeforeAcknowledgement(t *testing.T) {
	d, table, coordinator, committer, now := p339Fixture(t)
	committer.failAfter(errors.New("injected lost WAL acknowledgement"))
	fenceCalls := 0
	fencer := testFenceFunc(func(context.Context, string, string) error { fenceCalls++; return nil })
	report, err := p339Controller(table, &testReaderLeaseProbe{}, fencer, now).RecoverOnce(context.Background())
	if err == nil || report.Failed != 1 || fenceCalls != 1 { t.Fatalf("expected lost acknowledgement: report=%+v err=%v calls=%d", report, err, fenceCalls) }

	reopened, err := Open(d, "p339-reader-recovery")
	if err != nil { t.Fatal(err) }
	restartedTable, err := NewReaderTable(reopened, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }
	if got := p339ReadRecord(t, restartedTable); got.State != ReaderFenced { t.Fatalf("durable Fenced image was lost: %+v", got) }

	report, err = p339Controller(restartedTable, &testReaderLeaseProbe{}, fencer, now).RecoverOnce(context.Background())
	if err != nil || report.Fenced != 0 || fenceCalls != 1 { t.Fatalf("restart must not refence durable reader: report=%+v err=%v calls=%d", report, err, fenceCalls) }
}

func tableForP339(t *testing.T, d *memdev, coordinator *testReaderCoordinator, committer *p339FaultCommitter) *ReaderTable {
	t.Helper()
	store, err := Open(d, "p339-reader-recovery")
	if err != nil { t.Fatal(err) }
	table, err := NewReaderTable(store, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }
	return table
}

type p339ExclusiveProbe struct {
	mu sync.Mutex
	attempts chan struct{}
	firstAcquired chan struct{}
	allowFirstReturn chan struct{}
	count atomic.Int32
}

func (p *p339ExclusiveProbe) AcquireExpiredReader(context.Context, string, ReaderRecord) (func() error, error) {
	p.attempts <- struct{}{}
	p.mu.Lock()
	if p.count.Add(1) == 1 {
		close(p.firstAcquired)
		<-p.allowFirstReturn
	}
	return func() error { p.mu.Unlock(); return nil }, nil
}

func TestP339TwoRecoveryControllersFenceOneReaderOnlyOnce(t *testing.T) {
	d, tableA, coordinator, committer, now := p339Fixture(t)
	storeB, err := Open(d, "p339-reader-recovery")
	if err != nil { t.Fatal(err) }
	tableB, err := NewReaderTable(storeB, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }

	probe := &p339ExclusiveProbe{
		attempts: make(chan struct{}, 2),
		firstAcquired: make(chan struct{}),
		allowFirstReturn: make(chan struct{}),
	}
	var fenceCalls atomic.Int32
	fencer := testFenceFunc(func(context.Context, string, string) error {
		fenceCalls.Add(1)
		return nil
	})
	controllerA := p339Controller(tableA, probe, fencer, now)
	controllerB := p339Controller(tableB, probe, fencer, now)

	doneA := make(chan error, 1)
	go func() { _, err := controllerA.RecoverOnce(context.Background()); doneA <- err }()
	select {
	case <-probe.firstAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("first controller did not acquire the slot lease")
	}
	<-probe.attempts

	// The first controller deliberately pauses after acquiring the slot
	// lease but before taking the reader-set barrier. The second controller
	// can therefore snapshot the same stale record and contend for that lease.
	doneB := make(chan error, 1)
	go func() { _, err := controllerB.RecoverOnce(context.Background()); doneB <- err }()
	select {
	case <-probe.attempts:
	case <-time.After(3 * time.Second):
		close(probe.allowFirstReturn)
		t.Fatal("second controller did not reach the slot lease probe")
	}
	close(probe.allowFirstReturn)

	select {
	case err = <-doneA:
		if err != nil { t.Fatalf("first recovery failed: %v", err) }
	case <-time.After(3 * time.Second):
		t.Fatal("first recovery deadlocked")
	}
	select {
	case <-doneB: // ErrReaderIdentity is expected after the first Fenced commit.
	case <-time.After(3 * time.Second):
		t.Fatal("second recovery deadlocked")
	}
	if got := fenceCalls.Load(); got != 1 { t.Fatalf("same reader fenced %d times, want once", got) }
	if got := p339ReadRecord(t, tableA); got.State != ReaderFenced { t.Fatalf("reader not durably fenced: %+v", got) }
}


func TestP339PRFencingFailureKeepsReaderGenerationPinned(t *testing.T) {
	d, table, _, _, now := p339Fixture(t)
	_ = d
	fenceErr := errors.New("injected PR PREEMPT AND ABORT/path verification failure")
	fencer := testFenceFunc(func(context.Context, string, string) error { return fenceErr })
	controller := p339Controller(table, &testReaderLeaseProbe{}, fencer, now)
	report, err := controller.RecoverOnce(context.Background())
	if err == nil || report.Failed != 1 { t.Fatalf("PR failure must be reported: report=%+v err=%v", report, err) }
	if got := p339ReadRecord(t, table); got.State != ReaderActive {
		t.Fatalf("PR failure must leave reader generation-pinning: %+v", got)
	}

	fencer = testFenceFunc(func(context.Context, string, string) error { return nil })
	controller.Fencer = fencer
	report, err = controller.RecoverOnce(context.Background())
	if err != nil || report.Fenced != 1 { t.Fatalf("successful retry did not fence reader: report=%+v err=%v", report, err) }
	if got := p339ReadRecord(t, table); got.State != ReaderFenced { t.Fatalf("reader not durably fenced after retry: %+v", got) }
}

func TestP339DrainFailureRetainsRetireQueueAndRetries(t *testing.T) {
	d := newMemdev(32)
	store, err := Format(d, Layout{VolumeID: "p339-drain-retry", TotalPages: 32, ArenaStart: 2, ArenaPages: 12, LifecycleStart: 16, LifecyclePages: 2})
	if err != nil { t.Fatal(err) }
	if err = store.WritePage(Page{ID: 2, Type: 1, Generation: 1, Payload: []byte("old-root")}); err != nil { t.Fatal(err) }
	if err = store.Sync(); err != nil { t.Fatal(err) }
	if err = store.Publish(2, 1); err != nil { t.Fatal(err) }
	if err = FormatLifecycle(store); err != nil { t.Fatal(err) }

	committer := testCommitter{d: d}
	coordinator := &testReaderCoordinator{}
	arena, err := NewPersistentArena(store, []uint64{2}, committer, coordinator)
	if err != nil { t.Fatal(err) }
	table, err := NewReaderTable(store, 18, 2, committer, coordinator)
	if err != nil { t.Fatal(err) }
	reservation, err := arena.Reserve(1)
	if err != nil { t.Fatal(err) }
	newRoot := reservation.Pages()[0]
	if err = arena.CommitPagesAndPublish(reservation,
		[]Page{{ID: newRoot, Type: 1, Generation: 2, Payload: []byte("new-root")}},
		[]uint64{newRoot}, newRoot, 2, 1, []uint64{2}); err != nil { t.Fatal(err) }

	drain := &recordingMetadataDrain{err: errors.New("injected DM metadata I/O drain timeout")}
	controller := &ReaderRecoveryController{
		Table: table, Probe: &testReaderLeaseProbe{},
		Fencer: testFenceFunc(func(context.Context, string, string) error { return nil }),
		Arena: arena, Drain: drain,
		Config: ReaderRecoveryConfig{HeartbeatTimeout: time.Minute},
	}
	report, err := controller.RecoverOnce(context.Background())
	if err == nil || report.ReclaimedPages != 0 || len(arena.Pending()) == 0 {
		t.Fatalf("failed drain must retain retire queue: report=%+v err=%v pending=%+v", report, err, arena.Pending())
	}

	drain.err = nil
	report, err = controller.RecoverOnce(context.Background())
	if err != nil || report.ReclaimedPages != 1 || len(arena.Pending()) != 0 {
		t.Fatalf("drain retry did not reclaim pending page: report=%+v err=%v pending=%+v", report, err, arena.Pending())
	}
}
