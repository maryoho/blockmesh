package volume

import (
	"context"
	"errors"
	"io"
	"testing"
)

// These tests model process/host restart by constructing a new store and a new
// OwnerCoordinator over the same SAN device bytes. No in-memory store state is
// carried across the restart boundary.

func newRecoveryStore(t *testing.T, dev OwnerRecordDevice) *SANOwnerRecordStore {
	t.Helper()
	store, err := NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{
		Device: dev, VolumeID: "vol-a", SlotAOffset: 4096, SlotBOffset: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestSANOwnerRecordRestartSelectsHighestDurableGeneration(t *testing.T) {
	dev := newOwnerRecordMemDevice()
	beforeRestart := newRecoveryStore(t, dev)
	first := OwnerRecord{VolumeID: "vol-a", NodeID: "node-a", BootID: "boot-a", Epoch: 7}
	second := OwnerRecord{VolumeID: "vol-a", NodeID: "node-b", BootID: "boot-b", Epoch: 8}
	if err := beforeRestart.CompareAndSwap(context.Background(), "vol-a", nil, first); err != nil {
		t.Fatal(err)
	}
	if err := beforeRestart.CompareAndSwap(context.Background(), "vol-a", &first, second); err != nil {
		t.Fatal(err)
	}

	afterRestart := newRecoveryStore(t, dev)
	got, exists, err := afterRestart.Load(context.Background(), "vol-a")
	if err != nil || !exists || got != second {
		t.Fatalf("restart load=(%+v,%v,%v), want latest record %+v", got, exists, err, second)
	}
	slot, ok, err := afterRestart.readCurrent()
	if err != nil || !ok || slot.generation != 2 {
		t.Fatalf("restart generation=(%d,%v,%v), want 2", slot.generation, ok, err)
	}
}

func TestSANOwnerRecordRestartFallsBackFromTornInactiveSlot(t *testing.T) {
	dev := newOwnerRecordMemDevice()
	firstStore := newRecoveryStore(t, dev)
	first := OwnerRecord{VolumeID: "vol-a", NodeID: "node-a", BootID: "boot-a", Epoch: 20}
	second := OwnerRecord{VolumeID: "vol-a", NodeID: "node-b", BootID: "boot-b", Epoch: 21}
	if err := firstStore.CompareAndSwap(context.Background(), "vol-a", nil, first); err != nil {
		t.Fatal(err)
	}
	dev.shortWrite = true
	if err := firstStore.CompareAndSwap(context.Background(), "vol-a", &first, second); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("interrupted inactive-slot write error=%v, want %v", err, io.ErrShortWrite)
	}
	dev.shortWrite = false

	afterRestart := newRecoveryStore(t, dev)
	got, exists, err := afterRestart.Load(context.Background(), "vol-a")
	if err != nil || !exists || got != first {
		t.Fatalf("restart did not recover prior valid slot: %+v exists=%v err=%v", got, exists, err)
	}
	third := OwnerRecord{VolumeID: "vol-a", NodeID: "node-c", BootID: "boot-c", Epoch: 22}
	if err := afterRestart.CompareAndSwap(context.Background(), "vol-a", &first, third); err != nil {
		t.Fatal(err)
	}
	got, exists, err = newRecoveryStore(t, dev).Load(context.Background(), "vol-a")
	if err != nil || !exists || got != third {
		t.Fatalf("post-recovery publish=(%+v,%v,%v)", got, exists, err)
	}
}

func TestSANOwnerRecordSyncAmbiguityIsResolvedByRestartRead(t *testing.T) {
	dev := newOwnerRecordMemDevice()
	beforeRestart := newRecoveryStore(t, dev)
	first := OwnerRecord{VolumeID: "vol-a", NodeID: "node-a", BootID: "boot-a", Epoch: 30}
	second := OwnerRecord{VolumeID: "vol-a", NodeID: "node-b", BootID: "boot-b", Epoch: 31}
	if err := beforeRestart.CompareAndSwap(context.Background(), "vol-a", nil, first); err != nil {
		t.Fatal(err)
	}
	dev.syncErr = errors.New("injected flush completion loss")
	if err := beforeRestart.CompareAndSwap(context.Background(), "vol-a", &first, second); err == nil {
		t.Fatal("ambiguous Sync result was reported as success")
	}
	dev.syncErr = nil

	// The memory device models a write that reached media before the flush
	// acknowledgement was lost. A new process must read the durable bytes and
	// must not assume the old or new record from the failed call's return.
	got, exists, err := newRecoveryStore(t, dev).Load(context.Background(), "vol-a")
	if err != nil || !exists || (got != first && got != second) {
		t.Fatalf("restart could not resolve ambiguous Sync: %+v exists=%v err=%v", got, exists, err)
	}
}

func TestSANOwnerRecordRestartFailsClosedWhenBothCopiesCorrupt(t *testing.T) {
	dev := newOwnerRecordMemDevice()
	for _, off := range []int{4096, 8192} {
		dev.data[off] = 0x7f
	}
	store := newRecoveryStore(t, dev)
	if _, exists, err := store.Load(context.Background(), "vol-a"); !errors.Is(err, ErrSANOwnerRecordCorrupt) || exists {
		t.Fatalf("corrupt record load exists=%v err=%v, want fail closed", exists, err)
	}
}

func TestOwnerRecoveryAfterCrashBetweenRecordPublishAndReservation(t *testing.T) {
	recordsA, recordsB := newIntegrationSANStores(t)
	locker := newIntegrationSanlockLocker()
	failedReservation := &integrationReservation{err: errors.New("injected PR RESERVE response loss")}
	first, err := NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker: locker, ResourceFor: func(string) string { return "pool:vol-owner:vol-a" },
		Records: recordsA, Fencer: &integrationPathFencer{paths: []string{"mpatha", "mpathb"}},
		Reservation: failedReservation,
		ReadEpoch: func(context.Context, string) (uint64, error) { return 1, nil },
		DrainOldOwner: (&integrationDrain{}).Drain, LeasePollPeriod: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.AcquireOwner(context.Background(), "vol-a", "node-a", "boot-a"); err == nil {
		t.Fatal("first Owner unexpectedly succeeded despite injected reservation response loss")
	}
	persisted, exists, err := recordsB.Load(context.Background(), "vol-a")
	if err != nil || !exists || persisted.NodeID != "node-a" || persisted.Epoch != 2 {
		t.Fatalf("durable recovery identity missing after crash point: %+v exists=%v err=%v", persisted, exists, err)
	}

	// Re-open the SAN bytes as a new process would, then recover with a new
	// coordinator. The previous PR RESERVE may have reached the array despite
	// the lost response, so this identity must be fenced on every path.
	restartedRecords := newRecoveryStore(t, recordsA.dev)
	recoveryFencer := &integrationPathFencer{paths: []string{"mpatha", "mpathb"}}
	recoveryDrain := &integrationDrain{}
	second := newIntegrationCoordinator(t, locker, restartedRecords, "node-b", "boot-b", recoveryFencer, recoveryDrain, 1)
	lease, err := second.AcquireOwner(context.Background(), "vol-a", "node-b", "boot-b")
	if err != nil {
		t.Fatalf("recovery takeover failed: %v", err)
	}
	defer lease.Close()
	if lease.Epoch() != 3 {
		t.Fatalf("recovered OwnerEpoch=%d, want 3", lease.Epoch())
	}
	calls := recoveryFencer.snapshot()
	if len(calls) != 1 || calls[0].NodeID != "node-a" || calls[0].BootID != "boot-a" {
		t.Fatalf("recovery did not fence ambiguous prior Owner: %+v", calls)
	}
	drained := recoveryDrain.snapshot()
	if len(drained) != 1 || drained[0].Epoch != 2 {
		t.Fatalf("recovery did not drain prior Owner I/O: %+v", drained)
	}
}

func TestOwnerEpochRecoveryUsesMaximumOfDescriptorAndOwnerRecord(t *testing.T) {
	tests := []struct {
		name string
		recordEpoch uint64
		descriptorEpoch uint64
		want uint64
	}{
		{name: "OwnerRecord ahead after descriptor publication interruption", recordEpoch: 17, descriptorEpoch: 12, want: 18},
		{name: "descriptor ahead of OwnerRecord", recordEpoch: 9, descriptorEpoch: 21, want: 22},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := OwnerRecord{VolumeID: "vol", NodeID: "node-a", BootID: "boot-a", Epoch: tc.recordEpoch}
			c, records, _ := newOwnerTestCoordinator(t, &old, tc.descriptorEpoch, &ownerTestFencer{}, &ownerTestBarrier{})
			got, err := c.AcquireOwner(context.Background(), "vol", "node-b", "boot-b")
			if err != nil {
				t.Fatal(err)
			}
			defer got.Close()
			if got.Epoch() != tc.want || records.record.Epoch != tc.want {
				t.Fatalf("next epoch=%d durable=%d, want %d", got.Epoch(), records.record.Epoch, tc.want)
			}
		})
	}
}

func TestOwnerRecoveryDoesNotWrapEpochAtUint64Max(t *testing.T) {
	old := OwnerRecord{VolumeID: "vol", NodeID: "node-a", BootID: "boot-a", Epoch: ^uint64(0) - 1}
	c, records, lease := newOwnerTestCoordinator(t, &old, ^uint64(0)-2, &ownerTestFencer{}, &ownerTestBarrier{})
	if _, err := c.AcquireOwner(context.Background(), "vol", "node-b", "boot-b"); !errors.Is(err, ErrOwnerEpochExhausted) {
		t.Fatalf("epoch overflow recovery error=%v", err)
	}
	if records.writes != 0 || !lease.closed {
		t.Fatal("epoch overflow modified durable OwnerRecord or leaked sanlock lease")
	}
}
