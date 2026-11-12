package dynamicruntime

import (
	"context"
	"errors"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
)

type testDistributedLocker struct{}

func (testDistributedLocker) Distributed() bool { return true }
func (testDistributedLocker) Acquire(context.Context, string) (lock.Lease, error) {
	return nil, errors.New("unexpected Acquire")
}
func (testDistributedLocker) Lock(context.Context, string) (func() error, error) {
	return func() error { return nil }, nil
}

type testReaderCoordinator struct{}
func (testReaderCoordinator) LockReaderSet(string)(func(),error){return func(){},nil}

func TestNewRequiresDistributedLockerAndResourceNaming(t *testing.T) {
	open := func(context.Context, metastore.VolumeRuntimeSpec) (device.Device, error) {
		return nil, errors.New("unexpected device open")
	}
	if _, err := New(Config{OpenDevice: open}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid config without distributed locker, got %v", err)
	}
	if _, err := New(Config{OpenDevice: open, Locker: testDistributedLocker{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid config without WAL resource resolver, got %v", err)
	}
	if _, err := New(Config{
		OpenDevice: open,
		Locker: testDistributedLocker{},
		WALResourceForAG: func([16]byte, uint32) string { return "pool:uuid:ag:0:wal" },
		ReaderCoordinator: testReaderCoordinator{},
	}); err != nil {
		t.Fatalf("valid distributed configuration rejected: %v", err)
	}
}

func TestOpenReadWriteRejectsMissingOwnerBeforeOpeningDevice(t *testing.T) {
	opened := false
	backend, err := New(Config{
		OpenDevice: func(context.Context, metastore.VolumeRuntimeSpec) (device.Device, error) {
			opened = true
			return nil, errors.New("device must not be opened")
		},
		Locker: testDistributedLocker{},
		WALResourceForAG: func([16]byte, uint32) string { return "pool:uuid:ag:0:wal" },
		ReaderCoordinator: testReaderCoordinator{},
	})
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{
		VolumeID: "vol-1", Mode: metastore.VolumeRuntimeReadWrite,
		NodeID: "node-1", BootID: "boot-1", VolumeIndex: 0,
	}
	if _, err = backend.OpenReadWrite(context.Background(), spec, nil); !errors.Is(err, ErrOwnerProof) {
		t.Fatalf("expected missing Owner proof rejection, got %v", err)
	}
	if opened { t.Fatal("backend opened the parent LUN before validating the Owner proof") }
}

func TestOpenReadWriteRejectsReadOnlySpec(t *testing.T) {
	opened := false
	backend, err := New(Config{
		OpenDevice: func(context.Context, metastore.VolumeRuntimeSpec) (device.Device, error) {
			opened = true
			return nil, errors.New("device must not be opened")
		},
		Locker: testDistributedLocker{},
		WALResourceForAG: func([16]byte, uint32) string { return "pool:uuid:ag:0:wal" },
		ReaderCoordinator: testReaderCoordinator{},
	})
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{
		VolumeID: "vol-1", Mode: metastore.VolumeRuntimeReadOnly,
		NodeID: "node-1", BootID: "boot-1", VolumeIndex: 0,
	}
	if _, err = backend.OpenReadWrite(context.Background(), spec, nil); err == nil {
		t.Fatal("RW backend accepted a RO activation request")
	}
	if opened { t.Fatal("backend opened the parent LUN for a mismatched activation mode") }
}
