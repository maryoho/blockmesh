package dynamicruntime

import (
	"errors"
	"time"

	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
	"blockmesh/internal/volume"
)

// NewIntegratedDynamicVolumeRuntimeManager wires the production BMPOOL03
// Backend, P3 OwnerCoordinator, distributed Reader-set coordinator and DM
// RequestGate together. It verifies that Owner and Reader leases use the same
// SanlockLocker instance; test/development FileLocker configurations are
// intentionally rejected.
func NewIntegratedDynamicVolumeRuntimeManager(backend *Backend, ownerCoordinator *volume.OwnerCoordinator,
	gate RequestGate, readerHeartbeatInterval time.Duration) (*DynamicVolumeRuntimeManager,error) {
	if backend==nil || ownerCoordinator==nil || gate==nil {
		return nil,errors.New("BMPOOL03 Backend, P3 OwnerCoordinator and DM RequestGate are required")
	}
	sanlock,ok:=backend.cfg.Locker.(*lock.SanlockLocker)
	if !ok || sanlock==nil {
		return nil,errors.New("production BMPOOL03 Runtime requires SanlockLocker, not a local development locker")
	}
	if !ownerCoordinator.UsesSanlockLocker(sanlock) {
		return nil,errors.New("P3 OwnerCoordinator and BMPOOL03 Backend must share the same SanlockLocker instance")
	}
	if backend.cfg.ReaderLease.Locker!=nil && backend.cfg.ReaderLease.Locker!=sanlock {
		return nil,errors.New("BMPOOL03 ReaderLeaseConfig must use the same SanlockLocker instance")
	}
	readerCoordinator,ok:=backend.cfg.ReaderCoordinator.(*metastore.DistributedReaderCoordinator)
	if !ok || readerCoordinator.Locker!=backend.cfg.Locker {
		return nil,errors.New("production BMPOOL03 Runtime requires DistributedReaderCoordinator using the Backend distributed locker")
	}
	ownerAdapter,err:=NewOwnerCoordinatorAdapter(ownerCoordinator)
	if err!=nil{return nil,err}
	return NewDynamicVolumeRuntimeManager(ManagerConfig{
		AcquireOwner:ownerAdapter.AcquireOwner,
		OpenReadWrite:backend,
		OpenReadOnly:backend.ReadOnlyOpener(gate),
		Gate:gate,
		ReaderHeartbeatInterval:readerHeartbeatInterval,
	})
}
