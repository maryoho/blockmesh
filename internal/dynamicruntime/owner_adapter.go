package dynamicruntime

import (
	"context"
	"errors"

	"blockmesh/internal/metastore"
	"blockmesh/internal/volume"
)

// OwnerCoordinatorAdapter binds the P3 SAN Owner takeover state machine to
// the BMPOOL03 Runtime manager. The coordinator remains responsible for
// sanlock, durable OwnerRecord CAS, all-path SCSI PR fencing, and old-owner
// metadata I/O drain; this adapter does not synthesize any of those proofs.
type OwnerCoordinatorAdapter struct {
	coordinator *volume.OwnerCoordinator
}

func NewOwnerCoordinatorAdapter(coordinator *volume.OwnerCoordinator) (*OwnerCoordinatorAdapter, error) {
	if coordinator == nil { return nil, errors.New("P3 OwnerCoordinator is required") }
	return &OwnerCoordinatorAdapter{coordinator:coordinator},nil
}

func (a *OwnerCoordinatorAdapter) AcquireOwner(ctx context.Context, spec metastore.VolumeRuntimeSpec) (OwnerLease,error) {
	if a==nil || a.coordinator==nil || ctx==nil { return nil,errors.New("OwnerCoordinator adapter and context are required") }
	if spec.Mode!=metastore.VolumeRuntimeReadWrite || spec.VolumeID=="" || spec.NodeID=="" || spec.BootID=="" {
		return nil,errors.New("OwnerCoordinator adapter accepts only a fully identified RW Volume")
	}
	lease,err:=a.coordinator.AcquireOwner(ctx,spec.VolumeID,spec.NodeID,spec.BootID)
	if err!=nil{return nil,err}
	if lease==nil{return nil,errors.New("P3 OwnerCoordinator returned a nil lease")}
	return lease,nil
}

var _ OwnerLease = (*volume.OwnerLease)(nil)
