package metastore

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// OwnerRuntimeState is the monotonic activation state of one Volume runtime.
// The Owner lease is considered acquired only after the OwnerCoordinator has
// published OwnerRecord, fenced/drained the previous Owner, and verified the
// new SCSI PR reservation on every component path.
type OwnerRuntimeState uint32

const (
	OwnerStateCreated OwnerRuntimeState = iota
	OwnerStateDeviceReady
	OwnerStateOwnerLeaseReady
	OwnerStateWALRecovered
	OwnerStateMetadataReady
	OwnerStateOwnerEpochPublished
	OwnerStateReaderJoined
	OwnerStateRecoveryReady
	OwnerStateRequestsAdmitted
	OwnerStateLeaseLost
	OwnerStateStopping
	OwnerStateStopped
	OwnerStateFailed
)

func (s OwnerRuntimeState) String() string {
	switch s {
	case OwnerStateCreated: return "created"
	case OwnerStateDeviceReady: return "device-ready"
	case OwnerStateOwnerLeaseReady: return "owner-lease-ready"
	case OwnerStateWALRecovered: return "wal-recovered"
	case OwnerStateMetadataReady: return "metadata-ready"
	case OwnerStateOwnerEpochPublished: return "owner-epoch-published"
	case OwnerStateReaderJoined: return "reader-joined"
	case OwnerStateRecoveryReady: return "recovery-ready"
	case OwnerStateRequestsAdmitted: return "requests-admitted"
	case OwnerStateLeaseLost: return "owner-lease-lost"
	case OwnerStateStopping: return "stopping"
	case OwnerStateStopped: return "stopped"
	case OwnerStateFailed: return "failed"
	default: return fmt.Sprintf("unknown(%d)",s)
	}
}

var ErrOwnerStateTransition = errors.New("invalid Owner runtime state transition")

func validOwnerStateTransition(from,to OwnerRuntimeState) bool {
	if to==OwnerStateLeaseLost {
		return from>=OwnerStateOwnerLeaseReady && from<OwnerStateStopping
	}
	if to==OwnerStateStopping {
		return from!=OwnerStateStopped && from!=OwnerStateStopping
	}
	if to==OwnerStateFailed {
		return from!=OwnerStateStopped && from!=OwnerStateStopping
	}
	if to==OwnerStateStopped { return from==OwnerStateStopping }
	switch from {
	case OwnerStateCreated: return to==OwnerStateDeviceReady
	case OwnerStateDeviceReady: return to==OwnerStateOwnerLeaseReady || to==OwnerStateMetadataReady
	case OwnerStateOwnerLeaseReady: return to==OwnerStateWALRecovered
	case OwnerStateWALRecovered: return to==OwnerStateMetadataReady
	case OwnerStateMetadataReady: return to==OwnerStateOwnerEpochPublished || to==OwnerStateReaderJoined
	case OwnerStateOwnerEpochPublished,OwnerStateReaderJoined: return to==OwnerStateRecoveryReady
	case OwnerStateRecoveryReady: return to==OwnerStateRequestsAdmitted
	default: return false
	}
}

type ownerStateTracker struct { state atomic.Uint32 }

func (t *ownerStateTracker) current() OwnerRuntimeState {
	if t==nil { return OwnerStateFailed }
	return OwnerRuntimeState(t.state.Load())
}

func (t *ownerStateTracker) transition(next OwnerRuntimeState) error {
	if t==nil { return errors.New("nil Owner state tracker") }
	for {
		current:=OwnerRuntimeState(t.state.Load())
		if !validOwnerStateTransition(current,next) {
			return fmt.Errorf("%w: %s -> %s",ErrOwnerStateTransition,current,next)
		}
		if t.state.CompareAndSwap(uint32(current),uint32(next)) { return nil }
	}
}
