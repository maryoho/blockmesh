package metastore

import (
	"errors"
	"testing"
)

func TestOwnerRuntimeStateRejectsSkippedSafetyGates(t *testing.T) {
	var tracker ownerStateTracker
	for _, next := range []OwnerRuntimeState{
		OwnerStateWALRecovered, OwnerStateMetadataReady, OwnerStateOwnerEpochPublished,
		OwnerStateRecoveryReady, OwnerStateRequestsAdmitted,
	} {
		if err:=tracker.transition(next); !errors.Is(err,ErrOwnerStateTransition) {
			t.Fatalf("transition created -> %s = %v, want invalid transition",next,err)
		}
	}
	if got:=tracker.current();got!=OwnerStateCreated { t.Fatalf("invalid transition mutated state: %s",got) }
}

func TestOwnerRuntimeStateRWSequenceAndTerminalLeaseLoss(t *testing.T) {
	var tracker ownerStateTracker
	sequence:=[]OwnerRuntimeState{
		OwnerStateDeviceReady,OwnerStateOwnerLeaseReady,OwnerStateWALRecovered,
		OwnerStateMetadataReady,OwnerStateOwnerEpochPublished,
		OwnerStateRecoveryReady,OwnerStateRequestsAdmitted,
	}
	for _,next:=range sequence {
		if err:=tracker.transition(next);err!=nil { t.Fatalf("transition to %s: %v",next,err) }
	}
	if err:=tracker.transition(OwnerStateLeaseLost);err!=nil { t.Fatal(err) }
	if err:=tracker.transition(OwnerStateRequestsAdmitted);!errors.Is(err,ErrOwnerStateTransition) {
		t.Fatalf("lease-lost runtime re-entered admission: %v",err)
	}
	if err:=tracker.transition(OwnerStateStopping);err!=nil { t.Fatal(err) }
	if err:=tracker.transition(OwnerStateStopped);err!=nil { t.Fatal(err) }
	if err:=tracker.transition(OwnerStateOwnerLeaseReady);!errors.Is(err,ErrOwnerStateTransition) {
		t.Fatalf("stopped runtime restarted: %v",err)
	}
}

func TestOwnerRuntimeStateROSequence(t *testing.T) {
	var tracker ownerStateTracker
	for _,next:=range []OwnerRuntimeState{
		OwnerStateDeviceReady,OwnerStateMetadataReady,OwnerStateReaderJoined,
		OwnerStateRecoveryReady,OwnerStateRequestsAdmitted,
	} {
		if err:=tracker.transition(next);err!=nil { t.Fatalf("RO transition to %s: %v",next,err) }
	}
}
