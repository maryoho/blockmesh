package metaservice

import (
	"testing"

	"blockmesh/internal/uapi"
)

func TestAllocationFingerprintStableAcrossOwnerTakeoverAndTransportRetry(t *testing.T) {
	var volumeID, nodeID, bootID [16]byte
	volumeID[0], nodeID[0], bootID[0] = 1, 2, 3
	var key [16]byte
	key[0] = 9
	req := uapi.Request{
		Operation: uapi.OpAllocateZeroPublish,
		RequestID: 10, QueueGeneration: 20, OwnerEpoch: 30,
		LogicalStart: 100, BlockCount: 4,
		IdempotencyKey: key, VolumeID: volumeID, NodeID: nodeID, BootID: bootID,
	}
	first := allocationFingerprint(req)
	req.RequestID, req.QueueGeneration, req.OwnerEpoch = 11, 21, 31
	req.NodeID[0], req.BootID[0] = 8, 7
	if got := allocationFingerprint(req); got != first {
		t.Fatal("retry after Owner takeover changed the idempotency fingerprint")
	}
}

func TestAllocationFingerprintChangesForDifferentMutation(t *testing.T) {
	var volumeID [16]byte
	volumeID[0] = 1
	req := uapi.Request{Operation: uapi.OpAllocateZeroPublish, LogicalStart: 100, BlockCount: 4, VolumeID: volumeID}
	first := allocationFingerprint(req)
	req.LogicalStart++
	if got := allocationFingerprint(req); got == first {
		t.Fatal("different logical range reused the same fingerprint")
	}
}
