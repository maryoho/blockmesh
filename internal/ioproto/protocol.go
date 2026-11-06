// Package ioproto defines the versioned, transport-independent contract
// between the BlockMesh DM data path and the metadata service. These Go
// structures are not a stable kernel UAPI; the fixed-width wire ABI is defined
// separately in docs/p4-1.1-thin-provisioning-architecture.md.
package ioproto

import (
	"errors"
	"fmt"
)

const Version uint16 = 1
const MaxBatchBlocks uint32 = 256

type Operation uint16

const (
	OpLookupBatch Operation = iota + 1
	OpAllocateZeroPublish
	OpDiscard
	OpFlush
)

func (o Operation) Valid() bool {
	return o >= OpLookupBatch && o <= OpFlush
}

type Identity struct {
	VolumeID string
	NodeID   string
	BootID   string
	// OwnerEpoch is mandatory for mutating operations. RO lookups carry the
	// ReaderEpoch and RootGeneration instead.
	OwnerEpoch    uint64
	ReaderEpoch   uint64
	RootGeneration uint64
}

type Request struct {
	Version   uint16
	RequestID uint64
	Operation Operation
	Identity  Identity
	// LogicalStart and BlockCount are expressed in 4MiB logical blocks.
	LogicalStart uint64
	BlockCount   uint32
	// IdempotencyKey is stable across retries of a mutating request.
	IdempotencyKey [16]byte
}

type Mapping struct {
	LogicalBlock  uint64
	PhysicalBlock uint64
	BlockCount    uint32
}

type ResultCode uint16

const (
	ResultOK ResultCode = iota
	ResultUnmapped
	ResultStaleEpoch
	ResultReadOnly
	ResultNoSpace
	ResultRetry
	ResultFenced
	ResultInvalid
	ResultInternal
)

type Response struct {
	Version        uint16
	RequestID      uint64
	Code            ResultCode
	OwnerEpoch      uint64
	RootGeneration uint64
	Mappings        []Mapping
}

func (r Request) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("unsupported protocol version %d", r.Version)
	}
	if r.RequestID == 0 {
		return errors.New("request ID must be non-zero")
	}
	if !r.Operation.Valid() {
		return errors.New("invalid operation")
	}
	if r.Identity.VolumeID == "" || r.Identity.NodeID == "" || r.Identity.BootID == "" {
		return errors.New("VolumeID, NodeID and BootID are required")
	}
	switch r.Operation {
	case OpLookupBatch:
		if r.BlockCount == 0 || r.BlockCount > MaxBatchBlocks {
			return fmt.Errorf("lookup block count must be in [1,%d]", MaxBatchBlocks)
		}
		if r.Identity.OwnerEpoch == 0 && (r.Identity.ReaderEpoch == 0 || r.Identity.RootGeneration == 0) {
			return errors.New("lookup requires OwnerEpoch or ReaderEpoch plus RootGeneration")
		}
	case OpAllocateZeroPublish, OpDiscard:
		if r.Identity.OwnerEpoch == 0 {
			return errors.New("mutating operation requires non-zero OwnerEpoch")
		}
		if r.BlockCount == 0 || r.BlockCount > MaxBatchBlocks {
			return fmt.Errorf("mutating block count must be in [1,%d]", MaxBatchBlocks)
		}
		if r.IdempotencyKey == ([16]byte{}) {
			return errors.New("mutating operation requires an idempotency key")
		}
	case OpFlush:
		if r.Identity.OwnerEpoch == 0 {
			return errors.New("flush requires non-zero OwnerEpoch")
		}
		if r.BlockCount != 0 {
			return errors.New("flush must not specify a block range")
		}
	}
	if r.LogicalStart > ^uint64(0)-uint64(r.BlockCount) {
		return errors.New("logical block range overflows")
	}
	return nil
}

func (r Response) Validate(request Request) error {
	if r.Version != Version || r.RequestID == 0 || r.RequestID != request.RequestID {
		return errors.New("response version or request ID mismatch")
	}
	if r.Code > ResultInternal {
		return errors.New("unknown response code")
	}
	if r.Code != ResultOK && len(r.Mappings) != 0 {
		return errors.New("non-OK response must not carry mappings")
	}
	if r.Code == ResultOK && request.Operation == OpLookupBatch {
		if r.RootGeneration == 0 {
			return errors.New("successful lookup must identify a root generation")
		}
		for _, m := range r.Mappings {
			if m.BlockCount == 0 || m.LogicalBlock < request.LogicalStart ||
				m.LogicalBlock > ^uint64(0)-uint64(m.BlockCount) ||
				m.LogicalBlock+uint64(m.BlockCount) > request.LogicalStart+uint64(request.BlockCount) {
				return errors.New("mapping response is outside requested logical range")
			}
		}
	}
	if request.Identity.OwnerEpoch != 0 && r.OwnerEpoch != request.Identity.OwnerEpoch &&
		(r.Code == ResultOK || r.Code == ResultStaleEpoch) {
		return errors.New("response OwnerEpoch mismatch")
	}
	return nil
}
