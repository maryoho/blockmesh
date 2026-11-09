// Package uapi implements the fixed-width little-endian BlockMesh ring ABI v1.
package uapi

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Version uint16 = 1
	MaxBatch = 256
	MinRingOrder = 4
	MaxRingOrder = 10
	RequestSize = 128
	CompletionSize = 64
	MappingSize = 24
	RingControlSize = 64
	FeatureRequestRing uint64 = 1 << 0
	FeatureCompletionKick uint64 = 1 << 1
)
type Operation uint16
const (
	OpLookupBatch Operation = 1
	OpAllocateZeroPublish Operation = 2
	OpDiscard Operation = 3
	OpFlush Operation = 4
)
// Linux _IOW('B', 0x05, uint32) for BM_IOC_KICK.
const IoctlKick uint32 = (1 << 30) | (4 << 16) | ('B' << 8) | 0x05

type ResultCode uint16
const (
	CodeOK ResultCode = iota
	CodeUnmapped
	CodeStaleEpoch
	CodeReadOnly
	CodeNoSpace
	CodeRetry
	CodeFenced
	CodeInvalid
	CodeInternal
)
type Request struct {
	Operation Operation
	Flags uint16
	RequestID, QueueGeneration, OwnerEpoch, ReaderEpoch, RootGeneration, LogicalStart uint64
	BlockCount uint32
	IdempotencyKey, VolumeID, NodeID, BootID [16]byte
}
type Mapping struct {
	LogicalBlock, PhysicalBlock uint64
	BlockCount uint32
}
type Completion struct {
	Code ResultCode
	Flags uint16
	RequestID, QueueGeneration, OwnerEpoch, RootGeneration uint64
	PayloadOffset, MappingCount, PayloadBytes uint32
}
func (r Request) Validate() error {
	if r.Flags != 0 { return errors.New("request flags are reserved and must be zero") }
	if r.RequestID == 0 || r.QueueGeneration == 0 { return errors.New("request ID and queue generation must be non-zero") }
	if r.VolumeID == ([16]byte{}) || r.NodeID == ([16]byte{}) || r.BootID == ([16]byte{}) { return errors.New("volume, node and boot UUIDs are required") }
	switch r.Operation {
	case OpLookupBatch:
		if r.BlockCount == 0 || r.BlockCount > MaxBatch { return fmt.Errorf("lookup count must be in [1,%d]", MaxBatch) }
		if r.OwnerEpoch == 0 && (r.ReaderEpoch == 0 || r.RootGeneration == 0) { return errors.New("lookup requires owner epoch or reader epoch plus root generation") }
	case OpAllocateZeroPublish, OpDiscard:
		if r.OwnerEpoch == 0 || r.BlockCount == 0 || r.BlockCount > MaxBatch { return errors.New("mutation requires owner epoch and block count in [1,256]") }
		if r.IdempotencyKey == ([16]byte{}) { return errors.New("mutation requires idempotency key") }
	case OpFlush:
		if r.OwnerEpoch == 0 || r.BlockCount != 0 { return errors.New("flush requires owner epoch and empty block range") }
	default: return errors.New("unknown operation")
	}
	if r.LogicalStart > ^uint64(0)-uint64(r.BlockCount) { return errors.New("logical range overflows") }
	return nil
}
func (r Request) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil { return nil, err }
	b := make([]byte, RequestSize)
	binary.LittleEndian.PutUint16(b[0:2], Version)
	binary.LittleEndian.PutUint16(b[2:4], RequestSize)
	binary.LittleEndian.PutUint16(b[4:6], uint16(r.Operation))
	binary.LittleEndian.PutUint16(b[6:8], r.Flags)
	binary.LittleEndian.PutUint64(b[8:16], r.RequestID)
	binary.LittleEndian.PutUint64(b[16:24], r.QueueGeneration)
	binary.LittleEndian.PutUint64(b[24:32], r.OwnerEpoch)
	binary.LittleEndian.PutUint64(b[32:40], r.ReaderEpoch)
	binary.LittleEndian.PutUint64(b[40:48], r.RootGeneration)
	binary.LittleEndian.PutUint64(b[48:56], r.LogicalStart)
	binary.LittleEndian.PutUint32(b[56:60], r.BlockCount)
	copy(b[64:80], r.IdempotencyKey[:]); copy(b[80:96], r.VolumeID[:])
	copy(b[96:112], r.NodeID[:]); copy(b[112:128], r.BootID[:])
	return b, nil
}
func UnmarshalRequest(b []byte) (Request, error) {
	var r Request
	if len(b) != RequestSize { return r, fmt.Errorf("request size %d, want %d", len(b), RequestSize) }
	if binary.LittleEndian.Uint16(b[:2]) != Version || binary.LittleEndian.Uint16(b[2:4]) != RequestSize { return r, errors.New("request ABI version or size mismatch") }
	if binary.LittleEndian.Uint32(b[60:64]) != 0 { return r, errors.New("request reserved field is non-zero") }
	r.Operation = Operation(binary.LittleEndian.Uint16(b[4:6])); r.Flags = binary.LittleEndian.Uint16(b[6:8])
	r.RequestID = binary.LittleEndian.Uint64(b[8:16]); r.QueueGeneration = binary.LittleEndian.Uint64(b[16:24])
	r.OwnerEpoch = binary.LittleEndian.Uint64(b[24:32]); r.ReaderEpoch = binary.LittleEndian.Uint64(b[32:40])
	r.RootGeneration = binary.LittleEndian.Uint64(b[40:48]); r.LogicalStart = binary.LittleEndian.Uint64(b[48:56])
	r.BlockCount = binary.LittleEndian.Uint32(b[56:60])
	copy(r.IdempotencyKey[:], b[64:80]); copy(r.VolumeID[:], b[80:96])
	copy(r.NodeID[:], b[96:112]); copy(r.BootID[:], b[112:128])
	return r, r.Validate()
}
func (c Completion) MarshalBinary() ([]byte, error) {
	if c.RequestID == 0 || c.QueueGeneration == 0 { return nil, errors.New("completion request ID and generation must be non-zero") }
	if c.Code > CodeInternal { return nil, errors.New("unknown completion code") }
	if c.Flags != 0 { return nil, errors.New("completion flags are reserved and must be zero") }
	if c.MappingCount == 0 && c.PayloadOffset != 0 { return nil, errors.New("completion without mapping must have zero payload offset") }
	if c.Code != CodeOK && (c.MappingCount != 0 || c.PayloadBytes != 0) { return nil, errors.New("error completion cannot carry mappings") }
	if c.MappingCount > MaxBatch || c.PayloadBytes != c.MappingCount*MappingSize { return nil, errors.New("invalid mapping payload length") }
	b := make([]byte, CompletionSize)
	binary.LittleEndian.PutUint16(b[:2], Version); binary.LittleEndian.PutUint16(b[2:4], CompletionSize)
	binary.LittleEndian.PutUint16(b[4:6], uint16(c.Code)); binary.LittleEndian.PutUint16(b[6:8], c.Flags)
	binary.LittleEndian.PutUint64(b[8:16], c.RequestID); binary.LittleEndian.PutUint64(b[16:24], c.QueueGeneration)
	binary.LittleEndian.PutUint64(b[24:32], c.OwnerEpoch); binary.LittleEndian.PutUint64(b[32:40], c.RootGeneration)
	binary.LittleEndian.PutUint32(b[40:44], c.PayloadOffset); binary.LittleEndian.PutUint32(b[44:48], c.MappingCount)
	binary.LittleEndian.PutUint32(b[48:52], c.PayloadBytes)
	return b, nil
}
func UnmarshalCompletion(b []byte) (Completion, error) {
	var c Completion
	if len(b) != CompletionSize { return c, fmt.Errorf("completion size %d, want %d", len(b), CompletionSize) }
	if binary.LittleEndian.Uint16(b[:2]) != Version || binary.LittleEndian.Uint16(b[2:4]) != CompletionSize { return c, errors.New("completion ABI version or size mismatch") }
	if binary.LittleEndian.Uint32(b[52:56]) != 0 || binary.LittleEndian.Uint32(b[56:60]) != 0 || binary.LittleEndian.Uint32(b[60:64]) != 0 { return c, errors.New("completion reserved field is non-zero") }
	c.Code = ResultCode(binary.LittleEndian.Uint16(b[4:6])); c.Flags = binary.LittleEndian.Uint16(b[6:8])
	c.RequestID = binary.LittleEndian.Uint64(b[8:16]); c.QueueGeneration = binary.LittleEndian.Uint64(b[16:24])
	c.OwnerEpoch = binary.LittleEndian.Uint64(b[24:32]); c.RootGeneration = binary.LittleEndian.Uint64(b[32:40])
	c.PayloadOffset = binary.LittleEndian.Uint32(b[40:44]); c.MappingCount = binary.LittleEndian.Uint32(b[44:48])
	c.PayloadBytes = binary.LittleEndian.Uint32(b[48:52])
	_, err := c.MarshalBinary(); return c, err
}
// MarshalMappings encodes an ordered vector of logically contiguous extents.
func MarshalMappings(mappings []Mapping) ([]byte, error) {
	if len(mappings) == 0 || len(mappings) > MaxBatch { return nil, fmt.Errorf("mapping vector count must be in [1,%d]", MaxBatch) }
	b := make([]byte, len(mappings)*MappingSize)
	var previousEnd uint64
	for i, m := range mappings {
		encoded, err := m.MarshalBinary()
		if err != nil { return nil, fmt.Errorf("mapping[%d]: %w", i, err) }
		if i > 0 && m.LogicalBlock != previousEnd { return nil, fmt.Errorf("mapping[%d] is not logically contiguous", i) }
		previousEnd = m.LogicalBlock + uint64(m.BlockCount)
		copy(b[i*MappingSize:(i+1)*MappingSize], encoded)
	}
	return b, nil
}

// UnmarshalMappings decodes a logically contiguous Mapping Vector.
func UnmarshalMappings(b []byte, count uint32) ([]Mapping, error) {
	if count == 0 || count > MaxBatch || uint64(len(b)) != uint64(count)*MappingSize { return nil, errors.New("invalid mapping vector length") }
	out := make([]Mapping, count)
	var previousEnd uint64
	for i := range out {
		m, err := UnmarshalMapping(b[i*MappingSize:(i+1)*MappingSize])
		if err != nil { return nil, fmt.Errorf("mapping[%d]: %w", i, err) }
		if i > 0 && m.LogicalBlock != previousEnd { return nil, fmt.Errorf("mapping[%d] is not logically contiguous", i) }
		previousEnd = m.LogicalBlock + uint64(m.BlockCount)
		out[i] = m
	}
	return out, nil
}

func (m Mapping) MarshalBinary() ([]byte, error) {
	if m.BlockCount == 0 || m.LogicalBlock > ^uint64(0)-uint64(m.BlockCount) || m.PhysicalBlock > ^uint64(0)-uint64(m.BlockCount) { return nil, errors.New("invalid mapping range") }
	b := make([]byte, MappingSize)
	binary.LittleEndian.PutUint64(b[:8], m.LogicalBlock); binary.LittleEndian.PutUint64(b[8:16], m.PhysicalBlock)
	binary.LittleEndian.PutUint32(b[16:20], m.BlockCount)
	return b, nil
}
func UnmarshalMapping(b []byte) (Mapping, error) {
	var m Mapping
	if len(b) != MappingSize { return m, fmt.Errorf("mapping size %d, want %d", len(b), MappingSize) }
	if binary.LittleEndian.Uint32(b[20:24]) != 0 { return m, errors.New("mapping reserved field is non-zero") }
	m.LogicalBlock = binary.LittleEndian.Uint64(b[:8]); m.PhysicalBlock = binary.LittleEndian.Uint64(b[8:16])
	m.BlockCount = binary.LittleEndian.Uint32(b[16:20])
	_, err := m.MarshalBinary(); return m, err
}
