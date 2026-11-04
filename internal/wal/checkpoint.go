package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"blockmesh/internal/format"
)

var (
	ErrCheckpointCorrupt = errors.New("WAL checkpoint descriptors are corrupt")
	ErrCheckpointMissing = errors.New("WAL checkpoint descriptor is missing; legacy WAL migration is required")
)

const (
	checkpointMagic uint64 = 0x424D57414C435031 // BMWALCP1
	checkpointVersion uint32 = 1
	checkpointDescriptorSize = format.PageSize
	checkpointSlotsPerAG = uint64(2)
	walBankCount = uint64(2)
	walBankSize = format.WALSegmentSize / walBankCount
)

type checkpointDescriptor struct {
	Generation uint64
	CheckpointTxID uint64
	ActiveBank uint32
	AG uint32
}

func checkpointOffset(sb format.Superblock, ag uint32, slot uint32) (uint64, error) {
	if slot >= 2 || ag >= sb.AGCount {
		return 0, errors.New("checkpoint descriptor index out of range")
	}
	rel := (uint64(ag)*checkpointSlotsPerAG + uint64(slot))*checkpointDescriptorSize
	if rel > sb.CheckpointSize || checkpointDescriptorSize > sb.CheckpointSize-rel {
		return 0, errors.New("checkpoint descriptor area exceeds checkpoint region")
	}
	if sb.CheckpointOffset > ^uint64(0)-rel {
		return 0, errors.New("checkpoint descriptor offset overflow")
	}
	return sb.CheckpointOffset+rel, nil
}

func encodeCheckpoint(d checkpointDescriptor) []byte {
	b := make([]byte, checkpointDescriptorSize)
	binary.LittleEndian.PutUint64(b[0:8], checkpointMagic)
	binary.LittleEndian.PutUint32(b[8:12], checkpointVersion)
	binary.LittleEndian.PutUint32(b[12:16], d.AG)
	binary.LittleEndian.PutUint64(b[16:24], d.Generation)
	binary.LittleEndian.PutUint64(b[24:32], d.CheckpointTxID)
	binary.LittleEndian.PutUint32(b[32:36], d.ActiveBank)
	binary.LittleEndian.PutUint32(b[checkpointDescriptorSize-4:], crc32.Checksum(b[:checkpointDescriptorSize-4], journalCRC))
	return b
}

func decodeCheckpoint(b []byte, ag uint32) (checkpointDescriptor, error) {
	var d checkpointDescriptor
	if len(b) != int(checkpointDescriptorSize) ||
		binary.LittleEndian.Uint64(b[0:8]) != checkpointMagic ||
		binary.LittleEndian.Uint32(b[8:12]) != checkpointVersion ||
		crc32.Checksum(b[:checkpointDescriptorSize-4], journalCRC) != binary.LittleEndian.Uint32(b[checkpointDescriptorSize-4:]) {
		return d, ErrCheckpointCorrupt
	}
	d.AG = binary.LittleEndian.Uint32(b[12:16])
	d.Generation = binary.LittleEndian.Uint64(b[16:24])
	d.CheckpointTxID = binary.LittleEndian.Uint64(b[24:32])
	d.ActiveBank = binary.LittleEndian.Uint32(b[32:36])
	if d.AG != ag || d.Generation == 0 || d.ActiveBank >= uint32(walBankCount) {
		return checkpointDescriptor{}, ErrCheckpointCorrupt
	}
	return d, nil
}

func (j *Journal) loadCheckpoint(ag uint32, sb format.Superblock) (checkpointDescriptor, uint32, error) {
	type candidate struct { d checkpointDescriptor; slot uint32 }
	var valid []candidate
	nonzero := false
	for slot := uint32(0); slot < 2; slot++ {
		off, err := checkpointOffset(sb, ag, slot)
		if err != nil { return checkpointDescriptor{}, 0, err }
		b := make([]byte, checkpointDescriptorSize)
		if err = readAtFull(j.dev, b, int64(off)); err != nil { return checkpointDescriptor{}, 0, err }
		if !allZero(b) { nonzero = true }
		d, decErr := decodeCheckpoint(b, ag)
		if decErr == nil { valid = append(valid, candidate{d:d, slot:slot}) }
	}
	if len(valid) == 0 {
		if nonzero { return checkpointDescriptor{}, 0, ErrCheckpointCorrupt }
		// Do not silently reinterpret an existing v1 WAL as an empty bank.
		probe := make([]byte, journalHeaderSize)
		if err := readAtFull(j.dev, probe, int64(j.segmentBase)); err != nil { return checkpointDescriptor{}, 0, err }
		if !allZero(probe) { return checkpointDescriptor{}, 0, ErrCheckpointMissing }
		d := checkpointDescriptor{Generation:1, AG:ag, ActiveBank:0}
		off, err := checkpointOffset(sb, ag, 0)
		if err != nil { return checkpointDescriptor{}, 0, err }
		if err = writeAtFull(j.dev, encodeCheckpoint(d), int64(off)); err != nil { return checkpointDescriptor{}, 0, err }
		if err = j.dev.Sync(); err != nil { return checkpointDescriptor{}, 0, err }
		return d, 0, nil
	}
	best := valid[0]
	for _, c := range valid[1:] {
		if c.d.Generation > best.d.Generation { best = c }
	}
	if len(valid) == 2 && valid[0].d.Generation == valid[1].d.Generation && valid[0].d != valid[1].d {
		return checkpointDescriptor{}, 0, fmt.Errorf("%w: conflicting descriptors at generation %d", ErrCheckpointCorrupt, best.d.Generation)
	}
	return best.d, best.slot, nil
}
