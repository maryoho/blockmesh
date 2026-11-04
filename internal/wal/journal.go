package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

var (
	ErrJournalFull = errors.New("AG WAL segment is full")
	ErrJournalCorrupt = errors.New("AG WAL segment is corrupt")
)

const (
	journalMagic uint32 = 0x424d4a52 // BMJR
	journalVersion uint16 = 1
	journalHeaderSize = 32
	journalIntent uint16 = 1
	journalCommit uint16 = 2
	journalMaxRecord = 16 << 20
)

var journalCRC = crc32.MakeTable(crc32.Castagnoli)

type PageUpdate struct {
	Offset uint64
	Image []byte
}

type journalTxn struct {
	txID uint64
	updates []PageUpdate
}

// Journal is an append-only redo journal in one AG's fixed metadata-device
// WAL segment. A transaction is durable only after its Commit record is synced.
type Journal struct {
	mu sync.Mutex
	dev device.Device
	base uint64
	size uint64
	segmentBase uint64
	metadataSize uint64
	walStart uint64
	walEnd uint64
	checkpointStart uint64
	checkpointEnd uint64
	checkpointSlot uint32
	checkpointGeneration uint64
	checkpointTxID uint64
	activeBank uint32
	end uint64
	nextTx uint64
	lastTx uint64
	poisoned bool
}

func New(dev device.Device, sb format.Superblock, ag uint32) (*Journal, error) {
	if dev == nil {
		return nil, errors.New("nil metadata device")
	}
	if ag >= sb.AGCount || sb.AGCount == 0 {
		return nil, fmt.Errorf("AG %d out of range", ag)
	}
	if sb.WALSize < uint64(sb.AGCount)*format.WALSegmentSize {
		return nil, errors.New("WAL region is smaller than configured AG segments")
	}
	if sb.MetadataSize == 0 || dev.Size() < 0 || uint64(dev.Size()) < sb.MetadataSize {
		return nil, errors.New("metadata device is smaller than the superblock geometry")
	}
	if sb.WALOffset > math.MaxUint64-sb.WALSize {
		return nil, errors.New("WAL region overflows address space")
	}
	segmentBase := sb.WALOffset + uint64(ag)*format.WALSegmentSize
	if segmentBase < sb.WALOffset || segmentBase > sb.MetadataSize || format.WALSegmentSize > sb.MetadataSize-segmentBase ||
		uint64(ag)*format.WALSegmentSize > sb.WALSize || format.WALSegmentSize > sb.WALSize-uint64(ag)*format.WALSegmentSize {
		return nil, errors.New("AG WAL segment outside metadata device")
	}
	checkpointEnd := sb.CheckpointOffset + sb.CheckpointSize
	if checkpointEnd < sb.CheckpointOffset || checkpointEnd > sb.MetadataSize {
		return nil, errors.New("checkpoint region outside metadata device")
	}
	if uint64(sb.AGCount)*checkpointSlotsPerAG*checkpointDescriptorSize > sb.CheckpointSize {
		return nil, errors.New("checkpoint region cannot hold dual per-AG descriptors")
	}
	j := &Journal{
		dev: dev, segmentBase: segmentBase, size: walBankSize,
		metadataSize: sb.MetadataSize, walStart: sb.WALOffset,
		walEnd: sb.WALOffset + sb.WALSize,
		checkpointStart: sb.CheckpointOffset, checkpointEnd: checkpointEnd,
		nextTx: 1,
	}
	desc, slot, err := j.loadCheckpoint(ag, sb)
	if err != nil { return nil, fmt.Errorf("load AG %d checkpoint: %w", ag, err) }
	j.activeBank, j.checkpointSlot = desc.ActiveBank, slot
	j.checkpointGeneration, j.checkpointTxID = desc.Generation, desc.CheckpointTxID
	j.lastTx = desc.CheckpointTxID
	j.nextTx = desc.CheckpointTxID + 1
	if j.nextTx == 0 { return nil, errors.New("WAL transaction ID exhausted") }
	j.base = segmentBase + uint64(desc.ActiveBank)*walBankSize
	// Recovery is mandatory before a caller can issue new transactions. This
	// prevents reads or later writes from observing pages left half-applied by
	// a crash after the durable Commit record.
	if _, err := j.Recover(); err != nil {
		return nil, fmt.Errorf("recover AG %d WAL: %w", ag, err)
	}
	return j, nil
}

// Commit writes complete page after-images to the WAL, syncs the intent,
// syncs the commit marker, then applies the after-images and syncs the device.
func (j *Journal) Commit(updates []PageUpdate) error {
	if len(updates) == 0 {
		return errors.New("empty WAL transaction")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.poisoned {
		return errors.New("WAL journal requires recovery after previous I/O error")
	}
	if j.nextTx == 0 || j.nextTx == math.MaxUint64 {
		return errors.New("WAL transaction ID exhausted")
	}
	if err := j.validateUpdates(updates); err != nil {
		return err
	}
	payload, err := encodeUpdates(updates)
	if err != nil {
		return err
	}
	txID := j.nextTx
	j.nextTx++

	intent, err := encodeJournalRecord(journalIntent, txID, payload)
	if err != nil {
		return err
	}
	if err = j.appendLocked(intent); err != nil {
		j.poisoned = true
		return err
	}
	if err = j.dev.Sync(); err != nil {
		j.poisoned = true
		return err
	}

	commitPayload := make([]byte, 4)
	binary.LittleEndian.PutUint32(commitPayload, crc32.Checksum(payload, journalCRC))
	commit, err := encodeJournalRecord(journalCommit, txID, commitPayload)
	if err != nil {
		j.poisoned = true
		return err
	}
	if err = j.appendLocked(commit); err != nil {
		j.poisoned = true
		return err
	}
	if err = j.dev.Sync(); err != nil {
		j.poisoned = true
		return err
	}

	for _, update := range updates {
		if err = writeAtFull(j.dev, update.Image, int64(update.Offset)); err != nil {
			j.poisoned = true
			return fmt.Errorf("apply WAL transaction %d at offset %d: %w", txID, update.Offset, err)
		}
	}
	if err = j.dev.Sync(); err != nil {
		j.poisoned = true
		return err
	}
	j.lastTx = txID
	return nil
}

// Recover replays all committed transactions in this AG segment. Replaying
// after-images is idempotent, so recovery may safely repeat after a crash.
func (j *Journal) Recover() (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	txns, end, next, err := j.scanLocked()
	if err != nil {
		return false, err
	}
	for _, txn := range txns {
		for _, update := range txn.updates {
			if err = writeAtFull(j.dev, update.Image, int64(update.Offset)); err != nil {
				return false, fmt.Errorf("replay WAL transaction %d at offset %d: %w", txn.txID, update.Offset, err)
			}
		}
	}
	if len(txns) != 0 {
		if err = j.dev.Sync(); err != nil {
			return false, err
		}
	}
	j.end, j.nextTx, j.poisoned = end, next, false
	for _, txn := range txns {
		if txn.txID > j.lastTx { j.lastTx = txn.txID }
	}
	return len(txns) != 0, nil
}

func (j *Journal) validateUpdates(updates []PageUpdate) error {
	seen := make(map[uint64]struct{}, len(updates))
	for _, update := range updates {
		if update.Offset%format.PageSize != 0 || uint64(len(update.Image)) != format.PageSize {
			return fmt.Errorf("WAL page update must be 4KiB aligned and exactly 4KiB")
		}
		if update.Offset < 2*format.PageSize || update.Offset > j.metadataSize || format.PageSize > j.metadataSize-update.Offset {
			return fmt.Errorf("WAL page update offset %d outside metadata regions", update.Offset)
		}
		end := update.Offset + format.PageSize
		if update.Offset < j.walEnd && end > j.walStart {
			return fmt.Errorf("WAL page update overlaps WAL region")
		}
		if update.Offset < j.checkpointEnd && end > j.checkpointStart {
			return fmt.Errorf("WAL page update overlaps checkpoint descriptor region")
		}
		if _, ok := seen[update.Offset]; ok {
			return fmt.Errorf("duplicate WAL page update offset %d", update.Offset)
		}
		seen[update.Offset] = struct{}{}
	}
	return nil
}

func (j *Journal) appendLocked(record []byte) error {
	if j.end > j.size || uint64(len(record)) > j.size-j.end {
		return ErrJournalFull
	}
	if err := writeAtFull(j.dev, record, int64(j.base+j.end)); err != nil {
		return err
	}
	j.end += uint64(len(record))
	return nil
}

func (j *Journal) scanLocked() ([]journalTxn, uint64, uint64, error) {
	var committed []journalTxn
	intents := make(map[uint64][]PageUpdate)
	intentCRC := make(map[uint64]uint32)
	var offset uint64
	nextTx := j.checkpointTxID + 1
	if nextTx == 0 { return nil, 0, 0, errors.New("WAL transaction ID exhausted") }
	for offset < j.size {
		header := make([]byte, journalHeaderSize)
		if j.size-offset < journalHeaderSize {
			break
		}
		n, err := j.dev.ReadAt(header, int64(j.base+offset))
		if err != nil && err != io.EOF {
			return nil, 0, 0, err
		}
		if n == 0 && err == io.EOF {
			break
		}
		if n < len(header) {
			// A torn final header is not committed; the next append overwrites it.
			break
		}
		if allZero(header) {
			break
		}
		if binary.LittleEndian.Uint32(header[0:4]) != journalMagic ||
			binary.LittleEndian.Uint16(header[4:6]) != journalVersion {
			return nil, 0, 0, ErrJournalCorrupt
		}
		storedHeaderCRC := binary.LittleEndian.Uint32(header[24:28])
		if crc32.Checksum(header[:24], journalCRC) != storedHeaderCRC {
			return nil, 0, 0, ErrJournalCorrupt
		}
		kind := binary.LittleEndian.Uint16(header[6:8])
		payloadLen := uint64(binary.LittleEndian.Uint32(header[8:12]))
		txID := binary.LittleEndian.Uint64(header[12:20])
		if txID == 0 || payloadLen > journalMaxRecord || payloadLen > j.size-offset-journalHeaderSize {
			return nil, 0, 0, ErrJournalCorrupt
		}
		payload := make([]byte, int(payloadLen))
		if payloadLen != 0 {
			n, err = j.dev.ReadAt(payload, int64(j.base+offset+journalHeaderSize))
			if err != nil && err != io.EOF {
				return nil, 0, 0, err
			}
			if n < len(payload) {
				break
			}
		}
		if crc32.Checksum(payload, journalCRC) != binary.LittleEndian.Uint32(header[20:24]) {
			return nil, 0, 0, ErrJournalCorrupt
		}
		switch kind {
		case journalIntent:
			if _, exists := intents[txID]; exists {
				return nil, 0, 0, ErrJournalCorrupt
			}
			updates, decodeErr := decodeUpdates(payload)
			if decodeErr != nil {
				return nil, 0, 0, decodeErr
			}
			if decodeErr = j.validateUpdates(updates); decodeErr != nil {
				return nil, 0, 0, ErrJournalCorrupt
			}
			intents[txID] = updates
			intentCRC[txID] = crc32.Checksum(payload, journalCRC)
		case journalCommit:
			if len(payload) != 4 {
				return nil, 0, 0, ErrJournalCorrupt
			}
			updates, exists := intents[txID]
			if !exists || binary.LittleEndian.Uint32(payload) != intentCRC[txID] {
				return nil, 0, 0, ErrJournalCorrupt
			}
			committed = append(committed, journalTxn{txID: txID, updates: updates})
			delete(intents, txID)
			delete(intentCRC, txID)
		default:
			return nil, 0, 0, ErrJournalCorrupt
		}
		if txID >= nextTx {
			if txID == math.MaxUint64 {
				nextTx = 0
			} else {
				nextTx = txID + 1
			}
		}
		offset += journalHeaderSize + payloadLen
	}
	return committed, offset, nextTx, nil
}

func encodeJournalRecord(kind uint16, txID uint64, payload []byte) ([]byte, error) {
	if len(payload) > journalMaxRecord {
		return nil, errors.New("WAL record too large")
	}
	b := make([]byte, journalHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], journalMagic)
	binary.LittleEndian.PutUint16(b[4:6], journalVersion)
	binary.LittleEndian.PutUint16(b[6:8], kind)
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint64(b[12:20], txID)
	binary.LittleEndian.PutUint32(b[20:24], crc32.Checksum(payload, journalCRC))
	binary.LittleEndian.PutUint32(b[24:28], crc32.Checksum(b[:24], journalCRC))
	copy(b[journalHeaderSize:], payload)
	return b, nil
}

func encodeUpdates(updates []PageUpdate) ([]byte, error) {
	if len(updates) > math.MaxUint32 {
		return nil, errors.New("too many WAL page updates")
	}
	size := uint64(4) + uint64(len(updates))*(8+4+format.PageSize)
	if size > journalMaxRecord {
		return nil, errors.New("WAL transaction too large")
	}
	b := make([]byte, int(size))
	binary.LittleEndian.PutUint32(b[:4], uint32(len(updates)))
	off := 4
	for _, update := range updates {
		binary.LittleEndian.PutUint64(b[off:off+8], update.Offset)
		off += 8
		binary.LittleEndian.PutUint32(b[off:off+4], uint32(len(update.Image)))
		off += 4
		copy(b[off:off+int(format.PageSize)], update.Image)
		off += int(format.PageSize)
	}
	return b, nil
}

func decodeUpdates(payload []byte) ([]PageUpdate, error) {
	if len(payload) < 4 {
		return nil, ErrJournalCorrupt
	}
	count := uint64(binary.LittleEndian.Uint32(payload[:4]))
	const itemSize = uint64(8+4) + format.PageSize
	if count == 0 || count > uint64(len(payload)-4)/itemSize || uint64(len(payload)) != 4+count*itemSize {
		return nil, ErrJournalCorrupt
	}
	updates := make([]PageUpdate, 0, int(count))
	off := 4
	for i := uint64(0); i < count; i++ {
		pageOffset := binary.LittleEndian.Uint64(payload[off : off+8])
		off += 8
		n := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if n != uint32(format.PageSize) {
			return nil, ErrJournalCorrupt
		}
		image := append([]byte(nil), payload[off:off+int(n)]...)
		off += int(n)
		updates = append(updates, PageUpdate{Offset: pageOffset, Image: image})
	}
	return updates, nil
}

func readAtFull(dev device.Device, b []byte, off int64) error {
	n, err := dev.ReadAt(b, off)
	if err != nil && !(err == io.EOF && n == len(b)) {
		return err
	}
	if n != len(b) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func writeAtFull(dev device.Device, b []byte, off int64) error {
	n, err := dev.WriteAt(b, off)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}


func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
