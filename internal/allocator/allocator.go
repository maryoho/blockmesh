// Package allocator implements single-block allocation transactions.
// A transaction updates the AG bitmap descriptor, one bitmap data page and one
// caller-supplied Mapping page in a single redo-WAL commit.
package allocator

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"bblockmesh/internal/format"
	"bblockmesh/internal/lock"
	"bblockmesh/internal/metadata"
	"bblockmesh/internal/wal"
)

const bitmapDescriptorMagic = uint64(0x424D424D41503031) // BMBMAP01
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Allocator struct {
	Store *metadata.Store
	Locks lock.Locker
}

func New(store *metadata.Store, locks lock.Locker) (*Allocator, error) {
	if store == nil || locks == nil {
		return nil, fmt.Errorf("store and lock provider are required")
	}
	return &Allocator{Store: store, Locks: locks}, nil
}

// AllocateOne allocates one 4MiB physical block from ag and atomically publishes
// the supplied Mapping page. buildMapping receives the old page image and the
// chosen global physical block, and must return a complete 4KiB new page image.
// mappingOffset must point into the Mapping region. The caller's Volume Owner
// lease is represented by the volume:<volumeID> lock resource.
func (a *Allocator) AllocateOne(ctx context.Context, volumeLease lock.Lease, volumeID string, ag uint32, mappingOffset uint64, buildMapping func(oldPage []byte, physicalBlock uint64) ([]byte, error)) (uint64, error) {
	if volumeID == "" {
		return 0, fmt.Errorf("volume ID is empty")
	}
	if volumeLease == nil || !volumeLease.Valid() || volumeLease.Resource() != "volume:"+volumeID {
		return 0, fmt.Errorf("a valid exclusive Volume Owner Lease is required")
	}
	if ag >= a.Store.SB.AGCount {
		return 0, fmt.Errorf("AG %d out of range", ag)
	}
	if mappingOffset%format.PageSize != 0 || mappingOffset < a.Store.SB.MappingOffset || mappingOffset+format.PageSize > a.Store.SB.CheckpointOffset {
		return 0, fmt.Errorf("mapping page offset %d outside Mapping region", mappingOffset)
	}
	if buildMapping == nil {
		return 0, fmt.Errorf("mapping page builder is nil")
	}

	unlockAG, err := a.Locks.Lock(ctx, fmt.Sprintf("ag:%03d", ag))
	if err != nil {
		return 0, fmt.Errorf("acquire AG lock: %w", err)
	}
	defer unlockAG()
	unlockMapping, err := a.Locks.Lock(ctx, fmt.Sprintf("mapping-page:%d", mappingOffset))
	if err != nil {
		return 0, fmt.Errorf("acquire mapping page lock: %w", err)
	}
	defer unlockMapping()

	journal, err := wal.New(a.Store.Device(), a.Store.SB, ag)
	if err != nil {
		return 0, err
	}
	if _, err = journal.Recover(); err != nil {
		return 0, fmt.Errorf("recover AG %d WAL: %w", ag, err)
	}

	base := a.Store.SB.BitmapOffset + uint64(ag)*format.AGBitmapStride
	desc := make([]byte, format.PageSize)
	if err = a.Store.Device().ReadAt(desc, int64(base)); err != nil {
		return 0, err
	}
	if binary.LittleEndian.Uint64(desc[0:8]) != bitmapDescriptorMagic || binary.LittleEndian.Uint32(desc[8:12]) != ag {
		return 0, fmt.Errorf("AG %d bitmap descriptor invalid", ag)
	}
	seq := binary.LittleEndian.Uint64(desc[16:24])
	bitmap := make([]byte, format.BitmapBytes)
	if err = a.Store.Device().ReadAt(bitmap, int64(base+format.PageSize)); err != nil {
		return 0, err
	}
	if crc32.Checksum(bitmap, castagnoli) != binary.LittleEndian.Uint32(desc[24:28]) {
		return 0, fmt.Errorf("AG %d bitmap CRC32C mismatch", ag)
	}

	local, ok := findFree(bitmap)
	if !ok {
		return 0, fmt.Errorf("AG %d is full", ag)
	}
	bitmap[local/8] |= byte(1 << (local % 8))
	physical := uint64(ag)*format.AGBlocks + local

	bitmapPageIndex := local / (format.PageSize * 8)
	bitmapPageOffset := base + format.PageSize + bitmapPageIndex*format.PageSize
	bitmapPage := append([]byte(nil), bitmap[bitmapPageIndex*format.PageSize:(bitmapPageIndex+1)*format.PageSize]...)

	newDesc := make([]byte, format.PageSize)
	copy(newDesc, desc)
	binary.LittleEndian.PutUint64(newDesc[16:24], seq+1)
	binary.LittleEndian.PutUint32(newDesc[24:28], crc32.Checksum(bitmap, castagnoli))

	oldMapping, err := a.Store.ReadPage(mappingOffset)
	if err != nil {
		return 0, err
	}
	newMapping, err := buildMapping(oldMapping, physical)
	if err != nil {
		return 0, fmt.Errorf("build mapping page: %w", err)
	}
	if len(newMapping) != int(format.PageSize) {
		return 0, fmt.Errorf("mapping builder must return exactly 4KiB")
	}

	updates := []wal.PageUpdate{
		{Offset: base, Image: newDesc},
		{Offset: bitmapPageOffset, Image: bitmapPage},
		{Offset: mappingOffset, Image: newMapping},
	}
	if err = journal.Commit(updates); err != nil {
		return 0, fmt.Errorf("commit allocation transaction: %w", err)
	}
	return physical, nil
}

func findFree(bitmap []byte) (uint64, bool) {
	for i, b := range bitmap {
		if b == 0xff {
			continue
		}
		for bit := uint(0); bit < 8; bit++ {
			if b&(1<<bit) == 0 {
				return uint64(i*8) + uint64(bit), true
			}
		}
	}
	return 0, false
}

// RecoverAG obtains the AG lock and replays its committed redo transaction.
// Before calling this after a node failure, the caller must complete the
// platform-specific fencing procedure for the previous owner.
func (a *Allocator) RecoverAG(ctx context.Context, ag uint32) (bool, error) {
	if ag >= a.Store.SB.AGCount {
		return false, fmt.Errorf("AG %d out of range", ag)
	}
	unlock, err := a.Locks.Lock(ctx, fmt.Sprintf("ag:%03d", ag))
	if err != nil {
		return false, fmt.Errorf("acquire AG lock: %w", err)
	}
	defer unlock()
	j, err := wal.New(a.Store.Device(), a.Store.SB, ag)
	if err != nil {
		return false, err
	}
	return j.Recover()
}

// RecoverAll replays all AG journals serially. It is intended for controlled
// pool startup/recovery, not for the ordinary I/O path.
func (a *Allocator) RecoverAll(ctx context.Context) (int, error) {
	replayed := 0
	for ag := uint32(0); ag < a.Store.SB.AGCount; ag++ {
		ok, err := a.RecoverAG(ctx, ag)
		if err != nil {
			return replayed, fmt.Errorf("recover AG %d: %w", ag, err)
		}
		if ok {
			replayed++
		}
	}
	return replayed, nil
}
