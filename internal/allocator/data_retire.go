package allocator

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sort"

	"blockmesh/internal/btree"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

const (
	DataRetireRecordPageType uint16 = 0x101
	dataRetireMagic = "BMDRET01"
	dataRetireVersion = uint32(1)
	dataRetirePayloadSize = 48
	dataRetireNamespace = uint64(1) << 63
)

var ErrDataRetireRecord = errors.New("invalid persistent Data Retire record")

// DataRetireRecord pins a physical DataView extent after a Mapping root switch.
// RetireGeneration is the root generation that could still reference the old
// extent. AG is persisted because Bitmap WAL ownership is AG-scoped.
type DataRetireRecord struct {
	IndexKey uint64
	AG uint32
	RetireGeneration uint64
	PhysicalStart uint64
	BlockCount uint32
}

func (r DataRetireRecord) EncodePayload() ([]byte, error) {
	if r.IndexKey&dataRetireNamespace == 0 || r.RetireGeneration == 0 ||
		r.BlockCount == 0 || r.PhysicalStart > math.MaxUint64-uint64(r.BlockCount) {
		return nil, ErrDataRetireRecord
	}
	b := make([]byte, dataRetirePayloadSize)
	copy(b[:8], dataRetireMagic)
	binary.LittleEndian.PutUint32(b[8:12], dataRetireVersion)
	binary.LittleEndian.PutUint32(b[12:16], r.AG)
	binary.LittleEndian.PutUint64(b[16:24], r.IndexKey)
	binary.LittleEndian.PutUint64(b[24:32], r.RetireGeneration)
	binary.LittleEndian.PutUint64(b[32:40], r.PhysicalStart)
	binary.LittleEndian.PutUint32(b[40:44], r.BlockCount)
	binary.LittleEndian.PutUint32(b[44:48], crc32.Checksum(b[:44], castagnoli))
	return b, nil
}

func DecodeDataRetireRecordPayload(b []byte) (DataRetireRecord, error) {
	var r DataRetireRecord
	if len(b) != dataRetirePayloadSize || string(b[:8]) != dataRetireMagic ||
		binary.LittleEndian.Uint32(b[8:12]) != dataRetireVersion ||
		crc32.Checksum(b[:44], castagnoli) != binary.LittleEndian.Uint32(b[44:48]) {
		return r, ErrDataRetireRecord
	}
	r = DataRetireRecord{
		AG: binary.LittleEndian.Uint32(b[12:16]),
		IndexKey: binary.LittleEndian.Uint64(b[16:24]),
		RetireGeneration: binary.LittleEndian.Uint64(b[24:32]),
		PhysicalStart: binary.LittleEndian.Uint64(b[32:40]),
		BlockCount: binary.LittleEndian.Uint32(b[40:44]),
	}
	if _, err := r.EncodePayload(); err != nil { return DataRetireRecord{}, err }
	return r, nil
}

func BuildDataRetirePage(id, pageGeneration uint64, r DataRetireRecord) (metastore.Page, error) {
	if id == 0 || id == metastore.NoPage || pageGeneration == 0 {
		return metastore.Page{}, ErrDataRetireRecord
	}
	payload, err := r.EncodePayload()
	if err != nil { return metastore.Page{}, err }
	return metastore.Page{ID:id, Type:DataRetireRecordPageType, Generation:pageGeneration, Payload:payload}, nil
}

// dataRetireIndexKey hashes the immutable range identity into the high-bit
// external-page namespace. The caller probes on collision against the live
// B+Tree, so it cannot overwrite an idempotency index entry.
func dataRetireIndexKey(r DataRetireRecord) uint64 {
	var b [24]byte
	binary.LittleEndian.PutUint32(b[0:4], r.AG)
	binary.LittleEndian.PutUint64(b[4:12], r.RetireGeneration)
	binary.LittleEndian.PutUint64(b[12:20], r.PhysicalStart)
	binary.LittleEndian.PutUint32(b[20:24], r.BlockCount)
	h := crc32.Checksum(b[:], castagnoli)
	return dataRetireNamespace | uint64(h)
}

func reserveDataRetireKeys(tree *btree.Tree, ranges []DataRetireRecord) ([]DataRetireRecord, error) {
	out := make([]DataRetireRecord, len(ranges))
	used := make(map[uint64]struct{}, len(ranges))
	for i, r := range ranges {
		key := dataRetireIndexKey(r)
		for probes := uint64(0); probes < 1<<20; probes++ {
			if _, exists := used[key]; exists {
				key++
				continue
			}
			if _, err := tree.Get(key); errors.Is(err, btree.ErrKeyNotFound) {
				break
			} else if err != nil {
				return nil, err
			}
			key++
			if key&dataRetireNamespace == 0 { key = dataRetireNamespace }
			if probes == (1<<20)-1 { return nil, ErrDataRetireRecord }
		}
		r.IndexKey = key
		used[key] = struct{}{}
		out[i] = r
	}
	return out, nil
}

// dataRetireRanges compacts old physical block IDs into non-overlapping
// contiguous ranges. It rejects duplicates and preserves the AG boundary.
func dataRetireRanges(physical []uint64, generation uint64, agSizeBlocks uint64) ([]DataRetireRecord, error) {
	if generation == 0 || agSizeBlocks == 0 { return nil, ErrDataRetireRecord }
	ids := append([]uint64(nil), physical...)
	if len(ids) == 0 { return nil, nil }
	// A Volume Descriptor is bound to one AG. Cross-AG extents would require a
	// multi-journal atomic transaction, which BMPOOL03 intentionally forbids.
	for i := range ids {
		if ids[i] >= math.MaxUint64-1 { return nil, ErrDataRetireRecord }
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]DataRetireRecord, 0)
	for i := 0; i < len(ids); {
		start := ids[i]
		ag := uint32(start / agSizeBlocks)
		end := i + 1
		for end < len(ids) && ids[end] == ids[end-1]+1 && uint32(ids[end]/agSizeBlocks) == ag { end++ }
		for j := i + 1; j < end; j++ {
			if ids[j] == ids[j-1] { return nil, ErrDataRetireRecord }
		}
		out = append(out, DataRetireRecord{AG:ag, RetireGeneration:generation,
			PhysicalStart:start, BlockCount:uint32(end-i)})
		i = end
	}
	return out, nil
}

// PlanDynamicBitmapFree produces redo images that clear a previously allocated
// extent. Every bit must currently be set; a double-free or cross-AG request
// fails closed. The descriptor checksum and sequence are updated with bitmap
// pages in the same WAL transaction.
func PlanDynamicBitmapFree(r metastore.Device, sb format.DynamicSuperblock, ag uint32, startBlock uint64, count uint32) ([]metastore.PageImage, error) {
	if r == nil || sb.Validate() != nil || count == 0 { return nil, ErrDataRetireRecord }
	g := sb.Geometry
	agBlocks, err := g.AGBlocks(ag)
	if err != nil { return nil, err }
	agStart, err := g.AGStartBlock(ag)
	if err != nil { return nil, err }
	if startBlock < agStart || startBlock-agStart > agBlocks || uint64(count) > agBlocks-(startBlock-agStart) {
		return nil, ErrDataRetireRecord
	}
	base := g.BitmapOffset + uint64(ag)*g.BitmapStride
	desc := make([]byte, format.PageSize)
	if err := readAtFull(r, desc, int64(base)); err != nil { return nil, err }
	if binary.LittleEndian.Uint64(desc[:8]) != bitmapDescriptorMagic ||
		binary.LittleEndian.Uint32(desc[8:12]) != ag { return nil, ErrDataRetireRecord }
	seq := binary.LittleEndian.Uint64(desc[16:24])
	if seq == math.MaxUint64 { return nil, ErrDataRetireRecord }
	bitmap := make([]byte, g.BitmapPaddedBytes)
	if err := readAtFull(r, bitmap, int64(base+format.PageSize)); err != nil { return nil, err }
	if crc32.Checksum(bitmap, castagnoli) != binary.LittleEndian.Uint32(desc[24:28]) {
		return nil, ErrDataRetireRecord
	}
	local := startBlock-agStart
	for i := uint64(0); i < uint64(count); i++ {
		bit := local+i
		if bitmap[bit/8]&(1<<(bit%8)) == 0 { return nil, ErrDataRetireRecord }
	}
	for i := uint64(0); i < uint64(count); i++ {
		bit := local+i
		bitmap[bit/8] &^= 1 << (bit%8)
	}
	firstPage := local/(format.PageSize*8)
	lastPage := (local+uint64(count)-1)/(format.PageSize*8)
	images := make([]metastore.PageImage, 0, int(lastPage-firstPage+2))
	for p := firstPage; p <= lastPage; p++ {
		off := base+format.PageSize+p*format.PageSize
		page := append([]byte(nil), bitmap[p*format.PageSize:(p+1)*format.PageSize]...)
		images = append(images, metastore.PageImage{Offset:off, Data:page})
	}
	next := append([]byte(nil), desc...)
	binary.LittleEndian.PutUint64(next[16:24], seq+1)
	binary.LittleEndian.PutUint32(next[24:28], crc32.Checksum(bitmap, castagnoli))
	images = append(images, metastore.PageImage{Offset:base, Data:next})
	return images, nil
}
