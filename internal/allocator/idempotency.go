package allocator

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"blockmesh/internal/format"
)

const (
	idempotencyMagic = "BMIDEM01"
	idempotencyVersion = uint32(1)
	idempotencyHeaderSize = 64
	idempotencyCRCOffset = int(format.PageSize) - 4
)

var (
	ErrIdempotencyRecord = errors.New("invalid persistent idempotency record")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
	ErrIdempotencyFull = errors.New("persistent idempotency index is full")
)

// IdempotencyRecord is one immutable record page in the Volume PersistentArena.
// NextPage links records with the same 63-bit hash, so a hash collision never
// aliases two 128-bit keys. The B+Tree index stores hash -> head page.
type IdempotencyRecord struct {
	NextPage uint64
	Key [16]byte
	Fingerprint [32]byte
	LogicalStart uint64
	PhysicalStart uint64
	BlockCount uint32
	Operation uint16
	OwnerEpoch uint64
}

func (r IdempotencyRecord) Encode() ([]byte, error) {
	if r.Key == ([16]byte{}) || r.Fingerprint == ([32]byte{}) ||
		r.BlockCount == 0 || r.OwnerEpoch == 0 ||
		r.LogicalStart > ^uint64(0)-uint64(r.BlockCount) ||
		r.PhysicalStart > ^uint64(0)-uint64(r.BlockCount) {
		return nil, ErrIdempotencyRecord
	}
	b := make([]byte, format.PageSize)
	copy(b[:8], idempotencyMagic)
	binary.LittleEndian.PutUint32(b[8:12], idempotencyVersion)
	binary.LittleEndian.PutUint64(b[16:24], r.NextPage)
	copy(b[24:40], r.Key[:])
	copy(b[40:72], r.Fingerprint[:])
	binary.LittleEndian.PutUint64(b[72:80], r.LogicalStart)
	binary.LittleEndian.PutUint64(b[80:88], r.PhysicalStart)
	binary.LittleEndian.PutUint32(b[88:92], r.BlockCount)
	binary.LittleEndian.PutUint16(b[92:94], r.Operation)
	binary.LittleEndian.PutUint64(b[96:104], r.OwnerEpoch)
	binary.LittleEndian.PutUint32(b[idempotencyCRCOffset:], crc32.Checksum(b[:idempotencyCRCOffset], castagnoli))
	return b, nil
}

func DecodeIdempotencyRecord(b []byte) (IdempotencyRecord, error) {
	var r IdempotencyRecord
	if len(b) != int(format.PageSize) || string(b[:8]) != idempotencyMagic ||
		binary.LittleEndian.Uint32(b[8:12]) != idempotencyVersion ||
		crc32.Checksum(b[:idempotencyCRCOffset], castagnoli) != binary.LittleEndian.Uint32(b[idempotencyCRCOffset:]) {
		return r, ErrIdempotencyRecord
	}
	for _, v := range b[12:16] { if v != 0 { return r, ErrIdempotencyRecord } }
	for _, v := range b[94:96] { if v != 0 { return r, ErrIdempotencyRecord } }
	for _, v := range b[104:idempotencyCRCOffset] { if v != 0 { return r, ErrIdempotencyRecord } }
	r.NextPage = binary.LittleEndian.Uint64(b[16:24])
	copy(r.Key[:], b[24:40])
	copy(r.Fingerprint[:], b[40:72])
	r.LogicalStart = binary.LittleEndian.Uint64(b[72:80])
	r.PhysicalStart = binary.LittleEndian.Uint64(b[80:88])
	r.BlockCount = binary.LittleEndian.Uint32(b[88:92])
	r.Operation = binary.LittleEndian.Uint16(b[92:94])
	r.OwnerEpoch = binary.LittleEndian.Uint64(b[96:104])
	if _, err := r.Encode(); err != nil { return IdempotencyRecord{}, err }
	return r, nil
}

// IdempotencyHash reserves the high key-space bit for idempotency index keys.
// Pool logical block addresses are bounded by PoolSize/BlockSize and therefore
// cannot overlap this namespace. Exact 128-bit key comparison is still required
// after lookup; the hash is an index, not an identity.
func IdempotencyHash(key [16]byte) uint64 {
	h := crc32.Checksum(key[:], castagnoli)
	// Mix both halves and keep the namespace bit set.
	v := uint64(h)<<32 | uint64(crc32.ChecksumIEEE(key[:]))
	return (uint64(1)<<63) | (v & ((uint64(1)<<63)-1))
}
