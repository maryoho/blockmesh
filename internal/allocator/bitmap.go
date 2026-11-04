package allocator

import "errors"

const BlockSize = uint64(4 << 20)
const BlocksPerBitmapPage = 32768

// Bitmap stores one bit per 4 MiB physical block. Bit 1 means allocated.
type Bitmap struct {
	bits  []byte
	total uint64
}

// NewBitmap constructs an empty bitmap. New is reserved for the Allocator
// transaction manager in allocator.go.
func NewBitmap(total uint64) *Bitmap {
	return &Bitmap{bits: make([]byte, (total+7)/8), total: total}
}

func (b *Bitmap) Total() uint64 { return b.total }
func (b *Bitmap) Allocated(id uint64) bool {
	if id >= b.total {
		return false
	}
	return b.bits[id/8]&(1<<(id%8)) != 0
}
func (b *Bitmap) Allocate() (uint64, error) {
	for i := uint64(0); i < b.total; i++ {
		if !b.Allocated(i) {
			b.bits[i/8] |= 1 << (i % 8)
			return i, nil
		}
	}
	return 0, errors.New("pool full")
}
func (b *Bitmap) Mark(id uint64, used bool) error {
	if id >= b.total {
		return errors.New("block id out of range")
	}
	mask := byte(1 << (id % 8))
	if used {
		b.bits[id/8] |= mask
	} else {
		b.bits[id/8] &^= mask
	}
	return nil
}
func (b *Bitmap) Bytes() []byte { return append([]byte(nil), b.bits...) }
func FromBytes(total uint64, data []byte) (*Bitmap, error) {
	if uint64(len(data)) != (total+7)/8 {
		return nil, errors.New("bitmap size mismatch")
	}
	return &Bitmap{bits: append([]byte(nil), data...), total: total}, nil
}
