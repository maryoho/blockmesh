package device

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
)

// ErrRangeClosed is returned after a bounded device view is closed.
var ErrRangeClosed = errors.New("device range is closed")

// Range is a bounded, non-owning view over a byte range of a Device.
// Multiple Range values may safely refer to the same parent Device. Closing a
// Range never closes the parent; the component that opened the parent owns its
// lifetime. All offsets exposed by Range are relative to the beginning of the
// view, while parent I/O uses absolute device byte offsets.
type Range struct {
	parent Device
	base   int64
	size   int64

	mu     sync.RWMutex
	closed bool
}

// NewRange creates a bounded view [offset, offset+length) of parent.
func NewRange(parent Device, offset, length uint64) (*Range, error) {
	if parent == nil {
		return nil, errors.New("range parent device is required")
	}
	if offset > math.MaxInt64 || length > math.MaxInt64 {
		return nil, errors.New("device range exceeds signed I/O offset limit")
	}
	deviceSize := parent.Size()
	if deviceSize < 0 || offset > uint64(deviceSize) || length > uint64(deviceSize)-offset {
		return nil, fmt.Errorf("device range [%d,%d) exceeds device size %d", offset, offset+length, deviceSize)
	}
	return &Range{parent: parent, base: int64(offset), size: int64(length)}, nil
}

// ReadAt reads only within the view. A request extending beyond its end returns
// io.EOF after reading the in-range prefix, matching io.ReaderAt semantics.
func (r *Range) ReadAt(p []byte, off int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, ErrRangeClosed
	}
	if off < 0 {
		return 0, errors.New("negative device range offset")
	}
	if off >= r.size {
		if len(p) == 0 && off == r.size {
			return 0, nil
		}
		return 0, io.EOF
	}
	available := r.size - off
	if int64(len(p)) > available {
		n, err := r.parent.ReadAt(p[:int(available)], r.base+off)
		if err != nil && !errors.Is(err, io.EOF) {
			return n, err
		}
		return n, io.EOF
	}
	return r.parent.ReadAt(p, r.base+off)
}

// WriteAt rejects the entire request if any byte would cross the view boundary.
func (r *Range) WriteAt(p []byte, off int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, ErrRangeClosed
	}
	if off < 0 || off > r.size || int64(len(p)) > r.size-off {
		return 0, errors.New("device write outside bounded range")
	}
	if r.base > math.MaxInt64-off {
		return 0, errors.New("device range offset overflow")
	}
	return r.parent.WriteAt(p, r.base+off)
}

func (r *Range) Sync() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrRangeClosed
	}
	return r.parent.Sync()
}

func (r *Range) Size() int64 { return r.size }

// Close closes only this view. The parent Device remains owned by its creator.
func (r *Range) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

var _ Device = (*Range)(nil)
