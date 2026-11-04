package device

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

var (
	ErrClosed   = errors.New("device closed")
	ErrReadOnly = errors.New("device is read-only")
)

// Device is the random-access metadata-device contract. Size reports the
// backing object's byte size; production block-device backends may implement
// this with BLKGETSIZE64 rather than FileDevice's regular-file Stat size.
type Device interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
	Size() int64
	Close() error
}

// FileDevice is a regular-file development/test backend. Production should
// provide a block-device implementation and ensure flushes reach the SAN.
type FileDevice struct {
	mu       sync.Mutex
	f        *os.File
	readOnly bool
	closed   bool
}

// Open opens an existing device. Writable opens also create a missing regular
// file, which is useful for metadata image initialization.
func Open(path string, writable bool) (*FileDevice, error) {
	flags := os.O_RDONLY
	if writable {
		flags = os.O_CREATE | os.O_RDWR
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	return &FileDevice{f: f, readOnly: !writable}, nil
}

// OpenFile is kept as the simple writable file-backed constructor.
func OpenFile(path string) (*FileDevice, error) {
	return Open(path, true)
}

func (d *FileDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrClosed
	}
	n, err := d.f.ReadAt(p, off)
	if err == io.EOF && n == len(p) {
		err = nil
	}
	return n, err
}

func (d *FileDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrClosed
	}
	if d.readOnly {
		return 0, ErrReadOnly
	}
	return d.f.WriteAt(p, off)
}

// Discard punches a hole in a regular-file metadata image without
// materializing zero-filled blocks. It is an optimization for sparse test and
// development images only; production block-device backends must implement
// their own discard/zero semantics and do not use FileDevice.
func (d *FileDevice) Discard(offset, length uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed { return ErrClosed }
	if d.readOnly { return ErrReadOnly }
	if offset > uint64(^uint64(0)>>1) || length > uint64(^uint64(0)>>1)-offset {
		return errors.New("discard range overflows file offset")
	}
	info, err := d.f.Stat()
	if err != nil { return err }
	if !info.Mode().IsRegular() { return errors.New("discard is supported only for regular files") }
	return syscall.Fallocate(int(d.f.Fd()), 0x01|0x02, int64(offset), int64(length))
}

func (d *FileDevice) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if d.readOnly {
		return ErrReadOnly
	}
	return d.f.Sync()
}

func (d *FileDevice) Size() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0
	}
	info, err := d.f.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (d *FileDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.f.Close()
}
