package device

import (
	"errors"
	"io"
	"os"
	"sync"
)

var ErrClosed = errors.New("device closed")

type Device interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
	Close() error
}

// FileDevice is useful for development and tests. Production should provide a
// block-device implementation and ensure cache flushes reach the SAN target.
type FileDevice struct {
	mu     sync.Mutex
	f      *os.File
	closed bool
}

func OpenFile(path string) (*FileDevice, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &FileDevice{f: f}, nil
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
	return d.f.WriteAt(p, off)
}
func (d *FileDevice) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	return d.f.Sync()
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
