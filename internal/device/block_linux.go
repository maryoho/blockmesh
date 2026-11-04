//go:build linux

package device

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	blkGetSize64 = 0x80081272
	blkSSZGet    = 0x1268
)

var (
	ErrNotBlockDevice = errors.New("path is not a Linux block device")
	ErrInvalidSector  = errors.New("invalid logical sector size")
)

// BlockDevice is a Linux block-device handle. It intentionally does not use
// O_DIRECT: all metadata I/O is 4 KiB aligned and the kernel block layer still
// provides ordering/flush semantics through fsync. Deployments must ensure
// the underlying multipath device is the sole metadata path used by a node.
type BlockDevice struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	size     int64
	sector   int
	readOnly bool
	closed   bool
}

func OpenBlockDevice(path string, writable, exclusive bool) (*BlockDevice, error) {
	if path == "" {
		return nil, errors.New("block device path is required")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return nil, fmt.Errorf("stat block device %q: %w", path, err)
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFBLK {
		return nil, fmt.Errorf("%w: %s", ErrNotBlockDevice, path)
	}
	flags := syscall.O_CLOEXEC
	if writable {
		flags |= syscall.O_RDWR
	} else {
		flags |= syscall.O_RDONLY
	}
	if exclusive {
		flags |= syscall.O_EXCL
	}
	fd, err := syscall.Open(path, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("open block device %q: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	fail := func(err error) (*BlockDevice, error) {
		_ = f.Close()
		return nil, err
	}

	var size uint64
	if err = ioctl(fd, blkGetSize64, uintptr(unsafe.Pointer(&size))); err != nil {
		return fail(fmt.Errorf("BLKGETSIZE64 %q: %w", path, err))
	}
	var sector int32
	if err = ioctl(fd, blkSSZGet, uintptr(unsafe.Pointer(&sector))); err != nil {
		return fail(fmt.Errorf("BLKSSZGET %q: %w", path, err))
	}
	if sector < 512 || sector > 65536 || (sector&(sector-1)) != 0 {
		return fail(fmt.Errorf("%w: %d", ErrInvalidSector, sector))
	}
	if size == 0 || size > uint64(^uint64(0)>>1) {
		return fail(fmt.Errorf("invalid block device capacity: %d", size))
	}
	if size%uint64(sector) != 0 {
		return fail(fmt.Errorf("device capacity %d is not a multiple of logical sector size %d", size, sector))
	}
	return &BlockDevice{file: f, path: path, size: int64(size), sector: int(sector), readOnly: !writable}, nil
}

func ioctl(fd int, request uintptr, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), request, arg)
	if errno != 0 {
		return errno
	}
	return nil
}

func (d *BlockDevice) Path() string { return d.path }
func (d *BlockDevice) LogicalSectorSize() int { return d.sector }
func (d *BlockDevice) ReadOnly() bool { return d.readOnly }

func (d *BlockDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrClosed
	}
	if off < 0 || off > d.size || int64(len(p)) > d.size-off {
		return 0, io.ErrUnexpectedEOF
	}
	n, err := d.file.ReadAt(p, off)
	if err == io.EOF && n == len(p) {
		err = nil
	}
	return n, err
}

func (d *BlockDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrClosed
	}
	if d.readOnly {
		return 0, ErrReadOnly
	}
	if off < 0 || off > d.size || int64(len(p)) > d.size-off {
		return 0, io.ErrShortWrite
	}
	return d.file.WriteAt(p, off)
}

// Sync asks the kernel to flush all writes issued through this descriptor.
// On a read-only descriptor there cannot be dirty writes from this handle.
func (d *BlockDevice) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if d.readOnly {
		return nil
	}
	return d.file.Sync()
}

func (d *BlockDevice) Size() int64 {
	if d == nil || d.closed {
		return 0
	}
	return d.size
}

func (d *BlockDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.file.Close()
}

var _ Device = (*BlockDevice)(nil)
