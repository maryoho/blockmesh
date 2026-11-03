// Package lock defines the lock abstraction used by the metadata engine.
// FileLocker is ONLY a single-host development/test backend. It is not a
// distributed lock and must never be used by multiple SAN-connected hosts.
package lock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Lease interface {
	Resource() string
	Valid() bool
	Close() error
}

type Locker interface {
	// Acquire holds an exclusive resource for the lifetime of the returned Lease.
	Acquire(ctx context.Context, resource string) (Lease, error)
	// Lock is a short-lived exclusive lock, normally used for AG and mapping pages.
	Lock(ctx context.Context, resource string) (unlock func() error, err error)
}

type FileLocker struct{ Dir string }

type fileLease struct {
	f        *os.File
	resource string
	mu       sync.Mutex
	closed   bool
}

func NewFileLocker(dir string) (*FileLocker, error) {
	if dir == "" {
		return nil, fmt.Errorf("lock directory is empty")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &FileLocker{Dir: dir}, nil
}

func (l *FileLocker) Acquire(ctx context.Context, resource string) (Lease, error) {
	if strings.TrimSpace(resource) == "" {
		return nil, fmt.Errorf("empty lock resource")
	}
	sum := sha256.Sum256([]byte(resource))
	path := filepath.Join(l.Dir, hex.EncodeToString(sum[:])+".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &fileLease{f: f, resource: resource}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (l *FileLocker) Lock(ctx context.Context, resource string) (func() error, error) {
	lease, err := l.Acquire(ctx, resource)
	if err != nil {
		return nil, err
	}
	return lease.Close, nil
}

func (l *fileLease) Resource() string { return l.resource }
func (l *fileLease) Valid() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.closed
}
func (l *fileLease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	unlockErr := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	closeErr := l.f.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
