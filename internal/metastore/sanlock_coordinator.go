package metastore

import (
	"context"
	"errors"
	"sync"
	"time"

	"blockmesh/internal/lock"
)

// SanlockReaderCoordinator implements the reader-set barrier with a
// sanlock-backed distributed resource lease. Every node must use the same
// lockspace/resource name and the same shared metadata LUN.
type SanlockReaderCoordinator struct {
	Locker *lock.SanlockLocker
	ResourceForVolume func(string) string
	Timeout time.Duration
}

func (c *SanlockReaderCoordinator) LockReaderSet(volumeID string) (func(), error) {
	timeout := 30 * time.Second
	if c != nil && c.Timeout > 0 { timeout = c.Timeout }
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.LockReaderSetContext(ctx, volumeID)
}

func (c *SanlockReaderCoordinator) LockReaderSetContext(parent context.Context, volumeID string) (func(), error) {
	if c == nil || c.Locker == nil || c.ResourceForVolume == nil {
		return nil, ErrReaderCoordinatorRequired
	}
	resource := c.ResourceForVolume(volumeID)
	if resource == "" { return nil, errors.New("empty sanlock reader-set resource") }
	timeout := c.Timeout
	if timeout <= 0 { timeout = 30 * time.Second }
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	release, err := c.Locker.Lock(ctx, resource)
	if err != nil { return nil, err }
	var once sync.Once
	return func() { once.Do(func() { _ = release() }) }, nil
}
