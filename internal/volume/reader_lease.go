package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockmesh/internal/lock"
)

// ReaderLeaseConfig maps each (volume,node,boot,epoch) tuple to a unique
// sanlock resource. Resource names must be stable across all nodes.
type ReaderLeaseConfig struct {
	Locker *lock.SanlockLocker
	ResourceFor func(volumeID, nodeID, bootID string, epoch uint64) string
	PollInterval time.Duration
}

// ReaderLease is a process-lifetime distributed lease. The owner must stop
// issuing reads immediately when Lost is closed. The sanlock helper is
// parent-death-bound so a crashed metadata process does not leave an orphan
// reader lease behind.
type ReaderLease struct {
	lease lock.Lease
	lost chan struct{}
	done chan struct{}
	monitorDone chan struct{}
	once sync.Once
	errMu sync.Mutex
	err error
}

func (l *ReaderLease) Lost() <-chan struct{} { return l.lost }
func (l *ReaderLease) Valid() bool {
	if l == nil || l.lease == nil { return false }
	select { case <-l.lost: return false; default: return l.lease.Valid() }
}
func (l *ReaderLease) Err() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	return l.err
}
func (l *ReaderLease) Close() error {
	if l == nil { return nil }
	l.once.Do(func() { close(l.done) })
	if l.monitorDone != nil { <-l.monitorDone }
	if l.lease != nil { return l.lease.Close() }
	return nil
}

// AcquireReader acquires a unique sanlock resource for one reader instance.
// onLost is mandatory: loss of the sanlock helper must synchronously fence
// the local data path (for example suspend the DM device and stop new bios).
func (c ReaderLeaseConfig) AcquireReader(ctx context.Context, volumeID, nodeID, bootID string, epoch uint64, onLost func(error)) (*ReaderLease, error) {
	if c.Locker == nil || c.ResourceFor == nil || onLost == nil || volumeID == "" || nodeID == "" || bootID == "" || epoch == 0 {
		return nil, errors.New("reader lease requires sanlock locker, identity, epoch and fail-stop callback")
	}
	resource := c.ResourceFor(volumeID, nodeID, bootID, epoch)
	if resource == "" { return nil, errors.New("reader lease resource is empty") }
	lease, err := c.Locker.Acquire(ctx, resource)
	if err != nil { return nil, err }
	r := &ReaderLease{lease: lease, lost: make(chan struct{}), done: make(chan struct{}), monitorDone: make(chan struct{})}
	interval := c.PollInterval
	if interval <= 0 { interval = 250 * time.Millisecond }
	go func() {
		defer close(r.monitorDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				if !lease.Valid() {
					err := fmt.Errorf("sanlock reader lease lost: %s", resource)
					r.errMu.Lock(); r.err = err; r.errMu.Unlock()
					close(r.lost)
					onLost(err)
					return
				}
			}
		}
	}()
	return r, nil
}
