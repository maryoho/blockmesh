package volume

import (
	"context"
	"errors"
	"sync"
)

var ErrLeaseLost = errors.New("volume owner lease lost")

type Lease interface {
	AcquireOwner(context.Context, string, string) (uint64, error)
	RenewOwner(context.Context, string, string, uint64) error
	ReleaseOwner(context.Context, string, string, uint64) error
	ReaderAlive(context.Context, string, string, string) (bool, error)
}

// MemoryLease is process-local test scaffolding, not a distributed lock.
type MemoryLease struct {
	mu     sync.Mutex
	owners map[string]owner
}
type owner struct {
	node  string
	epoch uint64
}

func NewMemoryLease() *MemoryLease { return &MemoryLease{owners: map[string]owner{}} }
func (m *MemoryLease) AcquireOwner(_ context.Context, v, n string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.owners[v]
	if o.node != "" && o.node != n {
		return 0, ErrLeaseLost
	}
	o.node = n
	o.epoch++
	if o.epoch == 0 {
		o.epoch = 1
	}
	m.owners[v] = o
	return o.epoch, nil
}
func (m *MemoryLease) RenewOwner(_ context.Context, v, n string, e uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.owners[v]
	if o.node != n || o.epoch != e {
		return ErrLeaseLost
	}
	return nil
}
func (m *MemoryLease) ReleaseOwner(_ context.Context, v, n string, e uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.owners[v]
	if o.node != n || o.epoch != e {
		return ErrLeaseLost
	}
	delete(m.owners, v)
	return nil
}
func (m *MemoryLease) ReaderAlive(context.Context, string, string, string) (bool, error) {
	return true, nil
}
