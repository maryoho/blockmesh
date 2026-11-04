package volume

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
)

var (
	ErrOwnerRecordConflict = errors.New("authoritative owner record changed concurrently")
	ErrOwnerIdentityReuse = errors.New("same node/boot owner identity cannot be safely fenced as a previous owner")
	ErrOwnerEpochExhausted = errors.New("volume owner epoch exhausted")
)

// OwnerRecord is the authoritative cluster-side identity of the last Owner.
// It must be stored durably with compare-and-swap semantics (for example, a
// Kubernetes Lease with resourceVersion CAS). The Pool Volume Descriptor
// separately stores the epoch used by metadata readers.
type OwnerRecord struct {
	VolumeID string
	NodeID   string
	BootID   string
	Epoch    uint64
}

// OwnerRecordStore must provide linearizable reads and durable CAS updates.
// A nil expected value means the record must not exist.
type OwnerRecordStore interface {
	Load(context.Context, string) (record OwnerRecord, exists bool, err error)
	CompareAndSwap(context.Context, string, *OwnerRecord, OwnerRecord) error
}

// OwnerEpochReader reads the currently durable BMDVOL02 OwnerEpoch after the
// distributed Owner resource has been acquired and old-owner fencing completed.
type OwnerEpochReader func(context.Context, string) (uint64, error)

// OwnerIOBarrier must not return until the old owner's SAN-side metadata I/O
// is known quiescent. PR fencing alone is not a local DM queue drain proof.
type OwnerIOBarrier func(context.Context, string, OwnerRecord) error

type OwnerCoordinatorConfig struct {
	Locker          lock.DistributedLocker
	ResourceFor     func(volumeID string) string
	Records         OwnerRecordStore
	Fencer          fence.Fencer
	ReadEpoch       OwnerEpochReader
	DrainOldOwner   OwnerIOBarrier
	LeasePollPeriod time.Duration
}

type OwnerCoordinator struct { cfg OwnerCoordinatorConfig }

func NewOwnerCoordinator(cfg OwnerCoordinatorConfig) (*OwnerCoordinator, error) {
	if cfg.Locker == nil || !cfg.Locker.Distributed() || cfg.ResourceFor == nil ||
		cfg.Records == nil || cfg.Fencer == nil || cfg.ReadEpoch == nil || cfg.DrainOldOwner == nil {
		return nil, errors.New("owner coordinator requires distributed lock, authoritative record CAS, SAN fencer, epoch reader and old-owner I/O barrier")
	}
	switch cfg.Fencer.(type) {
	case fence.NoopFencer, *fence.NoopFencer:
		return nil, errors.New("NoopFencer is forbidden for Owner takeover")
	}
	if cfg.LeasePollPeriod <= 0 { cfg.LeasePollPeriod = 100 * time.Millisecond }
	return &OwnerCoordinator{cfg:cfg}, nil
}

// AcquireOwner performs the takeover closed loop while holding the exclusive
// sanlock resource: read previous identity -> PR PREEMPT AND ABORT -> verify
// all-path fencing in the Fencer -> drain old-owner metadata I/O -> compute a
// strictly increasing epoch -> CAS-publish the new authoritative identity.
// The caller must still replay WAL and publish that epoch in BMDVOL02 before
// enabling data requests.
func (c *OwnerCoordinator) AcquireOwner(ctx context.Context, volumeID, nodeID, bootID string) (*OwnerLease, error) {
	if c == nil || ctx == nil { return nil, errors.New("owner coordinator and context are required") }
	if strings.TrimSpace(volumeID)=="" || strings.TrimSpace(nodeID)=="" || strings.TrimSpace(bootID)=="" {
		return nil, errors.New("volume, node and boot identities are required")
	}
	resource:=c.cfg.ResourceFor(volumeID)
	if strings.TrimSpace(resource)=="" { return nil, errors.New("owner sanlock resource is empty") }
	lease,err:=c.cfg.Locker.Acquire(ctx,resource)
	if err!=nil { return nil,fmt.Errorf("acquire exclusive Owner sanlock resource: %w",err) }
	releaseOnError:=true
	defer func(){ if releaseOnError { _=lease.Close() } }()
	if !lease.Valid() { return nil,errors.New("Owner sanlock lease became invalid immediately after acquisition") }

	old,exists,err:=c.cfg.Records.Load(ctx,volumeID)
	if err!=nil { return nil,fmt.Errorf("load authoritative previous Owner record: %w",err) }
	if exists {
		if old.VolumeID!=volumeID || old.NodeID=="" || old.BootID=="" || old.Epoch==0 {
			return nil,errors.New("authoritative previous Owner record is malformed")
		}
		if old.NodeID==nodeID && old.BootID==bootID {
			// Do not PR-preempt the same initiator identity: it could remove the
			// new process's own registration. Require an operator/platform
			// recovery path that proves the prior local data path is stopped.
			return nil,ErrOwnerIdentityReuse
		}
		if err=c.cfg.Fencer.FenceNode(ctx,old.NodeID,old.BootID);err!=nil {
			return nil,fmt.Errorf("PREEMPT AND ABORT previous Owner %s/%s on all SAN paths: %w",old.NodeID,old.BootID,err)
		}
		if err=c.cfg.DrainOldOwner(ctx,volumeID,old);err!=nil {
			return nil,fmt.Errorf("old Owner metadata I/O is not drained: %w",err)
		}
	}
	if !lease.Valid() { return nil,errors.New("Owner sanlock lease lost during fencing") }
	descriptorEpoch,err:=c.cfg.ReadEpoch(ctx,volumeID)
	if err!=nil { return nil,fmt.Errorf("read durable Volume OwnerEpoch: %w",err) }
	base:=descriptorEpoch
	var expected *OwnerRecord
	if exists {
		expectedCopy:=old
		expected=&expectedCopy
		if old.Epoch>base { base=old.Epoch }
	}
	if base==math.MaxUint64 { return nil,ErrOwnerEpochExhausted }
	next:=OwnerRecord{VolumeID:volumeID,NodeID:nodeID,BootID:bootID,Epoch:base+1}
	if !lease.Valid() { return nil,errors.New("Owner sanlock lease lost before publishing takeover record") }
	if err=c.cfg.Records.CompareAndSwap(ctx,volumeID,expected,next);err!=nil {
		if errors.Is(err,ErrOwnerRecordConflict) { return nil,err }
		return nil,fmt.Errorf("durably publish new authoritative Owner identity: %w",err)
	}
	if !lease.Valid() {
		// Record is intentionally left as a tombstone-like last owner identity;
		// the next acquirer must fence it before publishing another epoch.
		return nil,errors.New("Owner sanlock lease lost after publishing takeover record")
	}
	out:=newOwnerLease(lease,next.Epoch,c.cfg.LeasePollPeriod)
	releaseOnError=false
	return out,nil
}

// OwnerLease monitors the sanlock helper process. A helper exit means the
// distributed lease can no longer be assumed held; callers must fail-stop the
// data path immediately. It never silently reacquires a lost lease.
type OwnerLease struct {
	lease lock.Lease
	epoch uint64
	lost chan error
	stop chan struct{}
	done chan struct{}
	once sync.Once
	mu sync.Mutex
	err error
}

func newOwnerLease(lease lock.Lease, epoch uint64, poll time.Duration) *OwnerLease {
	l:=&OwnerLease{lease:lease,epoch:epoch,lost:make(chan error,1),stop:make(chan struct{}),done:make(chan struct{})}
	go func(){
		defer close(l.done)
		t:=time.NewTicker(poll);defer t.Stop()
		for {
			select {
			case <-l.stop: return
			case <-t.C:
				if !lease.Valid() {
					l.mu.Lock();l.err=errors.New("distributed Owner sanlock lease lost");err:=l.err;l.mu.Unlock()
					l.lost<-err
					return
				}
			}
		}
	}()
	return l
}
func (l *OwnerLease) Epoch() uint64 { if l==nil{return 0};return l.epoch }
func (l *OwnerLease) Lost() <-chan error { if l==nil{return nil};return l.lost }
func (l *OwnerLease) Err() error { if l==nil{return errors.New("nil Owner lease")};l.mu.Lock();defer l.mu.Unlock();return l.err }
func (l *OwnerLease) Close() error {
	if l==nil{return nil}
	l.once.Do(func(){close(l.stop)})
	<-l.done
	return l.lease.Close()
}

// UsesSanlockLocker reports whether this coordinator holds the exact same
// sanlock locker instance as the Pool runtime's Reader/WAL coordination.
func (c *OwnerCoordinator) UsesSanlockLocker(want *lock.SanlockLocker) bool {
	if c == nil || want == nil { return false }
	got, ok := c.cfg.Locker.(*lock.SanlockLocker)
	return ok && got == want
}
