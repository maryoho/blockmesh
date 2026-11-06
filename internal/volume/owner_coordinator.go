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
	"blockmesh/internal/runtimeevents"
)

var (
	ErrOwnerRecordConflict = errors.New("authoritative owner record changed concurrently")
	ErrOwnerIdentityReuse = errors.New("same node/boot owner identity cannot be safely fenced as a previous owner")
	ErrOwnerEpochExhausted = errors.New("volume owner epoch exhausted")
)

// OwnerRecord is the authoritative durable identity of the last Owner. It is
// stored on the shared SAN metadata device; the Pool Volume Descriptor
// separately stores the epoch used by metadata readers.
type OwnerRecord struct {
	VolumeID string
	NodeID   string
	BootID   string
	Epoch    uint64
}

// OwnerRecordStore must provide durable reads and CAS updates. Production
// implementations must serialize CompareAndSwap across nodes with the
// exclusive sanlock Owner resource. A nil expected value means no record exists.
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
	Reservation     fence.OwnerReservation
	ReadEpoch       OwnerEpochReader
	DrainOldOwner   OwnerIOBarrier
	LeasePollPeriod time.Duration
	Events runtimeevents.Sink
}

type OwnerCoordinator struct { cfg OwnerCoordinatorConfig }

func NewOwnerCoordinator(cfg OwnerCoordinatorConfig) (*OwnerCoordinator, error) {
	if cfg.Locker == nil || !cfg.Locker.Distributed() || cfg.ResourceFor == nil ||
		cfg.Records == nil || cfg.Fencer == nil || cfg.Reservation == nil || cfg.ReadEpoch == nil || cfg.DrainOldOwner == nil {
		return nil, errors.New("owner coordinator requires distributed lock, authoritative record CAS, SAN fencer, Owner reservation manager, epoch reader and old-owner I/O barrier")
	}
	switch cfg.Fencer.(type) {
	case fence.NoopFencer, *fence.NoopFencer:
		return nil, errors.New("NoopFencer is forbidden for Owner takeover")
	}
	if cfg.LeasePollPeriod <= 0 { cfg.LeasePollPeriod = 100 * time.Millisecond }
	return &OwnerCoordinator{cfg:cfg}, nil
}

// AcquireOwner performs the takeover closed loop while holding the exclusive
// sanlock resource: load the previous identity -> prepare this initiator's
// path-scoped PR keys -> PREEMPT AND ABORT and verify the previous Owner on all
// paths -> drain old-owner metadata I/O -> compute the next epoch -> CAS-publish
// the new identity -> issue PR OUT RESERVE and verify its holder/type on every
// path. OwnerRecord is published before RESERVE so an ambiguous reservation
// outcome always has a durable identity for the next Owner to fence. Key
// registration happens before PREEMPT because the new initiator's key is
// needed to issue it; registration alone does not grant write access. WAL
// replay and BMDVOL02 OwnerEpoch publication must still complete before data
// requests are enabled.
func (c *OwnerCoordinator) AcquireOwner(ctx context.Context, volumeID, nodeID, bootID string) (result *OwnerLease, retErr error) {
	if c == nil || ctx == nil { return nil, errors.New("owner coordinator and context are required") }
	if strings.TrimSpace(volumeID)=="" || strings.TrimSpace(nodeID)=="" || strings.TrimSpace(bootID)=="" {
		return nil, errors.New("volume, node and boot identities are required")
	}
	resource:=c.cfg.ResourceFor(volumeID)
	if strings.TrimSpace(resource)=="" { return nil, errors.New("owner sanlock resource is empty") }
	lease,err:=c.cfg.Locker.Acquire(ctx,resource)
	if err!=nil { return nil,fmt.Errorf("acquire exclusive Owner sanlock resource: %w",err) }
	if lease==nil { return nil,errors.New("distributed locker returned a nil Owner lease") }
	if err=c.recordEvent(volumeID,nodeID,bootID,0,"sanlock.owner_lease_acquired","acquire","success","sanlock-helper-ready","exclusive sanlock Owner resource acquired");err!=nil{_ = lease.Close();return nil,err}
	releaseOnError:=true
	defer func(){
		if releaseOnError {
			if closeErr:=lease.Close();closeErr!=nil {
				retErr=errors.Join(retErr,fmt.Errorf("release Owner sanlock lease after failed takeover: %w",closeErr))
			}
		}
	}()
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
	}
	// The new initiator's path-scoped keys must be registered before it can
	// issue PREEMPT AND ABORT using those keys against the previous Owner.
	if err = c.cfg.Reservation.PrepareOwnerReservation(ctx, nodeID, bootID); err != nil {
		_ = c.recordEvent(volumeID,nodeID,bootID,0,"scsi_pr.registration_prepared","prepare","failure","reservation-manager-error",err.Error())
		return nil, fmt.Errorf("prepare new Owner SCSI PR registrations: %w", err)
	}
	if err=c.recordEvent(volumeID,nodeID,bootID,0,"scsi_pr.registration_prepared","prepare","success","reservation-manager-returned","new initiator path registrations prepared");err!=nil{return nil,err}
	if exists {
		if err=c.cfg.Fencer.FenceNode(ctx,old.NodeID,old.BootID);err!=nil {
			_ = c.recordEvent(volumeID,nodeID,bootID,old.Epoch,"scsi_pr.preempt_and_abort","fence_old_owner","failure","fencer-error",err.Error())
			return nil,fmt.Errorf("PREEMPT AND ABORT previous Owner %s/%s on all SAN paths: %w",old.NodeID,old.BootID,err)
		}
		if err=c.recordEvent(volumeID,nodeID,bootID,old.Epoch,"scsi_pr.preempt_and_abort","fence_old_owner","success","fencer-contract-completed","Fencer returned success for previous Owner identity");err!=nil{return nil,err}
		if err=c.cfg.DrainOldOwner(ctx,volumeID,old);err!=nil {
			_ = c.recordEvent(volumeID,nodeID,bootID,old.Epoch,"old_owner.metadata_io_drain","drain_old_owner","failure","io-barrier-error",err.Error())
			return nil,fmt.Errorf("old Owner metadata I/O is not drained: %w",err)
		}
		if err=c.recordEvent(volumeID,nodeID,bootID,old.Epoch,"old_owner.metadata_io_drain","drain_old_owner","success","io-barrier-returned","old Owner metadata I/O barrier returned success");err!=nil{return nil,err}
	}
	if !lease.Valid() { return nil,errors.New("Owner sanlock lease lost during fencing") }
	descriptorEpoch,err:=c.cfg.ReadEpoch(ctx,volumeID)
	if err!=nil { return nil,fmt.Errorf("read durable Volume OwnerEpoch: %w",err) }
	if descriptorEpoch==0 { return nil,errors.New("durable Volume OwnerEpoch is zero") }
	if !exists && descriptorEpoch>1 {
		return nil,errors.New("previous Owner identity is missing while Volume OwnerEpoch indicates prior RW ownership")
	}
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
		_ = c.recordEvent(volumeID,nodeID,bootID,next.Epoch,"owner_record.publish","publish_owner_record","failure","store-cas-error",err.Error())
		if errors.Is(err,ErrOwnerRecordConflict) { return nil,err }
		return nil,fmt.Errorf("durably publish new authoritative Owner identity: %w",err)
	}
	if err=c.recordEvent(volumeID,nodeID,bootID,next.Epoch,"owner_record.durable","publish_owner_record","success","store-cas-returned","OwnerRecord CompareAndSwap returned after its durability contract");err!=nil{return nil,err}
	if !lease.Valid() {
		// Record is intentionally left as a tombstone-like last owner identity;
		// the next acquirer must fence it before publishing another epoch.
		return nil,errors.New("Owner sanlock lease lost after publishing takeover record")
	}
	if err = c.cfg.Reservation.EstablishOwnerReservation(ctx, nodeID, bootID); err != nil {
		_ = c.recordEvent(volumeID,nodeID,bootID,next.Epoch,"scsi_pr.owner_reservation_verified","reserve_owner","failure","reservation-manager-error",err.Error())
		// Keep the OwnerRecord: PR REGISTER/RESERVE may have committed despite
		// an ambiguous command result. The next Owner must fence this identity.
		return nil, fmt.Errorf("establish and verify new Owner SCSI PR reservation: %w", err)
	}
	if err=c.recordEvent(volumeID,nodeID,bootID,next.Epoch,"scsi_pr.owner_reservation_verified","reserve_owner","success","reservation-manager-verification-returned","Owner reservation manager returned after its all-path verification contract");err!=nil{return nil,err}
	if !lease.Valid() {
		return nil, errors.New("Owner sanlock lease lost while establishing SCSI PR reservation")
	}
	out:=newOwnerLease(lease,next,c.cfg.LeasePollPeriod)
	releaseOnError=false
	return out,nil
}

 // ProveVolumeFenced is the destructive-operation proof used before a
 // BMPOOL03 Reserved Arena can be reclaimed. It serializes with normal Owner
 // acquisition, fences the last durable initiator on every configured SAN
 // path, and waits for the old-owner I/O barrier before returning success.
 func(c *OwnerCoordinator) ProveVolumeFenced(ctx context.Context,volumeID string,ownerEpoch uint64)error{
	if c==nil||ctx==nil||strings.TrimSpace(volumeID)==""||ownerEpoch==0{return errors.New("OwnerCoordinator, context, Volume ID and OwnerEpoch are required")}
	resource:=c.cfg.ResourceFor(volumeID);if strings.TrimSpace(resource)==""{return errors.New("Owner sanlock resource is empty")}
	lease,err:=c.cfg.Locker.Acquire(ctx,resource);if err!=nil{return fmt.Errorf("acquire exclusive Owner resource for Volume fencing: %w",err)}
	if lease==nil{return errors.New("distributed locker returned nil Owner lease")}
	record,exists,err:=c.cfg.Records.Load(ctx,volumeID)
	if err!=nil{return errors.Join(err,lease.Close())}
	if !exists{
		epoch,readErr:=c.cfg.ReadEpoch(ctx,volumeID)
		if readErr!=nil{return errors.Join(readErr,lease.Close())}
		if epoch>1||epoch!=ownerEpoch{return errors.Join(errors.New("cannot prove Volume fenced: OwnerRecord is absent while descriptor indicates prior ownership"),lease.Close())}
		if !lease.Valid(){return errors.Join(errors.New("Owner sanlock lease lost during empty-owner fencing proof"),lease.Close())}
		return lease.Close()
	}
	if record.VolumeID!=volumeID||record.Epoch!=ownerEpoch||record.NodeID==""||record.BootID==""{
		return errors.Join(errors.New("durable OwnerRecord does not match requested Volume/OwnerEpoch"),lease.Close())
	}
	if !lease.Valid(){return errors.Join(errors.New("Owner sanlock lease lost before PR fencing"),lease.Close())}
	if err=c.cfg.Fencer.FenceNode(ctx,record.NodeID,record.BootID);err!=nil{
		_ = c.recordEvent(volumeID,"reclaimer","reclaimer",ownerEpoch,"scsi_pr.preempt_and_abort","fence_volume_owner","failure","fencer-error",err.Error())
		return errors.Join(fmt.Errorf("fence last Volume Owner on every SAN path: %w",err),lease.Close())
	}
	if err=c.cfg.DrainOldOwner(ctx,volumeID,record);err!=nil{
		_ = c.recordEvent(volumeID,"reclaimer","reclaimer",ownerEpoch,"old_owner.metadata_io_drain","drain_volume_owner","failure","io-barrier-error",err.Error())
		return errors.Join(fmt.Errorf("old Volume Owner I/O is not drained: %w",err),lease.Close())
	}
	if !lease.Valid(){return errors.Join(errors.New("Owner sanlock lease lost after PR fencing and I/O drain"),lease.Close())}
	if err=c.recordEvent(volumeID,"reclaimer","reclaimer",ownerEpoch,"volume.fencing.proven","prove_volume_fenced","success","all-path-fencer-and-io-barrier-returned","last durable Owner was fenced and its metadata I/O drained");err!=nil{
		return errors.Join(err,lease.Close())
	}
	return lease.Close()
}

func (c *OwnerCoordinator) recordEvent(volumeID,nodeID,bootID string,epoch uint64,eventType,phase,result,proof,detail string) error {
	if c.cfg.Events==nil{return nil}
	return c.cfg.Events.Record(runtimeevents.Event{VolumeID:volumeID,NodeID:nodeID,BootID:bootID,OwnerEpoch:epoch,EventType:eventType,Phase:phase,Result:result,Proof:proof,Detail:detail})
}

// OwnerLease monitors the sanlock helper process. A helper exit means the
// distributed lease can no longer be assumed held; callers must fail-stop the
// data path immediately. It never silently reacquires a lost lease.
type OwnerLease struct {
	lease lock.Lease
	epoch uint64
	record OwnerRecord
	reservationVerified bool
	previousOwnerFenced bool
	oldOwnerIODrained bool
	lost chan error
	stop chan struct{}
	done chan struct{}
	once sync.Once
	closeErr error
	mu sync.Mutex
	err error
}

func newOwnerLease(lease lock.Lease, record OwnerRecord, poll time.Duration) *OwnerLease {
	l:=&OwnerLease{lease:lease,epoch:record.Epoch,record:record,reservationVerified:true,previousOwnerFenced:true,oldOwnerIODrained:true,lost:make(chan error,1),stop:make(chan struct{}),done:make(chan struct{})}
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
func (l *OwnerLease) VolumeID() string { if l==nil{return ""};return l.record.VolumeID }
func (l *OwnerLease) Resource() string { if l==nil||l.lease==nil{return ""};return l.lease.Resource() }
// Valid reports the current sanlock helper lease state synchronously.
func (l *OwnerLease) Valid() bool { return l!=nil && l.lease!=nil && l.lease.Valid() }
// OwnerRecord returns the durable identity published before PR RESERVE.
func (l *OwnerLease) OwnerRecord() OwnerRecord { if l==nil{return OwnerRecord{}};return l.record }
// ReservationVerified is true only after holder/type were verified on every configured SAN path.
func (l *OwnerLease) ReservationVerified() bool { return l!=nil && l.reservationVerified }
// PreviousOwnerFenced is true after the prior identity was fenced on all paths, or no prior Owner existed.
func (l *OwnerLease) PreviousOwnerFenced() bool { return l!=nil && l.previousOwnerFenced }
// OldOwnerIODrained is true after the old Owner I/O barrier completed, or no prior Owner existed.
func (l *OwnerLease) OldOwnerIODrained() bool { return l!=nil && l.oldOwnerIODrained }
func (l *OwnerLease) Lost() <-chan error { if l==nil{return nil};return l.lost }
func (l *OwnerLease) Err() error { if l==nil{return errors.New("nil Owner lease")};l.mu.Lock();defer l.mu.Unlock();return l.err }
func (l *OwnerLease) Close() error {
	if l==nil{return nil}
	l.once.Do(func(){
		close(l.stop)
		<-l.done
		l.closeErr=l.lease.Close()
	})
	return l.closeErr
}

// UsesSanlockLocker reports whether this coordinator holds the exact same
// sanlock locker instance as the Pool runtime's Reader/WAL coordination.
func (c *OwnerCoordinator) UsesSanlockLocker(want *lock.SanlockLocker) bool {
	if c == nil || want == nil { return false }
	got, ok := c.cfg.Locker.(*lock.SanlockLocker)
	return ok && got == want
}
