package volume

import (
	"context"
	"errors"
	"sync"
	"testing"
	"path/filepath"

	"blockmesh/internal/runtimeevents"
	"time"

	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
)

type ownerTestLease struct { valid bool; closed bool; mu sync.Mutex }
func (l *ownerTestLease) Resource() string{return "owner"}
func (l *ownerTestLease) Valid()bool{l.mu.Lock();defer l.mu.Unlock();return l.valid}
func (l *ownerTestLease) Close()error{l.mu.Lock();defer l.mu.Unlock();l.closed=true;l.valid=false;return nil}
type ownerTestLocker struct{ lease *ownerTestLease }
func (l *ownerTestLocker) Distributed()bool{return true}
func (l *ownerTestLocker) Acquire(context.Context,string)(lock.Lease,error){return l.lease,nil}
func (l *ownerTestLocker) Lock(context.Context,string)(func()error,error){return nil,errors.New("unused")}
type ownerTestFencer struct{ calls []OwnerRecord; err error }
func (f *ownerTestFencer) FenceNode(_ context.Context,node,boot string)error{f.calls=append(f.calls,OwnerRecord{NodeID:node,BootID:boot});return f.err}

type ownerTestReservation struct { prepareCalls int; calls int; node string; boot string; err error; recordVisible bool; records *ownerTestRecords }
func (r *ownerTestReservation) PrepareOwnerReservation(context.Context,string,string) error { r.prepareCalls++; return nil }
func (r *ownerTestReservation) EstablishOwnerReservation(_ context.Context,node,boot string) error {
	r.calls++; r.node=node; r.boot=boot
	if r.records != nil { r.recordVisible = r.records.exists && r.records.record.NodeID == node && r.records.record.BootID == boot }
	return r.err
}

type ownerTestRecords struct{ record OwnerRecord; exists bool; writes int; err error }
func (s *ownerTestRecords) Load(context.Context,string)(OwnerRecord,bool,error){return s.record,s.exists,nil}
func (s *ownerTestRecords) CompareAndSwap(_ context.Context,_ string,expected *OwnerRecord,next OwnerRecord)error{
	if s.err!=nil{return s.err}
	if s.exists!=(expected!=nil) || (expected!=nil && s.record!=*expected){return ErrOwnerRecordConflict}
	s.record=next;s.exists=true;s.writes++;return nil
}
type ownerTestBarrier struct{ calls int; err error }
func (b *ownerTestBarrier) DrainOldOwner(context.Context,string,OwnerRecord)error{b.calls++;return b.err}

func newOwnerTestCoordinator(t *testing.T, record *OwnerRecord, descriptorEpoch uint64, fencer *ownerTestFencer, barrier *ownerTestBarrier)(*OwnerCoordinator,*ownerTestRecords,*ownerTestLease){
	t.Helper()
	lease:=&ownerTestLease{valid:true}
	records:=&ownerTestRecords{}
	if record!=nil{records.record=*record;records.exists=true}
	c,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:&ownerTestLocker{lease:lease},ResourceFor:func(string)string{return "pool:vol-owner:/dev/test:0"},
		Records:records,Fencer:fencer,Reservation:&ownerTestReservation{records:records},ReadEpoch:func(context.Context,string)(uint64,error){return descriptorEpoch,nil},
		DrainOldOwner:barrier.DrainOldOwner,LeasePollPeriod:time.Millisecond,
	})
	if err!=nil{t.Fatal(err)}
	return c,records,lease
}

func TestOwnerCoordinatorFirstOwnerUsesDescriptorEpoch(t *testing.T){
	c,records,lease:=newOwnerTestCoordinator(t,nil,1,&ownerTestFencer{},&ownerTestBarrier{})
	got,err:=c.AcquireOwner(context.Background(),"vol","node-b","boot-b")
	if err!=nil{t.Fatal(err)}
	if got.Epoch()!=2{t.Fatalf("epoch=%d want 2",got.Epoch())}
	if records.record.NodeID!="node-b"||records.record.BootID!="boot-b"||records.record.Epoch!=2{t.Fatalf("record=%+v",records.record)}
	if err=got.Close();err!=nil{t.Fatal(err)}
	if !lease.closed{t.Fatal("sanlock lease was not released")}
}

func TestOwnerCoordinatorFencesAndDrainsBeforeCAS(t *testing.T){
	old:=OwnerRecord{VolumeID:"vol",NodeID:"node-a",BootID:"boot-a",Epoch:11}
	fencer:=&ownerTestFencer{}
	barrier:=&ownerTestBarrier{}
	c,records,_:=newOwnerTestCoordinator(t,&old,9,fencer,barrier)
	got,err:=c.AcquireOwner(context.Background(),"vol","node-b","boot-b")
	if err!=nil{t.Fatal(err)}
	defer got.Close()
	if got.Epoch()!=12{t.Fatalf("epoch=%d want 12",got.Epoch())}
	if len(fencer.calls)!=1||fencer.calls[0].NodeID!="node-a"||fencer.calls[0].BootID!="boot-a"{t.Fatalf("fencing calls=%+v",fencer.calls)}
	if barrier.calls!=1{t.Fatalf("drain calls=%d",barrier.calls)}
	if records.writes!=1||records.record.NodeID!="node-b"{t.Fatalf("new owner record not published: %+v",records)}
}

func TestOwnerCoordinatorNeverPublishesAfterFenceOrDrainFailure(t *testing.T){
	old:=OwnerRecord{VolumeID:"vol",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	for _,tc:=range []struct{name string;fenceErr,drainErr error}{
		{name:"fence",fenceErr:errors.New("path unavailable")},
		{name:"drain",drainErr:errors.New("I/O outstanding")},
	}{
		t.Run(tc.name,func(t *testing.T){
			f:=&ownerTestFencer{err:tc.fenceErr}
			b:=&ownerTestBarrier{err:tc.drainErr}
			c,records,lease:=newOwnerTestCoordinator(t,&old,2,f,b)
			if _,err:=c.AcquireOwner(context.Background(),"vol","node-b","boot-b");err==nil{t.Fatal("takeover unexpectedly succeeded")}
			if records.writes!=0{t.Fatal("owner record published despite failed fencing/drain")}
			if !lease.closed{t.Fatal("failed takeover leaked sanlock lease")}
		})
	}
}

func TestOwnerCoordinatorRejectsSameBootIdentityAndEpochOverflow(t *testing.T){
	old:=OwnerRecord{VolumeID:"vol",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	c,records,lease:=newOwnerTestCoordinator(t,&old,2,&ownerTestFencer{},&ownerTestBarrier{})
	if _,err:=c.AcquireOwner(context.Background(),"vol","node-a","boot-a");!errors.Is(err,ErrOwnerIdentityReuse){t.Fatalf("same identity error=%v",err)}
	if records.writes!=0||!lease.closed{t.Fatal("same-identity takeover changed record or leaked lease")}
	c,records,lease=newOwnerTestCoordinator(t,&old,^uint64(0),&ownerTestFencer{},&ownerTestBarrier{})
	if _,err:=c.AcquireOwner(context.Background(),"vol","node-b","boot-b");!errors.Is(err,ErrOwnerEpochExhausted){t.Fatalf("epoch overflow error=%v",err)}
	if records.writes!=0||!lease.closed{t.Fatal("epoch overflow changed record or leaked lease")}
}

func TestOwnerLeaseReportsSanlockLoss(t *testing.T){
	underlying:=&ownerTestLease{valid:true}
	lease:=newOwnerLease(underlying,OwnerRecord{VolumeID:"vol",NodeID:"node",BootID:"boot",Epoch:3},time.Millisecond)
	underlying.mu.Lock();underlying.valid=false;underlying.mu.Unlock()
	select{
	case err:=<-lease.Lost():
		if err==nil{t.Fatal("nil lease-loss error")}
	case <-time.After(time.Second):t.Fatal("lease loss was not reported")
	}
	if lease.Err()==nil{t.Fatal("lease loss not retained")}
	_ = lease.Close()
}

var _ fence.Fencer=(*ownerTestFencer)(nil)
var _ lock.DistributedLocker=(*ownerTestLocker)(nil)

func TestOwnerCoordinatorRejectsMissingHistoryAfterFirstOwnerEpoch(t *testing.T) {
	c,records,lease:=newOwnerTestCoordinator(t,nil,4,&ownerTestFencer{},&ownerTestBarrier{})
	if _,err:=c.AcquireOwner(context.Background(),"vol","node-b","boot-b");err==nil{
		t.Fatal("missing previous Owner identity accepted for an already-used Volume")
	}
	if records.writes!=0||!lease.closed{t.Fatal("missing-history takeover changed record or leaked lease")}
}

func TestOwnerLeaseCloseIsIdempotent(t *testing.T) {
	underlying:=&ownerTestLease{valid:true}
	lease:=newOwnerLease(underlying,OwnerRecord{VolumeID:"vol",NodeID:"node",BootID:"boot",Epoch:2},time.Millisecond)
	if err:=lease.Close();err!=nil{t.Fatal(err)}
	if err:=lease.Close();err!=nil{t.Fatal(err)}
	if !underlying.closed{t.Fatal("underlying lease was not closed")}
}


func TestOwnerCoordinatorRequiresReservationManager(t *testing.T) {
	lease:=&ownerTestLease{valid:true}
	_,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:&ownerTestLocker{lease:lease},ResourceFor:func(string)string{return "owner"},
		Records:&ownerTestRecords{},Fencer:&ownerTestFencer{},ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:(&ownerTestBarrier{}).DrainOldOwner,
	})
	if err==nil { t.Fatal("coordinator accepted missing Owner reservation manager") }
}

func TestOwnerCoordinatorPublishesIdentityBeforeReservationAndFailsClosed(t *testing.T) {
	lease:=&ownerTestLease{valid:true}
	records:=&ownerTestRecords{}
	reservation:=&ownerTestReservation{records:records,err:errors.New("reservation verification mismatch")}
	coordinator,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:&ownerTestLocker{lease:lease},ResourceFor:func(string)string{return "owner"},
		Records:records,Fencer:&ownerTestFencer{},Reservation:reservation,
		ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:(&ownerTestBarrier{}).DrainOldOwner,LeasePollPeriod:time.Millisecond,
	})
	if err!=nil { t.Fatal(err) }
	if _,err=coordinator.AcquireOwner(context.Background(),"vol","node-b","boot-b");err==nil { t.Fatal("Owner acquisition succeeded without verified reservation") }
	if records.writes!=1 || records.record.NodeID!="node-b" || records.record.BootID!="boot-b" {
		t.Fatalf("durable recovery identity was not retained: %+v writes=%d",records.record,records.writes)
	}
	if reservation.prepareCalls!=1 || reservation.calls!=1 || !reservation.recordVisible { t.Fatalf("reservation was not attempted after durable OwnerRecord: %+v",reservation) }
	if !lease.closed { t.Fatal("failed reservation leaked sanlock Owner lease") }
}


func TestOwnerLeaseCarriesReservationAndOwnerRecordProof(t *testing.T) {
	underlying:=&ownerTestLease{valid:true}
	record:=OwnerRecord{VolumeID:"vol",NodeID:"node-b",BootID:"boot-b",Epoch:7}
	lease:=newOwnerLease(underlying,record,time.Millisecond)
	if !lease.ReservationVerified() || !lease.PreviousOwnerFenced() || !lease.OldOwnerIODrained() { t.Fatal("Owner lease lacks complete takeover proof") }
	if got:=lease.OwnerRecord();got!=record { t.Fatalf("OwnerRecord=%+v want %+v",got,record) }
	_ = lease.Close()
}

func TestOwnerCoordinatorWritesDurableRuntimeEvents(t *testing.T) {
	path:=filepath.Join(t.TempDir(),"owner-events.jsonl")
	journal,err:=runtimeevents.Open(path,"op-owner-1")
	if err!=nil{t.Fatal(err)}
	defer journal.Close()
	lease:=&ownerTestLease{valid:true}
	records:=&ownerTestRecords{}
	coordinator,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:&ownerTestLocker{lease:lease},ResourceFor:func(string)string{return "owner"},
		Records:records,Fencer:&ownerTestFencer{},Reservation:&ownerTestReservation{records:records},
		ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:(&ownerTestBarrier{}).DrainOldOwner,Events:journal,
	})
	if err!=nil{t.Fatal(err)}
	got,err:=coordinator.AcquireOwner(context.Background(),"vol","node-b","boot-b")
	if err!=nil{t.Fatal(err)}
	if err=got.Close();err!=nil{t.Fatal(err)}
	recordsOut,err:=runtimeevents.Read(path)
	if err!=nil{t.Fatal(err)}
	want:=[]string{"sanlock.owner_lease_acquired","scsi_pr.registration_prepared","owner_record.durable","scsi_pr.owner_reservation_verified"}
	if len(recordsOut)!=len(want){t.Fatalf("events=%d want=%d",len(recordsOut),len(want))}
	for i,name:=range want{if recordsOut[i].Event.EventType!=name{t.Fatalf("event[%d]=%s want %s",i,recordsOut[i].Event.EventType,name)}}
}
