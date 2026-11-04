package volume

import (
	"context"
	"errors"
	"sync"
	"testing"
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
		Records:records,Fencer:fencer,ReadEpoch:func(context.Context,string)(uint64,error){return descriptorEpoch,nil},
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
	lease:=newOwnerLease(underlying,3,time.Millisecond)
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
	lease:=newOwnerLease(underlying,2,time.Millisecond)
	if err:=lease.Close();err!=nil{t.Fatal(err)}
	if err:=lease.Close();err!=nil{t.Fatal(err)}
	if !underlying.closed{t.Fatal("underlying lease was not closed")}
}
