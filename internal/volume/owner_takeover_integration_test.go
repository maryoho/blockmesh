package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
)

// This test models two hosts sharing the same SAN metadata LUN. It has no
// Kubernetes API, etcd, ServiceAccount or network control-plane dependency.
type integrationSanlockLocker struct {
	mu sync.Mutex
	resources map[string]chan struct{}
	active atomic.Int32
	maxActive atomic.Int32
}
func newIntegrationSanlockLocker()*integrationSanlockLocker{return &integrationSanlockLocker{resources:make(map[string]chan struct{})}}
func(l *integrationSanlockLocker)Distributed()bool{return true}
func(l *integrationSanlockLocker)resource(name string)chan struct{}{
	l.mu.Lock();defer l.mu.Unlock()
	ch:=l.resources[name];if ch==nil{ch=make(chan struct{},1);ch<-struct{}{};l.resources[name]=ch};return ch
}
func(l *integrationSanlockLocker)Acquire(ctx context.Context,name string)(lock.Lease,error){
	if name==""{return nil,errors.New("empty resource")}
	ch:=l.resource(name)
	select{case<-ctx.Done():return nil,ctx.Err();case<-ch:
		n:=l.active.Add(1);for old:=l.maxActive.Load();n>old&&!l.maxActive.CompareAndSwap(old,n);old=l.maxActive.Load(){}
		lease:=&integrationSanlockLease{resource:name,gate:ch,locker:l};lease.valid.Store(true);return lease,nil
	}
}
func(l *integrationSanlockLocker)Lock(ctx context.Context,name string)(func()error,error){lease,err:=l.Acquire(ctx,name);if err!=nil{return nil,err};return lease.Close,nil}
type integrationSanlockLease struct{resource string;gate chan struct{};locker *integrationSanlockLocker;once sync.Once;valid atomic.Bool}
func(l *integrationSanlockLease)Resource()string{return l.resource}
func(l *integrationSanlockLease)Valid()bool{return l.valid.Load()}
func(l *integrationSanlockLease)Close()error{l.once.Do(func(){l.valid.Store(false);l.locker.active.Add(-1);l.gate<-struct{}{}});return nil}

type integrationPathFencer struct{mu sync.Mutex;paths []string;failPath string;calls []OwnerRecord}
func(f *integrationPathFencer)FenceNode(_ context.Context,node,boot string)error{
	f.mu.Lock();defer f.mu.Unlock();f.calls=append(f.calls,OwnerRecord{NodeID:node,BootID:boot})
	for _,p:=range f.paths{if p==f.failPath{return fmt.Errorf("PREEMPT AND ABORT verification failed on %s",p)}}
	return nil
}
func(f *integrationPathFencer)snapshot()[]OwnerRecord{f.mu.Lock();defer f.mu.Unlock();return append([]OwnerRecord(nil),f.calls...)}
type integrationReservation struct{err error}
func(r *integrationReservation)PrepareOwnerReservation(context.Context,string,string)error{return nil}
func(r *integrationReservation)EstablishOwnerReservation(context.Context,string,string)error{return r.err}
type integrationDrain struct{mu sync.Mutex;calls []OwnerRecord;err error}
func(d *integrationDrain)Drain(_ context.Context,_ string,old OwnerRecord)error{d.mu.Lock();defer d.mu.Unlock();d.calls=append(d.calls,old);return d.err}
func(d *integrationDrain)snapshot()[]OwnerRecord{d.mu.Lock();defer d.mu.Unlock();return append([]OwnerRecord(nil),d.calls...)}

func newIntegrationCoordinator(t *testing.T,locker *integrationSanlockLocker,records OwnerRecordStore,node,boot string,fencer *integrationPathFencer,drain *integrationDrain,epoch uint64)*OwnerCoordinator{
	t.Helper()
	c,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{Locker:locker,ResourceFor:func(string)string{return "pool:vol-owner:vol-a"},Records:records,Fencer:fencer,Reservation:&integrationReservation{},ReadEpoch:func(context.Context,string)(uint64,error){return epoch,nil},DrainOldOwner:drain.Drain,LeasePollPeriod:time.Millisecond})
	if err!=nil{t.Fatalf("NewOwnerCoordinator(%s/%s): %v",node,boot,err)}
	return c
}
func newIntegrationSANStores(t *testing.T)(*SANOwnerRecordStore,*SANOwnerRecordStore){
	t.Helper();dev:=newOwnerRecordMemDevice()
	a,err:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192});if err!=nil{t.Fatal(err)}
	b,err:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192});if err!=nil{t.Fatal(err)}
	return a,b
}

func TestTwoNodeOwnerTakeoverSerializesAndFencesPreviousOwner(t *testing.T){
	recordsA,recordsB:=newIntegrationSANStores(t)
	locker:=newIntegrationSanlockLocker()
	fenceA:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	fenceB:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	drainA,drainB:=&integrationDrain{},&integrationDrain{}
	nodeA:=newIntegrationCoordinator(t,locker,recordsA,"node-a","boot-a",fenceA,drainA,1)
	nodeB:=newIntegrationCoordinator(t,locker,recordsB,"node-b","boot-b",fenceB,drainB,1)
	type result struct{node string;lease *OwnerLease;err error}
	results:=make(chan result,2);start:=make(chan struct{});var started sync.WaitGroup;started.Add(2)
	go func(){started.Done();<-start;l,e:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a");results<-result{"node-a",l,e}}()
	go func(){started.Done();<-start;l,e:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b");results<-result{"node-b",l,e}}()
	started.Wait();close(start);first:=<-results
	if first.err!=nil{t.Fatalf("first acquisition %s: %v",first.node,first.err)}
	if first.lease.Epoch()!=2||locker.active.Load()!=1{t.Fatalf("first epoch=%d active=%d",first.lease.Epoch(),locker.active.Load())}
	if err:=first.lease.Close();err!=nil{t.Fatal(err)}
	second:=<-results
	if second.err!=nil{t.Fatalf("second acquisition %s: %v",second.node,second.err)}
	defer second.lease.Close()
	if second.lease.Epoch()!=3||locker.maxActive.Load()!=1{t.Fatalf("second epoch=%d max concurrent Owners=%d",second.lease.Epoch(),locker.maxActive.Load())}
	record,exists,err:=recordsA.Load(context.Background(),"vol-a")
	if err!=nil||!exists||record.NodeID!=second.node||record.Epoch!=3{t.Fatalf("final SAN OwnerRecord=%+v exists=%v err=%v",record,exists,err)}
	fencer,drain:=fenceA,drainA;if second.node=="node-b"{fencer,drain=fenceB,drainB}
	calls:=fencer.snapshot();if len(calls)!=1||calls[0].NodeID!=first.node{t.Fatalf("fencing calls=%+v",calls)}
	drained:=drain.snapshot();if len(drained)!=1||drained[0].NodeID!=first.node{t.Fatalf("drain calls=%+v",drained)}
}

func TestTwoNodeTakeoverFailsClosedWhenOneMultipathPathCannotBeFenced(t *testing.T){
	recordsA,recordsB:=newIntegrationSANStores(t)
	old:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=recordsA.CompareAndSwap(context.Background(),"vol-a",nil,old);err!=nil{t.Fatal(err)}
	locker:=newIntegrationSanlockLocker();fencer:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"},failPath:"mpathb"};drain:=&integrationDrain{}
	coordinator:=newIntegrationCoordinator(t,locker,recordsB,"node-b","boot-b",fencer,drain,2)
	if _,err:=coordinator.AcquireOwner(context.Background(),"vol-a","node-b","boot-b");err==nil{t.Fatal("takeover succeeded despite failed path fencing")}
	got,exists,err:=recordsA.Load(context.Background(),"vol-a")
	if err!=nil||!exists||got!=old{t.Fatalf("OwnerRecord changed: %+v exists=%v err=%v",got,exists,err)}
	if len(drain.snapshot())!=0||locker.active.Load()!=0{t.Fatal("drain ran before complete fencing or lock leaked")}
}

func TestSANOwnerRecordAmbiguousFlushIsRecoveredByNextOwner(t *testing.T){
	recordsA,recordsB:=newIntegrationSANStores(t)
	// Model a write that reached the target but whose flush completion was
	// ambiguous to the caller: the next owner must inspect the durable slots.
	locker:=newIntegrationSanlockLocker()
	dev:=recordsA.dev.(*ownerRecordMemDevice)
	dev.syncErr=errors.New("injected flush response loss")
	nodeA:=newIntegrationCoordinator(t,locker,recordsA,"node-a","boot-a",&integrationPathFencer{},&integrationDrain{},1)
	if _,err:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a");err==nil{t.Fatal("expected ambiguous flush failure")}
	dev.syncErr=nil
	// The first slot may already contain the record despite the lost flush
	// acknowledgement. A subsequent node must fence that identity before RW.
	nodeB:=newIntegrationCoordinator(t,locker,recordsB,"node-b","boot-b",&integrationPathFencer{paths:[]string{"mpatha"}},&integrationDrain{},1)
	lease,err:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b")
	if err!=nil{t.Fatalf("next owner failed to recover SAN record: %v",err)}
	defer lease.Close()
	if lease.Epoch()!=3{t.Fatalf("recovered Owner epoch=%d want 3",lease.Epoch())}
}

var _ fence.Fencer=(*integrationPathFencer)(nil)
var _ lock.DistributedLocker=(*integrationSanlockLocker)(nil)
