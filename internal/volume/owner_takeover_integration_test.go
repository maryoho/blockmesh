package volume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
)

// integrationSanlockLocker models one shared sanlock resource visible to two
// independent node processes. It intentionally exposes only the distributed
// locker contract; it is not a production lock implementation.
type integrationSanlockLocker struct {
	mu sync.Mutex
	resources map[string]chan struct{}
	active atomic.Int32
	maxActive atomic.Int32
}

func newIntegrationSanlockLocker() *integrationSanlockLocker {
	return &integrationSanlockLocker{resources:make(map[string]chan struct{})}
}
func (l *integrationSanlockLocker) Distributed() bool { return true }
func (l *integrationSanlockLocker) resource(name string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch:=l.resources[name]
	if ch==nil { ch=make(chan struct{},1);ch<-struct{}{};l.resources[name]=ch }
	return ch
}
func (l *integrationSanlockLocker) Acquire(ctx context.Context,name string)(lock.Lease,error) {
	if name=="" { return nil,errors.New("empty resource") }
	ch:=l.resource(name)
	select {
	case <-ctx.Done(): return nil,ctx.Err()
	case <-ch:
		n:=l.active.Add(1)
		for old:=l.maxActive.Load();n>old&&!l.maxActive.CompareAndSwap(old,n);old=l.maxActive.Load() {}
		lease:=&integrationSanlockLease{resource:name,gate:ch,locker:l}
		lease.valid.Store(true)
		return lease,nil
	}
}
func (l *integrationSanlockLocker) Lock(ctx context.Context,name string)(func()error,error) {
	lease,err:=l.Acquire(ctx,name);if err!=nil{return nil,err};return lease.Close,nil
}
type integrationSanlockLease struct {
	resource string
	gate chan struct{}
	locker *integrationSanlockLocker
	once sync.Once
	valid atomic.Bool
}
func (l *integrationSanlockLease) Resource()string{return l.resource}
func (l *integrationSanlockLease) Valid()bool{return l.valid.Load()}
func (l *integrationSanlockLease) Close()error{
	l.once.Do(func(){l.valid.Store(false);l.locker.active.Add(-1);l.gate<-struct{}{}})
	return nil
}

type integrationPathFencer struct {
	mu sync.Mutex
	paths []string
	failPath string
	calls []OwnerRecord
}
func (f *integrationPathFencer) FenceNode(_ context.Context,node,boot string)error {
	f.mu.Lock();defer f.mu.Unlock()
	f.calls=append(f.calls,OwnerRecord{NodeID:node,BootID:boot})
	for _,path:=range f.paths {
		if path==f.failPath { return fmt.Errorf("PREEMPT AND ABORT verification failed on %s",path) }
	}
	return nil
}
func (f *integrationPathFencer) snapshot()[]OwnerRecord{f.mu.Lock();defer f.mu.Unlock();return append([]OwnerRecord(nil),f.calls...)}

type integrationDrain struct {
	mu sync.Mutex
	calls []OwnerRecord
	err error
}
func (d *integrationDrain) Drain(_ context.Context,_ string,old OwnerRecord)error{
	d.mu.Lock();defer d.mu.Unlock();d.calls=append(d.calls,old);return d.err
}
func (d *integrationDrain) snapshot()[]OwnerRecord{d.mu.Lock();defer d.mu.Unlock();return append([]OwnerRecord(nil),d.calls...)}

func newIntegrationCoordinator(t *testing.T,locker *integrationSanlockLocker,records OwnerRecordStore,
	node,boot string,fencer *integrationPathFencer,drain *integrationDrain,epoch uint64)*OwnerCoordinator{
	t.Helper()
	c,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:locker,ResourceFor:func(string)string{return "pool:vol-owner:vol-a"},
		Records:records,Fencer:fencer,ReadEpoch:func(context.Context,string)(uint64,error){return epoch,nil},
		DrainOldOwner:drain.Drain,LeasePollPeriod:time.Millisecond,
	})
	if err!=nil{t.Fatalf("NewOwnerCoordinator(%s/%s): %v",node,boot,err)}
	return c
}

func newIntegrationKubernetesStore(t *testing.T,handler http.Handler,timeout time.Duration)(*KubernetesOwnerRecordStore,*httptest.Server){
	t.Helper()
	server:=httptest.NewServer(handler)
	store,err:=NewKubernetesOwnerRecordStore(KubernetesOwnerRecordStoreConfig{
		BaseURL:server.URL,Namespace:"blockmesh-system",BearerToken:"test-token",
		HTTPClient:&http.Client{Transport:server.Client().Transport,Timeout:timeout},
	})
	if err!=nil{server.Close();t.Fatal(err)}
	return store,server
}

// TestTwoNodeOwnerTakeoverSerializesAndFencesPreviousOwner exercises two
// independent coordinators, one shared distributed-lock service, and one
// Kubernetes Lease API. It verifies there is never more than one sanlock
// Owner lease and that the second node fences/drains the first identity.
func TestTwoNodeOwnerTakeoverSerializesAndFencesPreviousOwner(t *testing.T){
	api:=&ownerLeaseAPIServer{}
	records,server:=newIntegrationKubernetesStore(t,api,time.Second);defer server.Close()
	locker:=newIntegrationSanlockLocker()
	fenceA:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	fenceB:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	drainA,drainB:=&integrationDrain{},&integrationDrain{}
	nodeA:=newIntegrationCoordinator(t,locker,records,"node-a","boot-a",fenceA,drainA,1)
	nodeB:=newIntegrationCoordinator(t,locker,records,"node-b","boot-b",fenceB,drainB,1)

	type result struct{node string;lease *OwnerLease;err error}
	results:=make(chan result,2)
	start:=make(chan struct{})
	var started sync.WaitGroup
	started.Add(2)
	go func(){started.Done();<-start;l,e:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a");results<-result{"node-a",l,e}}()
	go func(){started.Done();<-start;l,e:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b");results<-result{"node-b",l,e}}()
	started.Wait();close(start)
	first:=<-results
	if first.err!=nil{t.Fatalf("first Owner acquisition (%s): %v",first.node,first.err)}
	if first.lease.Epoch()!=2{t.Fatalf("first Owner epoch=%d want 2",first.lease.Epoch())}
	if locker.active.Load()!=1{t.Fatalf("active sanlock Owner leases=%d want 1",locker.active.Load())}

	// The first node is now considered crashed: closing the test lease models
	// sanlock helper/process teardown and makes the shared resource available.
	if err:=first.lease.Close();err!=nil{t.Fatal(err)}
	second:=<-results
	if second.err!=nil{t.Fatalf("second Owner acquisition (%s): %v",second.node,second.err)}
	if second.lease.Epoch()!=3{t.Fatalf("second Owner epoch=%d want 3",second.lease.Epoch())}
	if locker.maxActive.Load()!=1{t.Fatalf("observed %d simultaneous sanlock Owners",locker.maxActive.Load())}
	if locker.active.Load()!=1{t.Fatalf("active sanlock Owner leases=%d want 1",locker.active.Load())}
	record,exists,err:=records.Load(context.Background(),"vol-a")
	if err!=nil||!exists{t.Fatalf("load final OwnerRecord: exists=%v err=%v",exists,err)}
	if record.NodeID!=second.node||record.Epoch!=3{t.Fatalf("final OwnerRecord=%+v",record)}
	var fencer *integrationPathFencer
	var drain *integrationDrain
	if second.node=="node-a"{fencer,drain=fenceA,drainA}else{fencer,drain=fenceB,drainB}
	calls:=fencer.snapshot()
	if len(calls)!=1||calls[0].NodeID!=first.node{t.Fatalf("second node fenced %+v, want previous Owner %s",calls,first.node)}
	drained:=drain.snapshot()
	if len(drained)!=1||drained[0].NodeID!=first.node{t.Fatalf("second node drained %+v, want previous Owner %s",drained,first.node)}
	if err:=second.lease.Close();err!=nil{t.Fatal(err)}
	if locker.active.Load()!=0{t.Fatalf("sanlock Owner lease leaked: %d",locker.active.Load())}
}

func TestTwoNodeTakeoverFailsClosedWhenOneMultipathPathCannotBeFenced(t *testing.T){
	api:=&ownerLeaseAPIServer{}
	records,server:=newIntegrationKubernetesStore(t,api,time.Second);defer server.Close()
	old:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=records.CompareAndSwap(context.Background(),"vol-a",nil,old);err!=nil{t.Fatal(err)}
	locker:=newIntegrationSanlockLocker()
	fencer:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"},failPath:"mpathb"}
	drain:=&integrationDrain{}
	coordinator:=newIntegrationCoordinator(t,locker,records,"node-b","boot-b",fencer,drain,2)
	if _,err:=coordinator.AcquireOwner(context.Background(),"vol-a","node-b","boot-b");err==nil{
		t.Fatal("takeover succeeded despite failed fencing on mpathb")
	}
	got,exists,err:=records.Load(context.Background(),"vol-a")
	if err!=nil||!exists||got!=old{t.Fatalf("OwnerRecord changed after partial fencing: %+v exists=%v err=%v",got,exists,err)}
	if len(drain.snapshot())!=0{t.Fatal("old Owner I/O drain ran before all-path fencing succeeded")}
	if locker.active.Load()!=0{t.Fatal("failed fencing leaked sanlock Owner lease")}
}

func TestOwnerTakeoverFailsClosedWhenKubernetesAPIUnavailable(t *testing.T){
	records,server:=newIntegrationKubernetesStore(t,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		http.Error(w,"API unavailable",http.StatusServiceUnavailable)
	}),time.Second)
	defer server.Close()
	locker:=newIntegrationSanlockLocker()
	coordinator:=newIntegrationCoordinator(t,locker,records,"node-a","boot-a",&integrationPathFencer{},&integrationDrain{},1)
	if _,err:=coordinator.AcquireOwner(context.Background(),"vol-a","node-a","boot-a");err==nil{
		t.Fatal("Owner acquisition succeeded while Kubernetes API was unavailable")
	}
	if locker.active.Load()!=0{t.Fatal("Kubernetes API failure leaked sanlock Owner lease")}
}

func TestAmbiguousKubernetesCreateIsFencedByNextOwner(t *testing.T){
	var mu sync.Mutex
	var object map[string]any
	var delayCreate atomic.Bool
	handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		mu.Lock()
		defer mu.Unlock()
		if r.Method==http.MethodGet{
			if object==nil{http.Error(w,"not found",404);return}
			_ = json.NewEncoder(w).Encode(object);return
		}
		if r.Method==http.MethodPost{
			if object!=nil{http.Error(w,"already exists",409);return}
			if err:=json.NewDecoder(r.Body).Decode(&object);err!=nil{http.Error(w,err.Error(),400);return}
			nestedMap(object,"metadata")["resourceVersion"]="1"
			if delayCreate.Load(){time.Sleep(80*time.Millisecond)}
			w.WriteHeader(http.StatusCreated);_ = json.NewEncoder(w).Encode(object);return
		}
		http.Error(w,"method not allowed",405)
	})
	records,server:=newIntegrationKubernetesStore(t,handler,15*time.Millisecond)
	defer server.Close()
	locker:=newIntegrationSanlockLocker()
	fenceA:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	fenceB:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	drainA,drainB:=&integrationDrain{},&integrationDrain{}
	nodeA:=newIntegrationCoordinator(t,locker,records,"node-a","boot-a",fenceA,drainA,1)
	nodeB:=newIntegrationCoordinator(t,locker,records,"node-b","boot-b",fenceB,drainB,1)

	delayCreate.Store(true)
	if _,err:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a");err==nil{
		t.Fatal("expected client timeout after server committed initial OwnerRecord")
	}
	if locker.active.Load()!=0{t.Fatal("ambiguous create leaked sanlock Owner lease")}
	delayCreate.Store(false)
	record,exists,err:=records.Load(context.Background(),"vol-a")
	if err!=nil||!exists{t.Fatalf("server-side committed record not visible: exists=%v err=%v",exists,err)}
	if record.NodeID!="node-a"||record.BootID!="boot-a"||record.Epoch!=2{t.Fatalf("ambiguous committed record=%+v",record)}

	lease,err:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b")
	if err!=nil{t.Fatalf("next Owner did not recover ambiguous commit: %v",err)}
	defer lease.Close()
	if lease.Epoch()!=3{t.Fatalf("next Owner epoch=%d want 3",lease.Epoch())}
	calls:=fenceB.snapshot()
	if len(calls)!=1||calls[0].NodeID!="node-a"||calls[0].BootID!="boot-a"{
		t.Fatalf("next Owner did not fence durably recorded identity: %+v",calls)
	}
}

var _ fence.Fencer=(*integrationPathFencer)(nil)
var _ lock.DistributedLocker=(*integrationSanlockLocker)(nil)
