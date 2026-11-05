package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"blockmesh/internal/lock"
)

type takeoverDMGate struct {
	mu sync.Mutex
	epoch uint64
	open bool
	fenced bool
}
func newTakeoverDMGate()*takeoverDMGate{return &takeoverDMGate{fenced:true}}
func(g *takeoverDMGate)Prepare(_ context.Context,_ string,epoch uint64)error{
	g.mu.Lock();defer g.mu.Unlock()
	if epoch==0||epoch<=g.epoch{return errors.New("stale prepare")}
	g.open=false;g.fenced=true;g.epoch=epoch;g.fenced=false
	return nil
}
func(g *takeoverDMGate)Enable(_ context.Context,_ string,epoch uint64)error{
	g.mu.Lock();defer g.mu.Unlock()
	if epoch==0||epoch!=g.epoch||g.fenced||g.open{return errors.New("invalid enable")}
	g.open=true;return nil
}
func(g *takeoverDMGate)Fence(_ context.Context,_ string,epoch uint64)error{
	g.mu.Lock();defer g.mu.Unlock()
	if epoch==0||epoch<g.epoch{return errors.New("stale fence")}
	g.open=false;g.fenced=true
	if epoch>g.epoch{g.epoch=epoch}
	return nil
}
func(g *takeoverDMGate)Drain(ctx context.Context,device string,epoch uint64)error{return g.Fence(ctx,device,epoch)}
func(g *takeoverDMGate)AdmitWrite()bool{g.mu.Lock();defer g.mu.Unlock();return g.open&&!g.fenced&&g.epoch!=0}
func(g *takeoverDMGate)snapshot()(uint64,bool,bool){g.mu.Lock();defer g.mu.Unlock();return g.epoch,g.open,g.fenced}

type takeoverDMPathFencer struct{
	paths []string
	failPath string
	oldGate *takeoverDMGate
	oldEpoch uint64
	mu sync.Mutex
	verified []string
}
func(f *takeoverDMPathFencer)FenceNode(ctx context.Context,node,boot string)error{
	f.mu.Lock()
	defer f.mu.Unlock()
	for _,path:=range f.paths{
		if path==f.failPath{return fmt.Errorf("injected PREEMPT AND ABORT verification failure on %s",path)}
		f.verified=append(f.verified,path)
	}
	if f.oldGate!=nil{
		if err:=f.oldGate.Fence(ctx,"dm-old",f.oldEpoch);err!=nil{return fmt.Errorf("fence old local DM gate: %w",err)}
	}
	return nil
}
func(f *takeoverDMPathFencer)pathsVerified()[]string{f.mu.Lock();defer f.mu.Unlock();return append([]string(nil),f.verified...)}

func newTakeoverCoordinator(t *testing.T,locker lock.DistributedLocker,records OwnerRecordStore,node,boot string,
	fencer interface{FenceNode(context.Context,string,string)error},drain OwnerIOBarrier)*OwnerCoordinator{
	t.Helper()
	c,err:=NewOwnerCoordinator(OwnerCoordinatorConfig{
		Locker:locker,ResourceFor:func(string)string{return "pool:vol-owner:vol-a"},
		Records:records,Fencer:fencer,Reservation:&integrationReservation{},
		ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:drain,LeasePollPeriod:1e9,
	})
	if err!=nil{t.Fatalf("new coordinator %s/%s: %v",node,boot,err)}
	return c
}

func TestTwoNodeTakeoverFencesOldDMGateBeforeNewEpochAdmission(t *testing.T){
	recordsA,recordsB:=newIntegrationSANStores(t)
	locker:=newIntegrationSanlockLocker()
	gateA,gateB:=newTakeoverDMGate(),newTakeoverDMGate()
	drainA,drainB:=&integrationDrain{},&integrationDrain{}
	fencerA:=&integrationPathFencer{paths:[]string{"mpatha","mpathb"}}
	nodeA:=newIntegrationCoordinator(t,locker,recordsA,"node-a","boot-a",fencerA,drainA,1)
	leaseA,err:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a")
	if err!=nil{t.Fatal(err)}
	if err=gateA.Prepare(context.Background(),"dm-old",leaseA.Epoch());err!=nil{t.Fatal(err)}
	if err=gateA.Enable(context.Background(),"dm-old",leaseA.Epoch());err!=nil{t.Fatal(err)}
	if !gateA.AdmitWrite(){t.Fatal("old Owner did not become writable")}
	// Model a crashed/stale old process: sanlock ownership is gone, but its
	// local kernel gate has not yet observed lease loss. Only SAN PR fencing
	// can make the takeover safe in this interval.
	if err=leaseA.Close();err!=nil{t.Fatal(err)}
	fencerB:=&takeoverDMPathFencer{paths:[]string{"mpatha","mpathb"},oldGate:gateA,oldEpoch:leaseA.Epoch()}
	nodeB:=newTakeoverCoordinator(t,locker,recordsB,"node-b","boot-b",fencerB,drainB.Drain)
	leaseB,err:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b")
	if err!=nil{t.Fatalf("new Owner acquisition failed: %v",err)}
	defer leaseB.Close()
	if leaseB.Epoch()!=3{t.Fatalf("new Owner epoch=%d want 3",leaseB.Epoch())}
	if err=gateB.Prepare(context.Background(),"dm-new",leaseB.Epoch());err!=nil{t.Fatal(err)}
	if err=gateB.Enable(context.Background(),"dm-new",leaseB.Epoch());err!=nil{t.Fatal(err)}
	if gateA.AdmitWrite(){t.Fatal("old node DM gate still admits writes after PR fencing")}
	if !gateB.AdmitWrite(){t.Fatal("new node DM gate did not admit writes after new epoch publication")}
	if locker.maxActive.Load()!=1{t.Fatalf("simultaneous sanlock Owners=%d",locker.maxActive.Load())}
	record,exists,err:=recordsA.Load(context.Background(),"vol-a")
	if err!=nil||!exists||record.NodeID!="node-b"||record.Epoch!=3{t.Fatalf("final SAN OwnerRecord=%+v exists=%v err=%v",record,exists,err)}
	if got:=fencerB.pathsVerified();len(got)!=2||got[0]!="mpatha"||got[1]!="mpathb"{t.Fatalf("verified SAN paths=%v",got)}
}

func TestTwoNodeTakeoverDoesNotEnableNewDMGateWhenAnyPRPathFails(t *testing.T){
	recordsA,recordsB:=newIntegrationSANStores(t)
	locker:=newIntegrationSanlockLocker()
	gateA,gateB:=newTakeoverDMGate(),newTakeoverDMGate()
	nodeA:=newIntegrationCoordinator(t,locker,recordsA,"node-a","boot-a",&integrationPathFencer{paths:[]string{"mpatha","mpathb"}},&integrationDrain{},1)
	leaseA,err:=nodeA.AcquireOwner(context.Background(),"vol-a","node-a","boot-a")
	if err!=nil{t.Fatal(err)}
	if err=gateA.Prepare(context.Background(),"dm-old",leaseA.Epoch());err!=nil{t.Fatal(err)}
	if err=gateA.Enable(context.Background(),"dm-old",leaseA.Epoch());err!=nil{t.Fatal(err)}
	if err=leaseA.Close();err!=nil{t.Fatal(err)}
	fencerB:=&takeoverDMPathFencer{paths:[]string{"mpatha","mpathb"},failPath:"mpathb",oldGate:gateA,oldEpoch:leaseA.Epoch()}
	nodeB:=newTakeoverCoordinator(t,locker,recordsB,"node-b","boot-b",fencerB,&integrationDrain{})
	leaseB,err:=nodeB.AcquireOwner(context.Background(),"vol-a","node-b","boot-b")
	if err==nil{_ = leaseB.Close();t.Fatal("new Owner acquired despite failed PR verification on one path")}
	if gateB.AdmitWrite(){t.Fatal("new node admitted writes after incomplete PR fencing")}
	if !gateA.AdmitWrite(){t.Fatal("failed path injection unexpectedly claimed old Owner was fenced")}
	if locker.active.Load()!=0{t.Fatalf("sanlock lease leaked after failed takeover: %d",locker.active.Load())}
}

func TestTwoNodeTakeoverDelayedOldEpochFenceCannotCloseNewOwner(t *testing.T){
	g:=newTakeoverDMGate()
	if err:=g.Prepare(context.Background(),"dm",50);err!=nil{t.Fatal(err)}
	if err:=g.Enable(context.Background(),"dm",50);err!=nil{t.Fatal(err)}
	if err:=g.Fence(context.Background(),"dm",51);err!=nil{t.Fatal(err)}
	if err:=g.Prepare(context.Background(),"dm",52);err!=nil{t.Fatal(err)}
	if err:=g.Enable(context.Background(),"dm",52);err!=nil{t.Fatal(err)}
	if err:=g.Fence(context.Background(),"dm",50);err==nil{t.Fatal("stale old Owner fence accepted")}
	epoch,open,fenced:=g.snapshot()
	if epoch!=52||!open||fenced{t.Fatalf("stale fence changed new Owner state: epoch=%d open=%v fenced=%v",epoch,open,fenced)}
}
