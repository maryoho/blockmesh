package dynamicruntime

import (
	"context"
	"errors"
	"sync"
	"testing"

	"blockmesh/internal/metastore"
)

type fakeDMOwnerGate struct {
	mu sync.Mutex
	next uint64
	prepared,enabled,fenced,drained,current int
	err error
}
func (g *fakeDMOwnerGate) FenceCurrent(context.Context,string)error{g.mu.Lock();defer g.mu.Unlock();g.current++;return g.err}
func (g *fakeDMOwnerGate) Prepare(context.Context,string,uint64)(uint64,error){g.mu.Lock();defer g.mu.Unlock();g.prepared++;g.next++;return g.next,g.err}
func (g *fakeDMOwnerGate) Enable(context.Context,string,uint64,uint64)error{g.mu.Lock();defer g.mu.Unlock();g.enabled++;return g.err}
func (g *fakeDMOwnerGate) Fence(context.Context,string,uint64,uint64)error{g.mu.Lock();defer g.mu.Unlock();g.fenced++;return g.err}
func (g *fakeDMOwnerGate) Drain(context.Context,string,uint64,uint64)error{g.mu.Lock();defer g.mu.Unlock();g.drained++;return g.err}

type fakeDMReaderGate struct { mu sync.Mutex; next uint64; prepared,enabled,fenced int; err error }
func(g *fakeDMReaderGate)PrepareReader(context.Context,string,uint64,uint64)(uint64,error){g.mu.Lock();defer g.mu.Unlock();g.prepared++;g.next++;return g.next,g.err}
func(g *fakeDMReaderGate)EnableReader(context.Context,string,uint64,uint64,uint64)error{g.mu.Lock();defer g.mu.Unlock();g.enabled++;return g.err}
func(g *fakeDMReaderGate)FenceReader(context.Context,string,uint64,uint64)error{g.mu.Lock();defer g.mu.Unlock();g.fenced++;return g.err}

func TestDMRequestGateRWLifecycle(t *testing.T){
	ownerGate:=&fakeDMOwnerGate{}
	readerGate:=&fakeDMReaderGate{}
	g,err:=NewDMRequestGate(ownerGate,readerGate,func(id string)string{return "/dev/mapper/"+id})
	if err!=nil{t.Fatal(err)}
	spec:=rwSpec()
	owner:=newManagerTestOwner(spec.VolumeID)
	rt:=&Runtime{Spec:spec,Owner:owner}
	checked:=false
	if err=g.EnableRequestsGuarded(context.Background(),spec,rt,func()error{checked=true;if !owner.Valid(){return errors.New("owner lease lost")};return nil});err!=nil{t.Fatal(err)}
	if !checked{t.Fatal("final health check was not called")}
	if ownerGate.prepared!=1||ownerGate.enabled!=1{t.Fatalf("prepare/enable=%d/%d",ownerGate.prepared,ownerGate.enabled)}
	if err=g.StopRequests(context.Background(),spec.VolumeID);err!=nil{t.Fatal(err)}
	if err=g.DrainRequests(context.Background(),spec.VolumeID);err!=nil{t.Fatal(err)}
	if ownerGate.fenced!=1{t.Fatalf("StopRequests fence must synchronously drain; got %d fences",ownerGate.fenced)}
}

func TestDMRequestGateROUsesReaderEpochAndNeverOwnerGate(t *testing.T){
	ownerGate:=&fakeDMOwnerGate{}
	readerGate:=&fakeDMReaderGate{}
	g,err:=NewDMRequestGate(ownerGate,readerGate,func(string)string{return "/dev/mapper/ro"})
	if err!=nil{t.Fatal(err)}
	spec:=metastore.VolumeRuntimeSpec{VolumeID:"vol-ro",Mode:metastore.VolumeRuntimeReadOnly,NodeID:"node",BootID:"boot",ReaderSlot:3,ReaderEpoch:12}
	rt:=&Runtime{Spec:spec,RootGeneration:44}
	if err=g.EnableRequestsGuarded(context.Background(),spec,rt,func()error{return nil});err!=nil{t.Fatal(err)}
	if readerGate.prepared!=1||readerGate.enabled!=1{t.Fatalf("reader prepare/enable=%d/%d",readerGate.prepared,readerGate.enabled)}
	if ownerGate.prepared!=0||ownerGate.enabled!=0{t.Fatal("RO activation touched RW Owner gate")}
	if err=g.StopRequests(context.Background(),spec.VolumeID);err!=nil{t.Fatal(err)}
	if err=g.DrainRequests(context.Background(),spec.VolumeID);err!=nil{t.Fatal(err)}
	if readerGate.fenced!=1{t.Fatalf("Reader StopRequests fence must synchronously drain; got %d fences",readerGate.fenced)}
}

func TestDMRequestGateHealthFailureFencesBeforeReturning(t *testing.T){
	ownerGate:=&fakeDMOwnerGate{}
	g,err:=NewDMRequestGate(ownerGate,&fakeDMReaderGate{},func(string)string{return "/dev/mapper/fail"})
	if err!=nil{t.Fatal(err)}
	spec:=rwSpec()
	rt:=&Runtime{Spec:spec,Owner:newManagerTestOwner(spec.VolumeID)}
	want:=errors.New("lease expired")
	if err=g.EnableRequestsGuarded(context.Background(),spec,rt,func()error{return want});!errors.Is(err,want){t.Fatalf("got %v, want health error",err)}
	if ownerGate.fenced!=1{t.Fatalf("health failure did not fence prepared target: %d",ownerGate.fenced)}
}
