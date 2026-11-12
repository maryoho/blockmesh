package dmowner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// kernelGateModel mirrors the DM target's spinlock-protected admission state.
// It is a deterministic software model, not a substitute for kernel testing.
type kernelGateModel struct {
	control sync.Mutex
	mu sync.Mutex
	cond *sync.Cond
	epoch uint64
	open bool
	fenced bool
	inflight int
}
func newKernelGateModel()*kernelGateModel{
	g:=&kernelGateModel{fenced:true}
	g.cond=sync.NewCond(&g.mu)
	return g
}
func(g *kernelGateModel)Prepare(_ context.Context,_ string,epoch uint64)error{
	if epoch==0{return errors.New("zero epoch")}
	g.control.Lock();defer g.control.Unlock()
	g.mu.Lock()
	if epoch<=g.epoch{g.mu.Unlock();return errors.New("stale epoch")}
	g.open=false;g.fenced=true
	for g.inflight!=0{g.cond.Wait()}
	g.epoch=epoch;g.fenced=false
	g.mu.Unlock()
	return nil
}
func(g *kernelGateModel)Enable(_ context.Context,_ string,epoch uint64)error{
	g.control.Lock();defer g.control.Unlock()
	g.mu.Lock();defer g.mu.Unlock()
	if epoch==0||epoch!=g.epoch{return errors.New("epoch mismatch")}
	if g.fenced||g.open||g.inflight!=0{return errors.New("gate is fenced, open, or not drained")}
	g.open=true
	return nil
}
func(g *kernelGateModel)Fence(_ context.Context,_ string,epoch uint64)error{
	g.control.Lock();defer g.control.Unlock()
	g.mu.Lock();defer g.mu.Unlock()
	if epoch==0||epoch<g.epoch{return errors.New("stale fence")}
	g.open=false;g.fenced=true
	if epoch>g.epoch{g.epoch=epoch}
	for g.inflight!=0{g.cond.Wait()}
	return nil
}
func(g *kernelGateModel)Drain(ctx context.Context,device string,epoch uint64)error{return g.Fence(ctx,device,epoch)}
func(g *kernelGateModel)AdmitWrite()bool{
	g.mu.Lock();defer g.mu.Unlock()
	if !g.open||g.fenced||g.epoch==0{return false}
	g.inflight++
	return true
}
func(g *kernelGateModel)CompleteWrite(){
	g.mu.Lock();defer g.mu.Unlock()
	if g.inflight<=0{panic("complete without admitted write")}
	g.inflight--
	if g.inflight==0{g.cond.Broadcast()}
}
func(g *kernelGateModel)snapshot()(uint64,bool,bool,int){
	g.mu.Lock();defer g.mu.Unlock()
	return g.epoch,g.open,g.fenced,g.inflight
}

func TestKernelGateFenceClosesAdmissionAndWaitsForInflightWrites(t *testing.T){
	ctx:=context.Background();g:=newKernelGateModel()
	if g.AdmitWrite(){t.Fatal("new target admitted writes before Owner prepare")}
	if err:=g.Prepare(ctx,"dm-a",10);err!=nil{t.Fatal(err)}
	if err:=g.Enable(ctx,"dm-a",10);err!=nil{t.Fatal(err)}
	if !g.AdmitWrite(){t.Fatal("prepared Owner was not admitted")}
	done:=make(chan error,1)
	go func(){done<-g.Fence(ctx,"dm-a",10)}()
	deadline:=time.Now().Add(time.Second)
	for{
		_,open,_,inflight:=g.snapshot()
		if !open&&inflight==1{break}
		if time.Now().After(deadline){t.Fatal("fence did not close gate")}
		time.Sleep(time.Millisecond)
	}
	select{case err:=<-done:t.Fatalf("fence returned before in-flight write completed: %v",err);default:}
	g.CompleteWrite()
	select{case err:=<-done:if err!=nil{t.Fatal(err)};case <-time.After(time.Second):t.Fatal("fence did not finish after I/O completion")}
	if g.AdmitWrite(){t.Fatal("write admitted after fence returned")}
	if err:=g.Enable(ctx,"dm-a",10);err==nil{t.Fatal("fenced epoch reopened")}
}

func TestKernelGateStaleMessagesCannotAffectNewOwner(t *testing.T){
	ctx:=context.Background();g:=newKernelGateModel()
	if err:=g.Prepare(ctx,"dm-a",20);err!=nil{t.Fatal(err)}
	if err:=g.Enable(ctx,"dm-a",20);err!=nil{t.Fatal(err)}
	if err:=g.Fence(ctx,"dm-a",21);err!=nil{t.Fatal(err)}
	if err:=g.Prepare(ctx,"dm-a",22);err!=nil{t.Fatal(err)}
	if err:=g.Enable(ctx,"dm-a",22);err!=nil{t.Fatal(err)}
	if err:=g.Fence(ctx,"dm-a",20);err==nil{t.Fatal("delayed old fence accepted")}
	if err:=g.Enable(ctx,"dm-a",20);err==nil{t.Fatal("delayed old enable accepted")}
	if err:=g.Prepare(ctx,"dm-a",21);err==nil{t.Fatal("older prepare accepted")}
	epoch,open,fenced,inflight:=g.snapshot()
	if epoch!=22||!open||fenced||inflight!=0{t.Fatalf("stale command changed current Owner: epoch=%d open=%v fenced=%v inflight=%d",epoch,open,fenced,inflight)}
}

func TestKernelGateTableReloadStartsClosedAndForgetsOldAdmission(t *testing.T){
	ctx:=context.Background();old:=newKernelGateModel()
	if err:=old.Prepare(ctx,"dm-a",30);err!=nil{t.Fatal(err)}
	if err:=old.Enable(ctx,"dm-a",30);err!=nil{t.Fatal(err)}
	reloaded:=newKernelGateModel()
	if reloaded.AdmitWrite(){t.Fatal("reloaded DM table inherited old open gate")}
	if err:=reloaded.Enable(ctx,"dm-a",30);err==nil{t.Fatal("reloaded table accepted old epoch without prepare")}
	if err:=reloaded.Prepare(ctx,"dm-a",31);err!=nil{t.Fatal(err)}
	if err:=reloaded.Enable(ctx,"dm-a",31);err!=nil{t.Fatal(err)}
}

func TestKernelGateConcurrentAdmissionAndFenceRace(t *testing.T){
	ctx:=context.Background();g:=newKernelGateModel()
	if err:=g.Prepare(ctx,"dm-a",40);err!=nil{t.Fatal(err)}
	if err:=g.Enable(ctx,"dm-a",40);err!=nil{t.Fatal(err)}
	var fenced atomic.Bool
	var admittedAfterFence atomic.Int64
	stop:=make(chan struct{})
	var workers sync.WaitGroup
	for i:=0;i<8;i++{
		workers.Add(1)
		go func(){
			defer workers.Done()
			for{
				select{case <-stop:return;default:}
				if g.AdmitWrite(){
					if fenced.Load(){admittedAfterFence.Add(1)}
					time.Sleep(time.Microsecond)
					g.CompleteWrite()
				}
			}
		}()
	}
	time.Sleep(5*time.Millisecond)
	if err:=g.Fence(ctx,"dm-a",40);err!=nil{t.Fatal(err)}
	fenced.Store(true)
	for i:=0;i<1000;i++{if g.AdmitWrite(){admittedAfterFence.Add(1);g.CompleteWrite()}}
	close(stop);workers.Wait()
	if n:=admittedAfterFence.Load();n!=0{t.Fatalf("%d writes admitted after Fence completed",n)}
	_,open,_,inflight:=g.snapshot()
	if open||inflight!=0{t.Fatalf("post-fence state open=%v inflight=%d",open,inflight)}
}
