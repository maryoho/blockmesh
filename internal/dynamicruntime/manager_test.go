package dynamicruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"blockmesh/internal/metastore"
	"blockmesh/internal/volume"
)

type managerTestOwner struct {
	volumeID string
	epoch uint64
	lost chan error
	mu sync.Mutex
	closed bool
}
func newManagerTestOwner(id string) *managerTestOwner {
	return &managerTestOwner{volumeID:id,epoch:7,lost:make(chan error,1)}
}
func (o *managerTestOwner) Resource() string { return "volume:"+o.volumeID }
func (o *managerTestOwner) Valid() bool { o.mu.Lock(); defer o.mu.Unlock(); return !o.closed }
func (o *managerTestOwner) Close() error { o.mu.Lock(); o.closed=true; o.mu.Unlock(); return nil }
func (o *managerTestOwner) Epoch() uint64 { return o.epoch }
func (o *managerTestOwner) VolumeID() string { return o.volumeID }
func (o *managerTestOwner) OwnerRecord() volume.OwnerRecord {
	return volume.OwnerRecord{VolumeID:o.volumeID,NodeID:"node-a",BootID:"boot-a",Epoch:o.epoch}
}
func (o *managerTestOwner) ReservationVerified() bool { return true }
func (o *managerTestOwner) PreviousOwnerFenced() bool { return true }
func (o *managerTestOwner) OldOwnerIODrained() bool { return true }
func (o *managerTestOwner) Lost() <-chan error { return o.lost }

type managerTestReader struct {
	mu sync.Mutex
	heartbeats int
	closed bool
}
func (r *managerTestReader) Heartbeat() error {
	r.mu.Lock(); defer r.mu.Unlock()
	if r.closed { return errors.New("reader closed") }
	r.heartbeats++
	return nil
}
func (r *managerTestReader) CloseDrained(_ context.Context, drained bool) error {
	if !drained { return errors.New("not drained") }
	r.mu.Lock(); r.closed=true; r.mu.Unlock()
	return nil
}

type managerTestGate struct {
	mu sync.Mutex
	enabled int
	stopped int
	drained int
	failStopped int
	failErr error
}
func (g *managerTestGate) EnableRequestsGuarded(_ context.Context, _ metastore.VolumeRuntimeSpec, _ *Runtime, check func() error) error {
	if err:=check(); err!=nil { return err }
	g.mu.Lock(); g.enabled++; g.mu.Unlock()
	return nil
}
func (g *managerTestGate) StopRequests(context.Context,string) error {
	g.mu.Lock(); g.stopped++; g.mu.Unlock(); return nil
}
func (g *managerTestGate) DrainRequests(context.Context,string) error {
	g.mu.Lock(); g.drained++; g.mu.Unlock(); return nil
}
func (g *managerTestGate) FailStop(_ context.Context,_ metastore.VolumeRuntimeSpec,_ error) error {
	g.mu.Lock(); defer g.mu.Unlock()
	g.failStopped++
	return g.failErr
}

func rwSpec() metastore.VolumeRuntimeSpec {
	return metastore.VolumeRuntimeSpec{VolumeID:"vol-1",Mode:metastore.VolumeRuntimeReadWrite,NodeID:"node-a",BootID:"boot-a"}
}

func TestDynamicVolumeRuntimeManagerReadWriteLifecycle(t *testing.T) {
	owner:=newManagerTestOwner("vol-1")
	gate:=&managerTestGate{}
	opened:=0
	m,err:=NewDynamicVolumeRuntimeManager(ManagerConfig{
		AcquireOwner:func(context.Context,metastore.VolumeRuntimeSpec)(OwnerLease,error){return owner,nil},
		OpenReadWrite:runtimeOpenerFunc(func(_ context.Context,s metastore.VolumeRuntimeSpec,o OwnerLease)(*Runtime,error){
			opened++
			return &Runtime{Spec:s,Owner:o},nil
		}),
		Gate:gate,
	})
	if err!=nil { t.Fatal(err) }
	spec:=rwSpec()
	if err=m.Activate(context.Background(),spec); err!=nil { t.Fatal(err) }
	if err=m.Activate(context.Background(),spec); err!=nil { t.Fatalf("idempotent activation: %v",err) }
	if opened!=1 { t.Fatalf("opened %d times, want 1",opened) }
	if got:=m.RuntimeState(spec.VolumeID); got!=RuntimeActive { t.Fatalf("state=%v, want active",got) }
	if err=m.Deactivate(context.Background(),spec.VolumeID); err!=nil { t.Fatal(err) }
	if got:=m.RuntimeState(spec.VolumeID); got!=RuntimeCreated { t.Fatalf("state after deactivate=%v",got) }
	gate.mu.Lock(); defer gate.mu.Unlock()
	if gate.enabled!=1 || gate.stopped!=1 || gate.drained!=1 { t.Fatalf("gate counts enable/stop/drain=%d/%d/%d",gate.enabled,gate.stopped,gate.drained) }
}

type runtimeOpenerFunc func(context.Context,metastore.VolumeRuntimeSpec,OwnerLease)(*Runtime,error)
func (f runtimeOpenerFunc) OpenReadWrite(ctx context.Context,s metastore.VolumeRuntimeSpec,o OwnerLease)(*Runtime,error) {
	return f(ctx,s,o)
}

func TestDynamicVolumeRuntimeManagerRejectsDifferentIdentity(t *testing.T) {
	owner:=newManagerTestOwner("vol-1")
	m,err:=NewDynamicVolumeRuntimeManager(ManagerConfig{
		AcquireOwner:func(context.Context,metastore.VolumeRuntimeSpec)(OwnerLease,error){return owner,nil},
		OpenReadWrite:runtimeOpenerFunc(func(_ context.Context,s metastore.VolumeRuntimeSpec,o OwnerLease)(*Runtime,error){return &Runtime{Spec:s,Owner:o},nil}),
		Gate:&managerTestGate{},
	})
	if err!=nil { t.Fatal(err) }
	spec:=rwSpec()
	if err=m.Activate(context.Background(),spec); err!=nil { t.Fatal(err) }
	other:=spec; other.NodeID="node-b"
	if err=m.Activate(context.Background(),other); !errors.Is(err,ErrRuntimeConflict) { t.Fatalf("got %v, want conflict",err) }
	if err=m.Deactivate(context.Background(),spec.VolumeID); err!=nil { t.Fatal(err) }
}

func TestDynamicVolumeRuntimeManagerReadOnlyLifecycle(t *testing.T) {
	gate:=&managerTestGate{}
	reader:=&managerTestReader{}
	m,err:=NewDynamicVolumeRuntimeManager(ManagerConfig{
		OpenReadWrite:runtimeOpenerFunc(func(context.Context,metastore.VolumeRuntimeSpec,OwnerLease)(*Runtime,error){return nil,errors.New("unexpected RW open")}),
		OpenReadOnly:func(_ context.Context,s metastore.VolumeRuntimeSpec)(*Runtime,ReadOnlySession,error){return &Runtime{Spec:s},reader,nil},
		Gate:gate,
	})
	if err!=nil { t.Fatal(err) }
	spec:=metastore.VolumeRuntimeSpec{VolumeID:"vol-ro",Mode:metastore.VolumeRuntimeReadOnly,NodeID:"node-ro",BootID:"boot-ro",ReaderSlot:2,ReaderEpoch:9}
	if err=m.Activate(context.Background(),spec); err!=nil { t.Fatal(err) }
	if err=m.Deactivate(context.Background(),spec.VolumeID); err!=nil { t.Fatal(err) }
	reader.mu.Lock(); closed:=reader.closed; heartbeats:=reader.heartbeats; reader.mu.Unlock()
	if !closed { t.Fatal("Reader session was not closed after drain") }
	if heartbeats==0 { t.Fatal("Reader session was not health-checked before admission") }
}

func TestDynamicVolumeRuntimeManagerPinsRuntimeWhenFailStopFails(t *testing.T) {
	owner:=newManagerTestOwner("vol-1")
	gate:=&managerTestGate{failErr:errors.New("DM suspend failed")}
	m,err:=NewDynamicVolumeRuntimeManager(ManagerConfig{
		AcquireOwner:func(context.Context,metastore.VolumeRuntimeSpec)(OwnerLease,error){return owner,nil},
		OpenReadWrite:runtimeOpenerFunc(func(_ context.Context,s metastore.VolumeRuntimeSpec,o OwnerLease)(*Runtime,error){return &Runtime{Spec:s,Owner:o},nil}),
		Gate:gate,
	})
	if err!=nil { t.Fatal(err) }
	spec:=rwSpec()
	if err=m.Activate(context.Background(),spec); err!=nil { t.Fatal(err) }
	owner.lost<-errors.New("lease lost")
	// Wait for the synchronous fail-stop callback to be observed.
	for i:=0;i<100;i++ {
		gate.mu.Lock(); n:=gate.failStopped; gate.mu.Unlock()
		if n>0 { break }
		if i==99 { t.Fatal("lease-loss watcher did not invoke fail-stop") }
		time.Sleep(time.Millisecond)
	}
	if err=m.Deactivate(context.Background(),spec.VolumeID); err==nil { t.Fatal("expected teardown to retain resources after fail-stop failure") }
	if m.RuntimeState(spec.VolumeID)!=RuntimeLeaseLost { t.Fatal("lost runtime state was not retained") }
}
