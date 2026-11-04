package metastore

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

type runtimeTestCloser struct {
	events *[]string
	mu *sync.Mutex
	name string
}
func (c *runtimeTestCloser) Close() error { c.mu.Lock(); defer c.mu.Unlock(); *c.events=append(*c.events,c.name); return nil }

type runtimeTestDevice struct { closer runtimeTestCloser }
func (d *runtimeTestDevice) ReadAt(p []byte, _ int64)(int,error){return len(p),nil}
func (d *runtimeTestDevice) WriteAt(p []byte, _ int64)(int,error){return len(p),nil}
func (d *runtimeTestDevice) Sync()error{return nil}
func (d *runtimeTestDevice) Size()int64{return 1<<20}
func (d *runtimeTestDevice) Close()error{return d.closer.Close()}

type runtimeTestReader struct { events *[]string; mu *sync.Mutex }
func (r *runtimeTestReader) CloseDrained(_ context.Context, drained bool) error {
	if !drained { return ErrUnsafeReclaim }
	r.mu.Lock(); defer r.mu.Unlock(); *r.events=append(*r.events,"reader-close"); return nil
}

type runtimeTestRecovery struct {
	id string
	started chan struct{}
	events *[]string
	mu *sync.Mutex
}
func (r *runtimeTestRecovery) VolumeID() string { return r.id }
func (r *runtimeTestRecovery) ValidateProduction() error { return nil }
func (r *runtimeTestRecovery) Run(ctx context.Context) error {
	r.mu.Lock(); *r.events=append(*r.events,"recovery-start"); r.mu.Unlock()
	close(r.started)
	<-ctx.Done()
	r.mu.Lock(); *r.events=append(*r.events,"recovery-stop"); r.mu.Unlock()
	return ctx.Err()
}

type runtimeTestBackend struct {
	mu sync.Mutex
	events []string
	drainErr error
	recoveryStarted chan struct{}
}
func (b *runtimeTestBackend) add(v string) { b.mu.Lock(); defer b.mu.Unlock(); b.events=append(b.events,v) }
func (b *runtimeTestBackend) snapshot() []string { b.mu.Lock(); defer b.mu.Unlock(); return append([]string(nil),b.events...) }
func (b *runtimeTestBackend) closer(name string) io.Closer { return &runtimeTestCloser{events:&b.events,mu:&b.mu,name:name} }
func (b *runtimeTestBackend) OpenDevice(context.Context,VolumeRuntimeSpec)(RuntimeMetadataDevice,error){b.add("device-open");return &runtimeTestDevice{closer:runtimeTestCloser{events:&b.events,mu:&b.mu,name:"device-close"}},nil}
func (b *runtimeTestBackend) AcquireOwnerLease(context.Context,VolumeRuntimeSpec)(io.Closer,error){b.add("owner-acquire");return b.closer("owner-release"),nil}
func (b *runtimeTestBackend) RecoverWAL(context.Context,VolumeRuntimeSpec,RuntimeMetadataDevice)error{b.add("wal-recover");return nil}
func (b *runtimeTestBackend) OpenMetadata(_ context.Context,s VolumeRuntimeSpec,_ RuntimeMetadataDevice)(*VolumeRuntimeMetadata,error){
	b.add("metadata-open")
	store:=&Store{sb:Superblock{VolumeID:s.VolumeID,TotalPages:16,ArenaStart:2,ArenaPages:8,RootPage:NoPage}}
	return &VolumeRuntimeMetadata{Store:store,ReaderTable:&ReaderTable{store:store},Arena:&PersistentArena{store:store}},nil
}
func (b *runtimeTestBackend) JoinReader(context.Context,VolumeRuntimeSpec,*VolumeRuntimeMetadata)(RuntimeReaderSession,error){b.add("reader-join");return &runtimeTestReader{events:&b.events,mu:&b.mu},nil}
func (b *runtimeTestBackend) BuildReaderRecovery(s VolumeRuntimeSpec,_ *VolumeRuntimeMetadata)(ManagedReaderRecovery,error){
	b.add("recovery-build")
	r := &runtimeTestRecovery{id:s.VolumeID,started:make(chan struct{}),events:&b.events,mu:&b.mu}
	b.recoveryStarted = r.started
	return r,nil
}
func (b *runtimeTestBackend) EnableRequests(context.Context,VolumeRuntimeSpec,*VolumeRuntimeMetadata)error{b.add("requests-enable");return nil}
func (b *runtimeTestBackend) StopRequests(context.Context,string)error{b.add("requests-stop");return nil}
func (b *runtimeTestBackend) DrainMetadataIO(context.Context,string)error{
	b.mu.Lock(); defer b.mu.Unlock()
	b.events=append(b.events,"io-drain")
	return b.drainErr
}
func (b *runtimeTestBackend) CloseMetadata(context.Context,VolumeRuntimeSpec,*VolumeRuntimeMetadata)error{b.add("metadata-close");return nil}

func waitRuntimeRecovery(t *testing.T,b *runtimeTestBackend) {
	t.Helper()
	if b.recoveryStarted == nil { t.Fatal("recovery controller was not built") }
	select { case <-b.recoveryStarted: case <-time.After(time.Second): t.Fatal("recovery worker did not start") }
}
func assertRuntimeEventOrder(t *testing.T,events []string,ordered ...string) {
	t.Helper()
	position:=0
	for _,want:=range ordered {
		found:=-1
		for i:=position;i<len(events);i++ { if events[i]==want { found=i;break } }
		if found<0 { t.Fatalf("event %q missing or out of order in %v",want,events) }
		position=found+1
	}
}

func runtimeTestSpec(mode VolumeRuntimeMode) VolumeRuntimeSpec {
	return VolumeRuntimeSpec{VolumeID:"vol-runtime",Mode:mode,NodeID:"node-a",BootID:"boot-a"}
}

func TestVolumeRuntimeReadOnlyLifecycleOrder(t *testing.T) {
	b:=&runtimeTestBackend{}
	m,err:=NewVolumeRuntimeManager(b);if err!=nil{t.Fatal(err)}
	spec:=runtimeTestSpec(VolumeRuntimeReadOnly)
	if err=m.Activate(context.Background(),spec);err!=nil{t.Fatal(err)}
	if err=m.Activate(context.Background(),spec);err!=nil{t.Fatalf("idempotent activation: %v",err)}
	conflict:=spec;conflict.Mode=VolumeRuntimeReadWrite
	if err=m.Activate(context.Background(),conflict);!errors.Is(err,ErrVolumeRuntimeConflict){t.Fatalf("conflicting activation: %v",err)}
	waitRuntimeRecovery(t,b)
	if got:=m.ActiveVolumes();!reflect.DeepEqual(got,[]string{"vol-runtime"}){t.Fatalf("active volumes=%v",got)}
	ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
	if err=m.Deactivate(ctx,"vol-runtime");err!=nil{t.Fatal(err)}
	got:=b.snapshot()
	assertRuntimeEventOrder(t,got,"device-open","metadata-open","reader-join","recovery-build","requests-enable","requests-stop","recovery-stop","io-drain","reader-close","metadata-close","device-close")
	assertRuntimeEventOrder(t,got,"recovery-build","recovery-start","recovery-stop")
}

func TestVolumeRuntimeReadWriteRecoversWALAfterOwnerLease(t *testing.T) {
	b:=&runtimeTestBackend{}
	m,err:=NewVolumeRuntimeManager(b);if err!=nil{t.Fatal(err)}
	if err=m.Activate(context.Background(),runtimeTestSpec(VolumeRuntimeReadWrite));err!=nil{t.Fatal(err)}
	waitRuntimeRecovery(t,b)
	ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
	if err=m.Deactivate(ctx,"vol-runtime");err!=nil{t.Fatal(err)}
	got:=b.snapshot()
	assertRuntimeEventOrder(t,got,"device-open","owner-acquire","wal-recover","metadata-open","recovery-build","requests-enable","recovery-start","requests-stop","recovery-stop","io-drain","metadata-close","owner-release","device-close")
	for _,event:=range got {if event=="reader-join"{t.Fatal("RW owner must not join a RO reader session")}}
}

func TestVolumeRuntimeDrainFailureRetainsResourcesForRetry(t *testing.T) {
	b:=&runtimeTestBackend{drainErr:errors.New("DM queue not drained")}
	m,err:=NewVolumeRuntimeManager(b);if err!=nil{t.Fatal(err)}
	if err=m.Activate(context.Background(),runtimeTestSpec(VolumeRuntimeReadOnly));err!=nil{t.Fatal(err)}
	waitRuntimeRecovery(t,b)
	ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
	if err=m.Deactivate(ctx,"vol-runtime");err==nil{t.Fatal("expected drain failure")}
	if got:=m.ActiveVolumes();len(got)!=0{t.Fatalf("stopping runtime reported active: %v",got)}
	if err=m.Activate(ctx,runtimeTestSpec(VolumeRuntimeReadOnly));!errors.Is(err,ErrVolumeRuntimeStopping){t.Fatalf("activation during pending cleanup: %v",err)}
	for _,event:=range b.snapshot(){if event=="metadata-close"||event=="device-close"||event=="reader-close"{t.Fatalf("resources released before successful drain: %v",b.snapshot())}}
	b.mu.Lock();b.drainErr=nil;b.mu.Unlock()
	if err=m.Deactivate(ctx,"vol-runtime");err!=nil{t.Fatal(err)}
	if got:=m.ActiveVolumes();len(got)!=0{t.Fatalf("runtime still active: %v",got)}
}

func TestVolumeRuntimeActivationFailureRollsBack(t *testing.T) {
	b:=&runtimeTestBackend{}
	// Inject a WAL recovery error after owner lease acquisition.
	failing:=&runtimeFailingWALBackend{runtimeTestBackend:b,err:errors.New("bad WAL")}
	m,err:=NewVolumeRuntimeManager(failing);if err!=nil{t.Fatal(err)}
	if err=m.Activate(context.Background(),runtimeTestSpec(VolumeRuntimeReadWrite));err==nil{t.Fatal("expected WAL recovery error")}
	want:=[]string{"device-open","owner-acquire","wal-recover","owner-release","device-close"}
	if got:=b.snapshot();!reflect.DeepEqual(got,want){t.Fatalf("rollback order got %v want %v",got,want)}
	if len(m.ActiveVolumes())!=0{t.Fatal("failed activation was published")}
}
type runtimeFailingWALBackend struct { *runtimeTestBackend; err error }
func (b *runtimeFailingWALBackend) RecoverWAL(context.Context,VolumeRuntimeSpec,RuntimeMetadataDevice)error{b.add("wal-recover");return b.err}
