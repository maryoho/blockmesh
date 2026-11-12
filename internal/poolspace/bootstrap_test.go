package poolspace

import (
	"context"
	"errors"
	"testing"

	"blockmesh/internal/device"
)

type testBootstrapLocker struct{}
func(testBootstrapLocker)Distributed()bool{return true}
func(testBootstrapLocker)Lock(context.Context,string)(func()error,error){return func()error{return nil},nil}

type testBootstrapOwner struct{epoch uint64;calls,failAt int}
func(o *testBootstrapOwner)Epoch()uint64{return o.epoch}
func(o *testBootstrapOwner)Valid()bool{o.calls++;return o.failAt==0||o.calls<o.failAt}

func attachBootstrapGuards(t *testing.T,s *BootstrapStore,owner *testBootstrapOwner){
	t.Helper()
	if err:=s.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	if err:=s.SetOwnerLease(owner);err!=nil{t.Fatal(err)}
}

type failWriteDevice struct{device.Device;failAt,writes int}
func(d *failWriteDevice)WriteAt(p []byte,off int64)(int,error){
	d.writes++
	if d.writes==d.failAt{return 0,errors.New("injected write failure")}
	return d.Device.WriteAt(p,off)
}
type failSyncDevice struct{device.Device;failAt,syncs int}
func(d *failSyncDevice)Sync()error{
	d.syncs++
	if d.syncs==d.failAt{return errors.New("injected sync uncertainty")}
	return d.Device.Sync()
}

func newSparsePool(t *testing.T)(*device.FileDevice,*BootstrapStore){
	t.Helper()
	f,err:=device.OpenFile(t.TempDir()+"/pool.img");if err!=nil{t.Fatal(err)}
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
	s,err:=FormatBootstrap(f,64,512);if err!=nil{t.Fatal(err)}
	return f,s
}

func TestBootstrapFormatOpenAndExpand(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	if s.Record().Generation!=1{t.Fatalf("generation=%d",s.Record().Generation)}
	reopened,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().PoolUUID!=s.Record().PoolUUID{t.Fatal("Pool UUID changed on reopen")}
	attachBootstrapGuards(t,reopened,&testBootstrapOwner{epoch:1})
	if _,err=reopened.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	reopened,err=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().Generation!=2{t.Fatalf("generation=%d",reopened.Record().Generation)}
	if reopened.Record().OwnerEpoch!=1{t.Fatalf("OwnerEpoch=%d",reopened.Record().OwnerEpoch)}
}

func TestBootstrapWALIgnoresUncommittedAfterImage(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	attachBootstrapGuards(t,s,&testBootstrapOwner{epoch:1})
	s.dev=&failWriteDevice{Device:f,failAt:4}
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err==nil{t.Fatal("expected injected commit write failure")}
	recovered,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if recovered.Record().Generation!=1{t.Fatalf("uncommitted root was published: generation=%d",recovered.Record().Generation)}
}

func TestBootstrapWALRecoversUncertainCommit(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	attachBootstrapGuards(t,s,&testBootstrapOwner{epoch:1})
	s.dev=&failSyncDevice{Device:f,failAt:4}
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err==nil{t.Fatal("expected injected Sync uncertainty")}
	recovered,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if recovered.Record().Generation!=2||recovered.Record().OwnerEpoch!=1{t.Fatalf("durable Commit was not recovered: %+v",recovered.Record())}
}

func TestBootstrapRejectsStaleWriter(t *testing.T){
	f,_:=newSparsePool(t);defer f.Close()
	first,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	second,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	attachBootstrapGuards(t,first,&testBootstrapOwner{epoch:1})
	attachBootstrapGuards(t,second,&testBootstrapOwner{epoch:1})
	if _,err=first.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	if _,err=second.ExpandMetadata(1<<30,1<<30);!errors.Is(err,ErrStaleBootstrap){t.Fatalf("expected stale writer rejection, got %v",err)}
}

func TestBootstrapOwnerLeaseLossPreventsCommit(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	owner:=&testBootstrapOwner{epoch:1,failAt:6}
	if err:=s.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	if err:=s.SetOwnerLease(owner);err!=nil{t.Fatal(err)}
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err==nil{t.Fatal("expected Owner lease loss before Commit")}
	recovered,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if recovered.Record().Generation!=1{t.Fatalf("published after Owner lease loss: generation=%d",recovered.Record().Generation)}
}

func TestBootstrapRescanPersistsOnlyFreeSpace(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	attachBootstrapGuards(t,s,&testBootstrapOwner{epoch:1})
	old:=uint64(f.Size());newSize:=old+(1<<30)
	if _,err:=f.WriteAt([]byte{0},int64(newSize-1));err!=nil{t.Fatal(err)}
	if got,err:=s.Rescan();err!=nil||got!=newSize{t.Fatalf("rescan got=%d err=%v",got,err)}
	reopened,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().DeviceBytes!=newSize{t.Fatalf("persisted device size=%d",reopened.Record().DeviceBytes)}
	var free uint64
	for _,e:=range reopened.Extents(){if e.Kind==Free{free+=e.Length}}
	if free<1<<30{t.Fatalf("new capacity was not added as Free: %d",free)}
}

func TestExtentViewUsesSameParentDevice(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	view,err:=s.OpenExtentView(0);if err!=nil{t.Fatal(err)}
	payload:=[]byte("same-lun-metadata")
	if _,err=view.WriteAt(payload,7<<20);err!=nil{t.Fatal(err)}
	physical:=make([]byte,len(payload))
	if _,err=f.ReadAt(physical,int64(7<<20));err!=nil{t.Fatal(err)}
	if string(physical)!=string(payload){t.Fatalf("parent device did not observe extent write: %q",physical)}
	if err=view.Close();err!=nil{t.Fatal(err)}
	if _,err=f.WriteAt([]byte{1},int64(7<<20));err!=nil{t.Fatalf("closing extent view closed parent: %v",err)}
}

func TestCommittedBootstrapMapCorruptionFailsClosed(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	attachBootstrapGuards(t,s,&testBootstrapOwner{epoch:1})
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	r:=s.Record()
	if _,err:=f.WriteAt([]byte{0xff},int64(r.ExtentMapOffset+70));err!=nil{t.Fatal(err)}
	if _,err:=OpenBootstrap(f);!errors.Is(err,ErrBootstrapWALCorrupt){t.Fatalf("expected committed root corruption to fail closed, got %v",err)}
}

func TestBootstrapOwnerEpochAdvances(t *testing.T){
 f,s:=newSparsePool(t);defer f.Close()
 attachBootstrapGuards(t,s,&testBootstrapOwner{epoch:1})
 if _,err:=s.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
 next,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
 attachBootstrapGuards(t,next,&testBootstrapOwner{epoch:2})
 if _,err=next.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
 final,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
 if final.Record().OwnerEpoch!=2||final.Record().Generation!=3{t.Fatalf("Owner epoch/generation not advanced: %+v",final.Record())}
}

type logicalSectorDevice struct{device.Device;sector int}
func(d logicalSectorDevice)LogicalSectorSize()int{return d.sector}
func TestBootstrapValidatesLogicalSectorGeometry(t *testing.T){
 f,err:=device.OpenFile(t.TempDir()+"/sector.img");if err!=nil{t.Fatal(err)};defer f.Close()
 if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
 d:=logicalSectorDevice{Device:f,sector:4096}
 if _,err=FormatBootstrap(d,64,512);err==nil{t.Fatal("expected configured sector mismatch")}
 s,err:=FormatBootstrap(d,64,4096);if err!=nil{t.Fatal(err)}
 if s.Record().LogicalSectorBytes!=4096{t.Fatalf("sector size=%d",s.Record().LogicalSectorBytes)}
 if _,err=OpenBootstrap(logicalSectorDevice{Device:f,sector:512});err==nil{t.Fatal("expected persisted sector mismatch")}
}

func TestFormatBootstrapRejectsDirtyReservedRegion(t *testing.T){
 f,err:=device.OpenFile(t.TempDir()+"/dirty.img");if err!=nil{t.Fatal(err)};defer f.Close()
 if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
 if _,err=f.WriteAt([]byte{0x7f},int64(6<<20));err!=nil{t.Fatal(err)}
 if _,err=FormatBootstrap(f,64,512);err==nil{t.Fatal("expected non-zero Bootstrap reservation to be rejected")}
}

func TestBootstrapAllocateDataExtentsPublishesConsistentViews(t *testing.T) {
	f, s := newSparsePool(t)
	defer f.Close()
	attachBootstrapGuards(t, s, &testBootstrapOwner{epoch: 1})
	allocated, err := s.AllocateDataExtents(8 << 20)
	if err != nil { t.Fatal(err) }
	if len(allocated) == 0 { t.Fatal("no Data extents allocated") }
	metadata, data, generation, err := s.OpenViews()
	if err != nil { t.Fatal(err) }
	if metadata == nil || data == nil || generation != 2 { t.Fatalf("invalid views metadata=%v data=%v generation=%d", metadata, data, generation) }
	if metadata.Generation()!=generation||data.Generation()!=generation{t.Fatalf("view generation mismatch metadata=%d data=%d extent-map=%d",metadata.Generation(),data.Generation(),generation)}
	if data.Size() != 8<<20 { t.Fatalf("Data view size=%d", data.Size()) }
	payload := []byte("logical data view on the same LUN")
	if n, err := data.WriteAt(payload, 0); err != nil || n != len(payload) { t.Fatalf("DataView write n=%d err=%v", n, err) }
	physical := make([]byte, len(payload))
	if _, err = f.ReadAt(physical, int64(allocated[0].Start)); err != nil { t.Fatal(err) }
	if string(physical) != string(payload) { t.Fatalf("parent LUN data=%q", physical) }
	reopened, err := OpenBootstrap(f)
	if err != nil { t.Fatal(err) }
	_, reopenedData, reopenedGeneration, err := reopened.OpenViews()
	if err != nil { t.Fatal(err) }
	if reopenedGeneration != generation || reopenedData == nil || reopenedData.Size() != data.Size() {
		t.Fatalf("reopened view generation=%d size=%v", reopenedGeneration, reopenedData)
	}
}

func TestBootstrapAllocateDataExtentsRequiresAllocationAlignment(t *testing.T) {
	f, s := newSparsePool(t)
	defer f.Close()
	attachBootstrapGuards(t, s, &testBootstrapOwner{epoch: 1})
	if _, err := s.AllocateDataExtents((4 << 20) + 1); err == nil {
		t.Fatal("expected non-4MiB-aligned Data extent allocation to fail")
	}
	if s.Record().Generation != 1 { t.Fatal("invalid Data extent request changed Bootstrap generation") }
}

func TestBootstrapRejectsLegacyFormat(t *testing.T){
	d,err:=device.OpenFile(t.TempDir()+"/legacy.img");if err!=nil{t.Fatal(err)};defer d.Close()
	page:=make([]byte,PageSize);binaryPutUint64(page,0x424D504F4F4C3032)
	if _,err=d.WriteAt(page,0);err!=nil{t.Fatal(err)}
	if _,err=FormatBootstrap(d,64,512);!errors.Is(err,ErrLegacyPoolFormat){t.Fatalf("expected legacy format rejection, got %v",err)}
}
func binaryPutUint64(b []byte,v uint64){for i:=0;i<8;i++{b[i]=byte(v>>(8*i))}}


func TestBootstrapFormatIntentRecoversInterruptedCreation(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/format-recovery.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil { t.Fatal(err) }
	fault:=&failWriteDevice{Device:f,failAt:2}
	if _,err=FormatBootstrapNamed(fault,"recover-pool",64,512);err==nil { t.Fatal("expected injected extent-map write failure") }
	if _,err=OpenBootstrap(f);!errors.Is(err,ErrBootstrapFormatIncomplete) { t.Fatalf("expected incomplete format marker, got %v",err) }
	recovered,err:=FormatBootstrapNamed(f,"recover-pool",64,512)
	if err!=nil { t.Fatalf("resume Bootstrap format: %v",err) }
	if recovered.Record().PoolName!="recover-pool" { t.Fatalf("PoolName=%q",recovered.Record().PoolName) }
	if _,err=OpenBootstrap(f);err!=nil { t.Fatalf("open recovered Bootstrap: %v",err) }
}

func TestBootstrapFormatIntentRejectsWrongPoolName(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/format-name.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil { t.Fatal(err) }
	fault:=&failWriteDevice{Device:f,failAt:2}
	if _,err=FormatBootstrapNamed(fault,"pool-a",64,512);err==nil { t.Fatal("expected injected failure") }
	if _,err=FormatBootstrapNamed(f,"pool-b",64,512);!errors.Is(err,ErrBootstrapFormatIncomplete) { t.Fatalf("expected format marker name mismatch, got %v",err) }
}
