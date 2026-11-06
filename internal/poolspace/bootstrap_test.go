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

type failWriteDevice struct{device.Device;failAt,writes int}
func(d *failWriteDevice)WriteAt(p []byte,off int64)(int,error){
	d.writes++;if d.writes==d.failAt{return 0,errors.New("injected write failure")};return d.Device.WriteAt(p,off)
}
type failSyncDevice struct{device.Device;failAt,syncs int}
func(d *failSyncDevice)Sync()error{
	d.syncs++;if d.syncs==d.failAt{return errors.New("injected sync uncertainty")};return d.Device.Sync()
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
	if err=reopened.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	if _,err=reopened.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	reopened,err=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().Generation!=2{t.Fatalf("generation=%d",reopened.Record().Generation)}
}
func TestBootstrapWALIgnoresUncommittedAfterImage(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	if err:=s.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	s.dev=&failWriteDevice{Device:f,failAt:4} // map, intent, superblock, commit
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err==nil{t.Fatal("expected injected commit write failure")}
	recovered,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if recovered.Record().Generation!=1{t.Fatalf("uncommitted root was published: generation=%d",recovered.Record().Generation)}
}
func TestBootstrapWALRecoversUncertainCommit(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	if err:=s.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	s.dev=&failSyncDevice{Device:f,failAt:4} // commit page was written; its flush outcome is uncertain
	if _,err:=s.ExpandMetadata(1<<30,1<<30);err==nil{t.Fatal("expected injected Sync uncertainty")}
	recovered,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if recovered.Record().Generation!=2{t.Fatalf("durable Commit was not recovered: generation=%d",recovered.Record().Generation)}
}
func TestBootstrapRejectsStaleWriter(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	first,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	second,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if err=first.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	if err=second.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	if _,err=first.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	if _,err=second.ExpandMetadata(1<<30,1<<30);!errors.Is(err,ErrStaleBootstrap){t.Fatalf("expected stale writer rejection, got %v",err)}
}
func TestBootstrapRescanPersistsOnlyFreeSpace(t *testing.T){
	f,s:=newSparsePool(t);defer f.Close()
	if err:=s.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
	old:=uint64(f.Size());newSize:=old+(1<<30)
	if _,err:=f.WriteAt([]byte{0},int64(newSize-1));err!=nil{t.Fatal(err)}
	if got,err:=s.Rescan();err!=nil||got!=newSize{t.Fatalf("rescan got=%d err=%v",got,err)}
	reopened,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().DeviceBytes!=newSize{t.Fatalf("persisted device size=%d",reopened.Record().DeviceBytes)}
	var free uint64;for _,e:=range reopened.Extents(){if e.Kind==Free{free+=e.Length}}
	if free<1<<30{t.Fatalf("new capacity was not added as Free: %d",free)}
}
func TestBootstrapRejectsLegacyFormat(t *testing.T){
	d,err:=device.OpenFile(t.TempDir()+"/legacy.img");if err!=nil{t.Fatal(err)};defer d.Close()
	page:=make([]byte,PageSize);binaryPutUint64(page,0x424D504F4F4C3032)
	if _,err=d.WriteAt(page,0);err!=nil{t.Fatal(err)}
	if _,err=FormatBootstrap(d,64,512);!errors.Is(err,ErrLegacyPoolFormat){t.Fatalf("expected legacy format rejection, got %v",err)}
}
func binaryPutUint64(b []byte,v uint64){for i:=0;i<8;i++{b[i]=byte(v>>(8*i))}}
