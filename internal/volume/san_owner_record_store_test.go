package volume

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
)

type ownerRecordMemDevice struct { mu sync.Mutex; data []byte; syncErr error; shortWrite bool }
func newOwnerRecordMemDevice() *ownerRecordMemDevice { return &ownerRecordMemDevice{data:make([]byte,64*1024)} }
func (d *ownerRecordMemDevice) ReadAt(p []byte,off int64)(int,error) {
	d.mu.Lock();defer d.mu.Unlock()
	if off<0 || off>=int64(len(d.data)) { return 0,io.EOF }
	n:=copy(p,d.data[off:])
	if n<len(p) { return n,io.EOF }
	return n,nil
}
func (d *ownerRecordMemDevice) WriteAt(p []byte,off int64)(int,error) {
	d.mu.Lock();defer d.mu.Unlock()
	if d.shortWrite { n:=len(p)/2;copy(d.data[off:],p[:n]);return n,nil }
	if off<0 || off+int64(len(p))>int64(len(d.data)) { return 0,io.EOF }
	return copy(d.data[off:],p),nil
}
func (d *ownerRecordMemDevice) Sync()error{return d.syncErr}

func TestSANOwnerRecordStoreCreateLoadAndCAS(t *testing.T) {
	dev:=newOwnerRecordMemDevice()
	store,err:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192})
	if err!=nil { t.Fatal(err) }
	ctx:=context.Background()
	if _,exists,err:=store.Load(ctx,"vol-a");err!=nil||exists { t.Fatalf("initial load exists=%v err=%v",exists,err) }
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err=store.CompareAndSwap(ctx,"vol-a",nil,first);err!=nil { t.Fatal(err) }
	got,exists,err:=store.Load(ctx,"vol-a")
	if err!=nil||!exists||got!=first { t.Fatalf("load=(%+v,%v,%v)",got,exists,err) }
	second:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err=store.CompareAndSwap(ctx,"vol-a",&first,second);err!=nil { t.Fatal(err) }
	got,exists,err=store.Load(ctx,"vol-a")
	if err!=nil||!exists||got!=second { t.Fatalf("updated load=(%+v,%v,%v)",got,exists,err) }
}

func TestSANOwnerRecordStoreRejectsStaleCASAndVolumeMismatch(t *testing.T) {
	dev:=newOwnerRecordMemDevice()
	store,_:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192})
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",nil,first);err!=nil { t.Fatal(err) }
	stale:=first;stale.Epoch=1
	next:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",&stale,next);!errors.Is(err,ErrOwnerRecordConflict) { t.Fatalf("stale CAS=%v",err) }
	if _,_,err:=store.Load(context.Background(),"vol-b");err==nil { t.Fatal("cross-volume read accepted") }
}

func TestSANOwnerRecordStoreSurvivesTornInactiveSlot(t *testing.T) {
	dev:=newOwnerRecordMemDevice()
	store,_:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192})
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",nil,first);err!=nil { t.Fatal(err) }
	dev.shortWrite=true
	second:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",&first,second);!errors.Is(err,io.ErrShortWrite) { t.Fatalf("short write=%v",err) }
	dev.shortWrite=false
	got,exists,err:=store.Load(context.Background(),"vol-a")
	if err!=nil||!exists||got!=first { t.Fatalf("torn inactive slot damaged prior record: %+v %v %v",got,exists,err) }
}

func TestSANOwnerRecordStoreSyncFailureDoesNotCorruptPreviousCopy(t *testing.T) {
	dev:=newOwnerRecordMemDevice()
	store,_:=NewSANOwnerRecordStore(SANOwnerRecordStoreConfig{Device:dev,VolumeID:"vol-a",SlotAOffset:4096,SlotBOffset:8192})
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",nil,first);err!=nil { t.Fatal(err) }
	dev.syncErr=errors.New("flush failed")
	second:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",&first,second);err==nil { t.Fatal("sync failure ignored") }
	dev.syncErr=nil
	got,exists,err:=store.Load(context.Background(),"vol-a")
	if err!=nil||!exists||got!=first { t.Fatalf("previous copy unavailable after sync failure: %+v %v %v",got,exists,err) }
}
