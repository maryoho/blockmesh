package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/volume"
)

type dynamicVolumeTestCommitter struct{dev Device}
func(c dynamicVolumeTestCommitter)CommitImages(images []PageImage)error{
	for _,image:=range images{if err:=writeFullAt(c.dev,image.Data,int64(image.Offset));err!=nil{return err}}
	return c.dev.Sync()
}
func(dynamicVolumeTestCommitter)ClusterSafe()bool{return true}

type dynamicVolumeTestFence struct{err error}
type dynamicVolumeTestFenceLease struct{volumeID string;epoch uint64;valid bool}
func(l *dynamicVolumeTestFenceLease)Valid()bool{return l.valid}
func(l *dynamicVolumeTestFenceLease)VolumeID()string{return l.volumeID}
func(l *dynamicVolumeTestFenceLease)Epoch()uint64{return l.epoch}
func(l *dynamicVolumeTestFenceLease)Close()error{l.valid=false;return nil}
func(f dynamicVolumeTestFence)AcquireVolumeFencing(_ context.Context,volumeID string,epoch uint64)(lock.VolumeFencingLease,error){
	if f.err!=nil{return nil,f.err}
	return &dynamicVolumeTestFenceLease{volumeID:volumeID,epoch:epoch,valid:true},nil
}

type dynamicVolumeFailDevice struct{Device; failOffset int64; failed bool}
func(d *dynamicVolumeFailDevice)WriteAt(p []byte,off int64)(int,error){
	if off==d.failOffset&&!d.failed{d.failed=true;return 0,errors.New("injected Reserved allocator write failure")}
	return d.Device.WriteAt(p,off)
}

func dynamicVolumeAllocatorFixture(t *testing.T)(*memdev,format.DynamicSuperblock){
	t.Helper()
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var uuid [16]byte;copy(uuid[:],"dynamic-vol-alloc")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil{t.Fatal(err)}
	return newMemdev(g.MetadataBytes/PageSize),sb
}

func dynamicVolumeDescriptor(sb format.DynamicSuperblock,id string)format.DynamicVolumeDescriptor{
	return format.DynamicVolumeDescriptor{VolumeID:id,AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:sb.Geometry.MappingOffset,BitmapSequence:1,OwnerEpoch:1,
		ArenaPages:64,RootPage:^uint64(0),LifecyclePages:4}
}

func TestDynamicVolumeAllocatorAllocatesIndependentVolumes(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	committer:=dynamicVolumeTestCommitter{dev}
	first,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"volume-a"),committer);if err!=nil{t.Fatal(err)}
	second,err:=a.AllocateVolume(1,dynamicVolumeDescriptor(sb,"volume-b"),committer);if err!=nil{t.Fatal(err)}
	if first.ArenaStart+first.ArenaPages+first.LifecyclePages>second.ArenaStart{
		t.Fatalf("Volume reservations overlap: first=%+v second=%+v",first,second)
	}
	if first.ArenaStart<sb.Geometry.ReservedOffset/format.PageSize+3{
		t.Fatalf("Volume Arena overlaps allocator metadata: %+v",first)
	}
	if _,err=OpenDynamicPoolVolume(dev,sb,0,"volume-a");err!=nil{t.Fatalf("open first Volume: %v",err)}
	if _,err=OpenDynamicPoolVolume(dev,sb,1,"volume-b");err!=nil{t.Fatalf("open second Volume: %v",err)}
	reopened,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	third,err:=reopened.AllocateVolume(2,dynamicVolumeDescriptor(sb,"volume-c"),committer);if err!=nil{t.Fatal(err)}
	if third.ArenaStart<second.ArenaStart+second.ArenaPages+second.LifecyclePages{t.Fatalf("allocator reused live Volume pages: %+v",third)}
}

func TestDynamicVolumeAllocatorRequiresFencingBeforeReuse(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	committer:=dynamicVolumeTestCommitter{dev}
	first,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"volume-release"),committer);if err!=nil{t.Fatal(err)}
	if err=a.ReleaseVolume(context.Background(),0,dynamicVolumeTestFence{err:errors.New("one SAN path not fenced")},committer);err==nil{
		t.Fatal("allocator reused Volume pages without a fencing proof")
	}
	if err=a.ReleaseVolume(context.Background(),0,dynamicVolumeTestFence{},committer);err!=nil{t.Fatal(err)}
	next,err:=a.AllocateVolume(1,dynamicVolumeDescriptor(sb,"volume-next"),committer);if err!=nil{t.Fatal(err)}
	if next.ArenaStart!=first.ArenaStart{t.Fatalf("released pages not reused: old=%d new=%d",first.ArenaStart,next.ArenaStart)}
	if _,err=OpenDynamicPoolVolume(dev,sb,0,"");err==nil{t.Fatal("released Volume descriptor remains visible")}
}

func TestDynamicVolumeAllocatorRejectsDuplicateVolumeIDAndCorruptBitmap(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	committer:=dynamicVolumeTestCommitter{dev}
	if _,err=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"duplicate"),committer);err!=nil{t.Fatal(err)}
	if _,err=a.AllocateVolume(1,dynamicVolumeDescriptor(sb,"duplicate"),committer);err==nil{t.Fatal("expected duplicate Volume ID rejection")}
	bitmapOff:=(a.header.RegionStart+2)*format.PageSize
	dev.b[bitmapOff]^=0x80
	if _,err=OpenDynamicVolumeAllocator(dev,sb);err==nil{t.Fatal("expected Reserved bitmap checksum rejection")}
}

func TestDynamicVolumeAllocatorRefusesNonEmptyReservedRegion(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	off:=sb.Geometry.ReservedOffset
	dev.b[off+10]=1
	if err:=FormatDynamicVolumeAllocator(dev,sb);err==nil{t.Fatal("expected non-empty Reserved region rejection")}
}

func TestDynamicVolumeAllocatorFormatCanResumeAfterMarker(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	start:=sb.Geometry.ReservedOffset/format.PageSize
	failed:=&dynamicVolumeFailDevice{Device:dev,failOffset:int64((start+2)*format.PageSize)}
	formatErr:=FormatDynamicVolumeAllocator(failed,sb)
	if formatErr==nil{t.Fatal("expected injected bitmap write failure")}
	if got:=binary.LittleEndian.Uint64(dev.b[start*format.PageSize:start*format.PageSize+8]);got!=dynamicReservedFormatMagic{
		t.Fatalf("format marker was not persisted: got=%x want=%x; format error=%v",got,dynamicReservedFormatMagic,formatErr)
	}
	if _,err:=OpenDynamicVolumeAllocator(dev,sb);!errors.Is(err,ErrDynamicVolumeAllocatorUnformatted){t.Fatalf("expected unformatted allocator while marker is present, got %v",err)}
	if err:=RecoverDynamicVolumeAllocatorFormat(dev,sb);err!=nil{t.Fatal(err)}
	if _,err:=OpenDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
}

func TestDynamicVolumeAllocatorGrowVolumeRelocatesLifecycle(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	committer:=dynamicVolumeTestCommitter{dev}
	d,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"grow-volume"),committer);if err!=nil{t.Fatal(err)}
	lifecycle:=make([]byte,d.LifecyclePages*format.PageSize)
	for i:=range lifecycle{lifecycle[i]=byte(i*13+5)}
	if _,err=dev.WriteAt(lifecycle,int64(d.LifecycleStart*format.PageSize));err!=nil{t.Fatal(err)}
	if err=dev.Sync();err!=nil{t.Fatal(err)}
	next,_,err:=a.GrowVolume(0,16,committer);if err!=nil{t.Fatal(err)}
	if next.ArenaPages!=d.ArenaPages+16||next.LifecycleStart!=d.LifecycleStart+16{t.Fatalf("unexpected grown descriptor: %+v",next)}
	got:=make([]byte,len(lifecycle))
	if _,err=dev.ReadAt(got,int64(next.LifecycleStart*format.PageSize));err!=nil{t.Fatal(err)}
	if string(got)!=string(lifecycle){t.Fatal("Lifecycle pages changed during Arena growth")}
	if _,err=OpenDynamicPoolVolume(dev,sb,0,"grow-volume");err!=nil{t.Fatalf("reopen grown Volume: %v",err)}
}

func TestDynamicVolumeAllocatorExpandRegionKeepsExistingAllocations(t *testing.T){
	oldDev,oldSB:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(oldDev,oldSB);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(oldDev,oldSB);if err!=nil{t.Fatal(err)}
	oldVolume,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(oldSB,"expand-region-volume"),dynamicVolumeTestCommitter{oldDev});if err!=nil{t.Fatal(err)}
	newMeta:=oldSB.Geometry.MetadataBytes+1<<20
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:oldSB.Geometry.DataBytes,MetadataBytes:newMeta,
		AGSizeBytes:oldSB.Geometry.AGSizeBlocks*format.BlockSize,WALSegmentBytes:oldSB.Geometry.WALSegmentBytes,
		MappingBytes:oldSB.Geometry.MappingSize,CheckpointBytes:oldSB.Geometry.CheckpointSize,AGCapacity:oldSB.Geometry.AGCapacity})
	if err!=nil{t.Fatal(err)}
	nextSB,err:=format.NewDynamicSuperblock(g,oldSB.PoolUUID,oldSB.Generation+1,oldSB.ViewGeneration+1);if err!=nil{t.Fatal(err)}
	newDev:=newMemdev(newMeta/PageSize)
	copy(newDev.b,oldDev.b)
	committer:=dynamicVolumeTestCommitter{newDev}
	if err=a.ExpandRegion(newDev,nextSB,committer);err!=nil{t.Fatal(err)}
	reopened,err:=OpenDynamicVolumeAllocator(newDev,nextSB);if err!=nil{t.Fatal(err)}
	nextVolume,err:=reopened.AllocateVolume(1,dynamicVolumeDescriptor(nextSB,"after-expand"),committer);if err!=nil{t.Fatal(err)}
	if nextVolume.ArenaStart<oldVolume.ArenaStart+oldVolume.ArenaPages+oldVolume.LifecyclePages{t.Fatalf("existing allocation overlapped: old=%+v new=%+v",oldVolume,nextVolume)}
}

func TestDynamicVolumeAllocatorReconcilesRegionAfterGeometryPublish(t *testing.T){
	oldDev,oldSB:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(oldDev,oldSB);err!=nil{t.Fatal(err)}
	newMeta:=oldSB.Geometry.MetadataBytes+1<<20
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:oldSB.Geometry.DataBytes,MetadataBytes:newMeta,
		AGSizeBytes:oldSB.Geometry.AGSizeBlocks*format.BlockSize,WALSegmentBytes:oldSB.Geometry.WALSegmentBytes,
		MappingBytes:oldSB.Geometry.MappingSize,CheckpointBytes:oldSB.Geometry.CheckpointSize,AGCapacity:oldSB.Geometry.AGCapacity})
	if err!=nil{t.Fatal(err)}
	nextSB,err:=format.NewDynamicSuperblock(g,oldSB.PoolUUID,oldSB.Generation+1,oldSB.ViewGeneration+1);if err!=nil{t.Fatal(err)}
	newDev:=newMemdev(newMeta/PageSize);copy(newDev.b,oldDev.b)
	// The Superblock is already newer but the allocator header still records
	// the old Reserved region size, as after a crash in the publication window.
	reopened,err:=OpenDynamicVolumeAllocator(newDev,nextSB);if err!=nil{t.Fatal(err)}
	if err=reopened.ReconcileRegion(dynamicArenaCommitter{newDev});err!=nil{t.Fatal(err)}
	if _,err=OpenDynamicVolumeAllocator(newDev,nextSB);err!=nil{t.Fatal(err)}
}

func TestStoreExpandDynamicArenaByUsesReservedAllocator(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	desc,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"store-grow"),dynamicArenaCommitter{dev});if err!=nil{t.Fatal(err)}
	store,err:=OpenDynamicPoolVolume(dev,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(store);err!=nil{t.Fatal(err)}
	if err=store.ExpandDynamicArenaBy(12,dynamicArenaCommitter{dev});err!=nil{t.Fatal(err)}
	got,err:=store.DynamicPoolDescriptor();if err!=nil{t.Fatal(err)}
	if got.ArenaPages!=desc.ArenaPages+12||got.LifecycleStart!=desc.LifecycleStart+12{t.Fatalf("Store Arena expansion mismatch: %+v",got)}
}

func TestDynamicOwnerEpochReaderReadsAuthoritativeVolumeDescriptor(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	d:=dynamicVolumeDescriptor(sb,"epoch-reader-volume");d.OwnerEpoch=42
	if _,err=a.AllocateVolume(0,d,dynamicArenaCommitter{dev});err!=nil{t.Fatal(err)}
	reader,err:=NewDynamicOwnerEpochReader(dev,sb,0,d.VolumeID);if err!=nil{t.Fatal(err)}
	epoch,err:=reader.ReadOwnerEpoch();if err!=nil{t.Fatal(err)}
	if epoch!=42{t.Fatalf("OwnerEpoch=%d want 42",epoch)}
	wrong,err:=NewDynamicOwnerEpochReader(dev,sb,0,"wrong-volume");if err!=nil{t.Fatal(err)}
	if _,err=wrong.ReadOwnerEpoch();err==nil{t.Fatal("expected mismatched Volume ID rejection")}
}

func TestDynamicVolumeSANOwnerRecordSurvivesArenaGrowth(t *testing.T){
	dev,sb:=dynamicVolumeAllocatorFixture(t)
	if err:=FormatDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
	a,err:=OpenDynamicVolumeAllocator(dev,sb);if err!=nil{t.Fatal(err)}
	d,err:=a.AllocateVolume(0,dynamicVolumeDescriptor(sb,"owner-record-volume"),dynamicArenaCommitter{dev});if err!=nil{t.Fatal(err)}
	if d.OwnerRecordStart==0{t.Fatal("allocator did not reserve OwnerRecord pages")}
	records,err:=NewDynamicSANOwnerRecordStore(dev,sb,0,d.VolumeID);if err!=nil{t.Fatal(err)}
	ctx:=context.Background()
	record:=volume.OwnerRecord{VolumeID:d.VolumeID,NodeID:"node-a",BootID:"boot-a",Epoch:1}
	if err=records.CompareAndSwap(ctx,d.VolumeID,nil,record);err!=nil{t.Fatal(err)}
	store,err:=OpenDynamicPoolVolume(dev,sb,0,d.VolumeID);if err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(store);err!=nil{t.Fatal(err)}
	if err=store.ExpandDynamicArenaBy(8,dynamicArenaCommitter{dev});err!=nil{t.Fatal(err)}
	recordsAfter,err:=NewDynamicSANOwnerRecordStore(dev,sb,0,d.VolumeID);if err!=nil{t.Fatal(err)}
	got,exists,err:=recordsAfter.Load(ctx,d.VolumeID);if err!=nil{t.Fatal(err)}
	if !exists||got!=record{t.Fatalf("OwnerRecord lost after Arena growth: exists=%v record=%+v",exists,got)}
}
