package metastore

import (
	"context"
	"errors"
	"testing"

	"blockmesh/internal/format"
)

type dynamicVolumeTestCommitter struct{dev Device}
func(c dynamicVolumeTestCommitter)CommitImages(images []PageImage)error{
	for _,image:=range images{if err:=writeFullAt(c.dev,image.Data,int64(image.Offset));err!=nil{return err}}
	return c.dev.Sync()
}
func(dynamicVolumeTestCommitter)ClusterSafe()bool{return true}

type dynamicVolumeTestFence struct{err error}
func(f dynamicVolumeTestFence)ProveVolumeFenced(_ context.Context,_ string,_ uint64)error{return f.err}

type dynamicVolumeFailDevice struct{Device; failOffset int64; failed bool}
func(d *dynamicVolumeFailDevice)WriteAt(p []byte,off int64)(int,error){
	if off==d.failOffset&&!d.failed{d.failed=true;return 0,errors.New("injected Reserved allocator write failure")}
	return d.Device.WriteAt(p,off)
}
func(d *dynamicVolumeFailDevice)Size()int64{if s,ok:=d.Device.(interface{Size()int64});ok{return s.Size()};return 0}

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
	if err:=FormatDynamicVolumeAllocator(failed,sb);err==nil{t.Fatal("expected injected bitmap write failure")}
	if got:=binary.LittleEndian.Uint64(dev.b[start*format.PageSize:start*format.PageSize+8]);got!=dynamicReservedFormatMagic{
		t.Fatalf("format marker was not persisted: got=%x want=%x",got,dynamicReservedFormatMagic)
	}
	if _,err:=OpenDynamicVolumeAllocator(dev,sb);!errors.Is(err,ErrDynamicVolumeAllocatorUnformatted){t.Fatalf("expected unformatted allocator while marker is present, got %v",err)}
	if err:=RecoverDynamicVolumeAllocatorFormat(dev,sb);err!=nil{t.Fatal(err)}
	if _,err:=OpenDynamicVolumeAllocator(dev,sb);err!=nil{t.Fatal(err)}
}
