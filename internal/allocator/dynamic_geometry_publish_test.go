package allocator

import (
	"testing"\n\t"encoding/binary"

	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

func dynamicGeometryUpdateFixture(t *testing.T)(*dynamicFormatMem,format.DynamicSuperblock){
	t.Helper()
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:4*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:8*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10,AGCapacity:4})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"geometry-grow03")
	sb,err:=format.NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	dev:=newDynamicFormatMem(g.MetadataBytes+16<<20)
	if err=FormatDynamicPool(dev,sb);err!=nil{t.Fatal(err)}
	return dev,sb
}

func TestBuildDynamicGeometryUpdateKeepsMetadataOffsets(t *testing.T){
	_,old:=dynamicGeometryUpdateFixture(t)
	next,err:=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes+8<<20,6*format.BlockSize,2)
	if err!=nil{t.Fatal(err)}
	if next.Geometry.AGCount!=old.Geometry.AGCount||next.Geometry.LastAGBlocks!=6||next.Geometry.MappingOffset!=old.Geometry.MappingOffset||
		next.Geometry.ReservedOffset!=old.Geometry.ReservedOffset{t.Fatalf("unexpected geometry update: %+v",next.Geometry)}
	if _,err=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes,9*format.BlockSize,2);err!=ErrDynamicGeometryRequiresMigration{
		t.Fatalf("expected AG-count migration requirement, got %v",err)
	}
}

func TestPublishDynamicGeometryExpandsLastAGBitmapAndDualSuperblock(t *testing.T){
	dev,old:=dynamicGeometryUpdateFixture(t)
	next,err:=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes+4<<20,6*format.BlockSize,2)
	if err!=nil{t.Fatal(err)}
	images,err:=PlanDynamicBitmapGrow(dev,old,next);if err!=nil{t.Fatal(err)}
	if len(images)<2{t.Fatalf("expected bitmap pages and descriptor, got %d images",len(images))}
	committer:=dynamicPageImageCommitter{dev:dev}
	if err=PublishDynamicGeometry(dev,old,next,committer);err!=nil{t.Fatal(err)}
	got,err:=OpenDynamicSuperblockPair(dev);if err!=nil{t.Fatal(err)}
	if got!=next{t.Fatalf("opened superblock=%+v want=%+v",got,next)}
	bitmap:=make([]byte,format.PageSize)
	if _,err=dev.ReadAt(bitmap,int64(next.Geometry.BitmapOffset+format.PageSize));err!=nil{t.Fatal(err)}
	for bit:=uint64(4);bit<6;bit++{if bitmap[bit/8]&(1<<uint(bit%8))!=0{t.Fatalf("newly exposed bit %d remains allocated",bit)}}
	for bit:=uint64(6);bit<8;bit++{if bitmap[bit/8]&(1<<uint(bit%8))==0{t.Fatalf("still-invalid padding bit %d became free",bit)}}
}

func TestBuildDynamicGeometryUpdateRejectsAGBeyondReservedCapacity(t *testing.T){
	_,old:=dynamicGeometryUpdateFixture(t)
	// The fixture reserves four AG slots. Activating a fifth AG must fail
	// before any metadata is modified.
	_,err:=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes,33*format.BlockSize,2)
	if err==nil{t.Fatal("expected growth beyond AGCapacity to be rejected")}
}

type dynamicPageImageCommitter struct{dev *dynamicFormatMem}
func(c dynamicPageImageCommitter)CommitImages(images []metastore.PageImage)error{
	for _,image:=range images{if _,err:=c.dev.WriteAt(image.Data,int64(image.Offset));err!=nil{return err}}
	return c.dev.Sync()
}

func TestDynamicGeometryGrowthActivatesReservedAG(t *testing.T) {
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:4*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:2*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10,AGCapacity:4})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"ag-capacity03")
	old,err:=format.NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	dev:=newDynamicFormatMem(g.MetadataBytes)
	if err=FormatDynamicPool(dev,old);err!=nil{t.Fatal(err)}
	next,err:=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes,6*format.BlockSize,2);if err!=nil{t.Fatal(err)}
	if next.Geometry.AGCount!=3||next.Geometry.AGCapacity!=4||next.Geometry.WALOffset!=old.Geometry.WALOffset||next.Geometry.BitmapOffset!=old.Geometry.BitmapOffset{t.Fatalf("bad AG growth geometry: %+v",next.Geometry)}
	if err=PublishDynamicGeometry(dev,old,next,dynamicPageImageCommitter{dev:dev});err!=nil{t.Fatal(err)}
	got,err:=OpenDynamicSuperblockPair(dev);if err!=nil{t.Fatal(err)}
	if got!=next{t.Fatalf("published=%+v want=%+v",got,next)}
	for ag:=uint32(0);ag<next.Geometry.AGCount;ag++{
		off:=next.Geometry.BitmapOffset+uint64(ag)*next.Geometry.BitmapStride
		desc:=make([]byte,format.PageSize);if _,err=dev.ReadAt(desc,int64(off));err!=nil{t.Fatal(err)}
		if binary.LittleEndian.Uint32(desc[8:12])!=ag{t.Fatalf("AG %d descriptor not initialized",ag)}
	}
}
