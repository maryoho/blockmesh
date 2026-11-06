package allocator

import (
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

func dynamicGeometryUpdateFixture(t *testing.T)(*dynamicFormatMem,format.DynamicSuperblock){
	t.Helper()
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:4*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:8*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"geometry-grow03")
	sb,err:=format.NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	dev:=newDynamicFormatMem(g.MetadataBytes)
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
	physical,_,err:=PlanDynamicBitmapAllocation(dev,next,0,2)
	if err!=nil{t.Fatal(err)}
	if physical!=4{t.Fatalf("newly exposed blocks start at %d, want 4",physical)}
}

func TestPublishDynamicGeometryRejectsNewAGWithoutMigration(t *testing.T){
	dev,old:=dynamicGeometryUpdateFixture(t)
	next,err:=BuildDynamicGeometryUpdate(old,old.Geometry.MetadataBytes,9*format.BlockSize,2)
	if err!=ErrDynamicGeometryRequiresMigration{t.Fatalf("expected migration requirement, got %v",err)}
	if err=PublishDynamicGeometry(dev,old,next,nil);err==nil{t.Fatal("invalid geometry published")}
}

type dynamicPageImageCommitter struct{dev *dynamicFormatMem}
func(c dynamicPageImageCommitter)CommitImages(images []metastore.PageImage)error{
	for _,image:=range images{if _,err:=c.dev.WriteAt(image.Data,int64(image.Offset));err!=nil{return err}}
	return c.dev.Sync()
}
