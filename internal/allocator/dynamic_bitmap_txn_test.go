package allocator

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"blockmesh/internal/format"
)

func dynamicBitmapFixture(t *testing.T) (format.DynamicSuperblock,*bytes.Reader) {
	t.Helper()
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{
		DataBytes:10*format.BlockSize,MetadataBytes:64<<20,AGSizeBytes:4*format.BlockSize,
		WALSegmentBytes:format.PageSize,MappingBytes:8<<20,CheckpointBytes:format.PageSize,
	})
	if err!=nil{t.Fatal(err)}
	var uuid [16]byte;copy(uuid[:],"dynamic-bitmap01")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil{t.Fatal(err)}
	image:=make([]byte,g.BitmapOffset+g.BitmapSize)
	for ag:=uint32(0);ag<g.AGCount;ag++{
		base:=g.BitmapOffset+uint64(ag)*g.BitmapStride
		desc:=image[base:base+format.PageSize]
		binary.LittleEndian.PutUint64(desc[:8],bitmapDescriptorMagic)
		binary.LittleEndian.PutUint32(desc[8:12],ag)
		bitmap:=image[base+format.PageSize:base+g.BitmapStride]
		binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
	}
	return sb,bytes.NewReader(image)
}

func TestPlanDynamicBitmapAllocationUsesPartialLastAG(t *testing.T){
	sb,r:=dynamicBitmapFixture(t)
	g:=sb.Geometry
	physical,images,err:=PlanDynamicBitmapAllocation(r,sb,g.AGCount-1,2)
	if err!=nil{t.Fatal(err)}
	want:=uint64(g.AGCount-1)*g.AGSizeBlocks
	if physical!=want{t.Fatalf("physical block=%d want=%d",physical,want)}
	if len(images)!=2{t.Fatalf("expected bitmap page and descriptor, got %d images",len(images))}
	if images[0].Offset!=g.BitmapOffset+uint64(g.AGCount-1)*g.BitmapStride+format.PageSize{
		t.Fatalf("bitmap page offset mismatch: %d",images[0].Offset)
	}
}

func TestPlanDynamicBitmapAllocationRejectsBeyondPartialAG(t *testing.T){
	sb,r:=dynamicBitmapFixture(t)
	if _,_,err:=PlanDynamicBitmapAllocation(r,sb,sb.Geometry.AGCount-1,3);err==nil{
		t.Fatal("expected request larger than final AG to fail")
	}
}

func TestPlanDynamicBitmapAllocationRejectsCorruptDescriptor(t *testing.T){
	sb,_:=dynamicBitmapFixture(t)
	image:=make([]byte,sb.Geometry.BitmapOffset+sb.Geometry.BitmapSize)
	if _,_,err:=PlanDynamicBitmapAllocation(bytes.NewReader(image),sb,0,1);err==nil{
		t.Fatal("expected invalid descriptor rejection")
	}
}
