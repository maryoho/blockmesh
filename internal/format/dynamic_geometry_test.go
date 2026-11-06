package format

import (
	"errors"
	"testing"
)

func TestPlanDynamicGeometryPartialLastAG(t *testing.T) {
	data := uint64(8*1024*1024*1024 + 512*1024*1024)
	g, err := PlanDynamicGeometry(DynamicGeometryConfig{
		DataBytes:data, MetadataBytes:2<<30, AGSizeBytes:4<<30,
		WALSegmentBytes:16<<20, MappingBytes:256<<20, CheckpointBytes:64<<20,
	})
	if err != nil { t.Fatal(err) }
	if g.TotalBlocks != data/BlockSize || g.AGCount != 3 || g.LastAGBlocks != (512<<20)/BlockSize {
		t.Fatalf("unexpected AG geometry: %+v",g)
	}
	if got,err:=g.AGBlocks(2);err!=nil||got!=g.LastAGBlocks { t.Fatalf("last AG blocks=%d err=%v",got,err) }
	if start,err:=g.AGStartBlock(2);err!=nil||start!=2*g.AGSizeBlocks { t.Fatalf("last AG start=%d err=%v",start,err) }
	if g.BitmapBytesPerAG != (g.AGSizeBlocks+7)/8 || g.BitmapPaddedBytes%PageSize!=0 {
		t.Fatalf("invalid bitmap geometry: %+v",g)
	}
	if g.ReservedOffset>g.MetadataBytes { t.Fatal("metadata layout exceeds view") }
}

func TestPlanDynamicGeometrySmallDeviceAndNonPowerOfTwoAG(t *testing.T) {
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{
		DataBytes:6*BlockSize, MetadataBytes:64<<20, AGSizeBytes:4*BlockSize,
		WALSegmentBytes:PageSize, MappingBytes:8<<20, CheckpointBytes:PageSize,
	})
	if err!=nil { t.Fatal(err) }
	if g.AGCount!=2 || g.LastAGBlocks!=2 || g.BitmapStride!=2*PageSize {
		t.Fatalf("unexpected small geometry: %+v",g)
	}
}

func TestPlanDynamicGeometryRejectsInvalidAndInsufficientMetadata(t *testing.T) {
	base:=DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:4<<20,AGSizeBytes:4*BlockSize,
		WALSegmentBytes:PageSize,MappingBytes:2<<20,CheckpointBytes:PageSize}
	if _,err:=PlanDynamicGeometry(base);!errors.Is(err,ErrInvalidDynamicGeometry) { t.Fatalf("expected metadata capacity error, got %v",err) }
	base.MetadataBytes=8<<20
	base.DataBytes=BlockSize+1
	if _,err:=PlanDynamicGeometry(base);!errors.Is(err,ErrInvalidDynamicGeometry) { t.Fatalf("expected alignment error, got %v",err) }
}

func TestDynamicGeometryRejectsAGOutOfRange(t *testing.T) {
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*BlockSize,WALSegmentBytes:PageSize,MappingBytes:8<<20,CheckpointBytes:PageSize})
	if err!=nil { t.Fatal(err) }
	if _,err=g.AGBlocks(g.AGCount);!errors.Is(err,ErrInvalidDynamicGeometry) { t.Fatalf("expected range error, got %v",err) }
}
