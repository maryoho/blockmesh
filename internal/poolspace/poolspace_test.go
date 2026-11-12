package poolspace

import (
	"bytes"
	"testing"
)

func TestPlanDeviceScalesAndAligns(t *testing.T){
	small,err:=PlanDevice(2<<40,64);if err!=nil{t.Fatal(err)}
	large,err:=PlanDevice(100<<40,64);if err!=nil{t.Fatal(err)}
	if small.InitialMetadata%ExtentSize!=0||large.InitialMetadata%ExtentSize!=0{t.Fatal("metadata plan is not extent aligned")}
	if large.InitialMetadata<=small.InitialMetadata||large.DataCapacity<=small.DataCapacity{t.Fatalf("capacity did not scale: small=%+v large=%+v",small,large)}
	if small.InitialMetadata>=small.DeviceBytes||small.DataCapacity==0{t.Fatal("invalid plan")}
}
func TestMetadataExpansionTargetIsAutomatic(t *testing.T){
	got,err:=MetadataExpansionTarget(2*ExtentSize,1500<<20,2<<30,ExtentSize);if err!=nil{t.Fatal(err)}
	if got<=2*ExtentSize||got%ExtentSize!=0{t.Fatalf("unexpected target %d",got)}
}
func TestAllocatorRescanAndMetadataExpansionNeverOverlapData(t *testing.T){
	a,err:=NewAllocator(8*ExtentSize,ExtentSize,2*ExtentSize);if err!=nil{t.Fatal(err)}
	data,err:=a.AllocateData(2*ExtentSize);if err!=nil{t.Fatal(err)}
	if err=a.Rescan(12*ExtentSize);err!=nil{t.Fatal(err)}
	meta,err:=a.AllocateMetadata(2*ExtentSize);if err!=nil{t.Fatal(err)}
	for _,m:=range meta{for _,d:=range data{if m.Start<d.Start+d.Length&&d.Start<m.Start+m.Length{t.Fatal("metadata overlaps existing data")}}}
	if got:=a.DeviceBytes();got!=12*ExtentSize{t.Fatalf("device bytes=%d",got)}
}
func TestExtentMapRoundTripAndCorruption(t *testing.T){
	m:=ExtentMap{Generation:3,DeviceBytes:4*ExtentSize,Extents:[]Extent{{0,ExtentSize,Metadata},{ExtentSize,2*ExtentSize,Data},{3*ExtentSize,ExtentSize,Free}}}
	b,err:=m.Encode();if err!=nil{t.Fatal(err)}
	got,err:=DecodeExtentMap(b);if err!=nil{t.Fatal(err)}
	if got.Generation!=m.Generation||len(got.Extents)!=len(m.Extents){t.Fatalf("round trip mismatch: %+v",got)}
	b[len(b)-1]^=1
	if _,err:=DecodeExtentMap(b);err==nil{t.Fatal("expected checksum failure")}
	if _,err:=m.Encode();err!=nil||bytes.Equal(b,mustEncode(t,m)){t.Fatal("test corruption setup invalid")}
}
func mustEncode(t *testing.T,m ExtentMap)[]byte{t.Helper();b,e:=m.Encode();if e!=nil{t.Fatal(e)};return b}

func TestFailedAllocationDoesNotConsumePartialFreeSpace(t *testing.T) {
 a,err:=NewAllocator(3*ExtentSize,ExtentSize,ExtentSize);if err!=nil{t.Fatal(err)}
 before:=a.Extents()
 if _,err=a.AllocateMetadata(2*ExtentSize);err==nil{t.Fatal("expected insufficient space")}
 after:=a.Extents()
 if len(before)!=len(after){t.Fatalf("extent count changed on failed allocation: before=%v after=%v",before,after)}
 for i:=range before{if before[i]!=after[i]{t.Fatalf("allocator mutated on failed request: before=%v after=%v",before,after)}}
}

func TestDataExpansionTargetUsesForecastAndDoesNotGrowWhenSufficient(t *testing.T) {
	current:=uint64(10*AllocationSize)
	got,err:=DataExpansionTarget(current,8*AllocationSize,2*AllocationSize,AllocationSize);if err!=nil{t.Fatal(err)}
	if got<=current||got%AllocationSize!=0{t.Fatalf("expected aligned Data growth, got %d",got)}
	noGrowth,err:=DataExpansionTarget(current,4*AllocationSize,AllocationSize,AllocationSize);if err!=nil{t.Fatal(err)}
	if noGrowth!=current{t.Fatalf("sufficient Data capacity grew unexpectedly: got=%d current=%d",noGrowth,current)}
}

func TestDataExpansionTargetRejectsOverflowAndInvalidUsage(t *testing.T) {
	if _,err:=DataExpansionTarget(4*AllocationSize,5*AllocationSize,0,AllocationSize);err==nil{t.Fatal("expected usage greater than capacity rejection")}
	if _,err:=DataExpansionTarget(4*AllocationSize,AllocationSize,^uint64(0),AllocationSize);err==nil{t.Fatal("expected forecast overflow rejection")}
}