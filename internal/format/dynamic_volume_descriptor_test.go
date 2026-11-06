package format

import (
	"io"
	"testing"
)

type dynamicDescriptorWriter struct{b []byte}
func(w *dynamicDescriptorWriter)WriteAt(p []byte,off int64)(int,error){if off<0||off+int64(len(p))>int64(len(w.b)){return 0,io.ErrShortWrite};return copy(w.b[int(off):],p),nil}

func TestDynamicVolumeDescriptorRoundTripAndValidation(t *testing.T){
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,AGSizeBytes:4*BlockSize,
		WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"dynamic-volume01")
	sb,err:=NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	arenaStart:=g.ReservedOffset/PageSize
	d:=DynamicVolumeDescriptor{VolumeID:"vm-01",AG:1,Generation:1,VolumeSequence:1,MappingRootOffset:g.MappingOffset,
		BitmapSequence:1,OwnerEpoch:7,ArenaStart:arenaStart,ArenaPages:128,RootPage:^uint64(0),
		LifecycleStart:arenaStart+128,LifecyclePages:8}
	b,err:=d.Encode();if err!=nil{t.Fatal(err)}
	got,err:=DecodeDynamicVolumeDescriptor(b);if err!=nil{t.Fatal(err)}
	if got!=d{t.Fatalf("descriptor mismatch: %+v",got)}
	if err=got.ValidatePool(sb);err!=nil{t.Fatal(err)}
	got.AG=sb.Geometry.AGCount
	if err=got.ValidatePool(sb);err==nil{t.Fatal("invalid AG accepted")}
}

func TestInitializeDynamicVolumeDescriptorCopies(t *testing.T){
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,AGSizeBytes:4*BlockSize,
		WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"dynamic-volume02")
	sb,err:=NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	start:=g.ReservedOffset/PageSize
	d:=DynamicVolumeDescriptor{VolumeID:"volume-02",AG:0,Generation:1,VolumeSequence:1,MappingRootOffset:g.MappingOffset,
		BitmapSequence:1,OwnerEpoch:1,ArenaStart:start,ArenaPages:128,RootPage:^uint64(0),LifecycleStart:start+128,LifecyclePages:8}
	w:=&dynamicDescriptorWriter{b:make([]byte,g.MetadataBytes)}
	if err=InitializeDynamicVolumeDescriptor(w,sb,2,d);err!=nil{t.Fatal(err)}
	pair,err:=DynamicVolumeDescriptorPairOffset(sb,2);if err!=nil{t.Fatal(err)}
	a,err:=DecodeDynamicVolumeDescriptor(w.b[pair:pair+PageSize]);if err!=nil{t.Fatal(err)}
	b,err:=DecodeDynamicVolumeDescriptor(w.b[pair+PageSize:pair+2*PageSize]);if err!=nil{t.Fatal(err)}
	if a!=d||b!=d{t.Fatal("descriptor copies differ")}
}
