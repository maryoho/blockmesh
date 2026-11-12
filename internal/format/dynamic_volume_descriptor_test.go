package format

import (
	"io"
	"testing"
	"encoding/binary"
	"hash/crc32"
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
	want:=d
	want.OwnerRecordStart=d.LifecycleStart+d.LifecyclePages
	want.ReaderTableStart=want.OwnerRecordStart+OwnerRecordPages
	want.ReaderTablePages=uint32(ReaderTablePages)
	if a!=want||b!=want{t.Fatalf("descriptor copies differ: a=%+v b=%+v want=%+v",a,b,want)}
}

func TestDynamicVolumeDescriptorCatalogFieldsRoundTrip(t *testing.T) {
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,AGSizeBytes:4*BlockSize,
		WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"dynamic-volume03")
	sb,err:=NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	d:=DynamicVolumeDescriptor{VolumeID:"volume-03",VolumeName:"vm-disk-03",LogicalSizeBytes:4*BlockSize,
		CreateToken:[16]byte{1,2,3},AG:0,Generation:1,VolumeSequence:1,MappingRootOffset:g.MappingOffset,
		BitmapSequence:1,OwnerEpoch:1,ArenaStart:g.ReservedOffset/PageSize,ArenaPages:64,RootPage:^uint64(0),LifecyclePages:4}
	b,err:=d.Encode();if err!=nil{t.Fatal(err)}
	if got:=binary.LittleEndian.Uint32(b[8:12]);got!=DynamicVolumeDescriptorVersion{t.Fatalf("descriptor version=%d",got)}
	got,err:=DecodeDynamicVolumeDescriptor(b);if err!=nil{t.Fatal(err)}
	if got!=d{t.Fatalf("catalog fields mismatch: got=%+v want=%+v",got,d)}
	if err=got.ValidatePool(sb);err!=nil{t.Fatal(err)}
	got.VolumeName="bad/name"
	if _,err=got.Encode();err==nil{t.Fatal("unsafe Volume name accepted")}
	got=d;got.LogicalSizeBytes++
	if _,err=got.Encode();err==nil{t.Fatal("unaligned logical size accepted")}
}

func TestDecodeLegacyDynamicVolumeDescriptorV4(t *testing.T) {
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,AGSizeBytes:4*BlockSize,
		WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"dynamic-volume04")
	sb,err:=NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	d:=DynamicVolumeDescriptor{VolumeID:"legacy-v4",AG:0,Generation:1,VolumeSequence:1,MappingRootOffset:g.MappingOffset,
		BitmapSequence:1,OwnerEpoch:1,ArenaStart:g.ReservedOffset/PageSize,ArenaPages:64,RootPage:^uint64(0)}
	b,err:=d.Encode();if err!=nil{t.Fatal(err)}
	binary.LittleEndian.PutUint32(b[8:12],CurrentLegacyDynamicVolumeDescriptorVersion)
	for i:=188;i<256;i++{b[i]=0}
	for i:=256;i<280;i++{b[i]=0}
	binary.LittleEndian.PutUint32(b[dynamicVolumeDescriptorCRC:],crc32.Checksum(b[:dynamicVolumeDescriptorCRC],castagnoli))
	got,err:=DecodeDynamicVolumeDescriptor(b);if err!=nil{t.Fatal(err)}
	if got.VolumeID!=d.VolumeID||got.VolumeName!=""||got.LogicalSizeBytes!=0{t.Fatalf("legacy v4 descriptor decoded incorrectly: %+v",got)}
}
