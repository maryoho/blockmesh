package format

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func testDynamicSuperblock(t *testing.T) DynamicSuperblock {
	t.Helper()
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:10*BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*BlockSize,WALSegmentBytes:PageSize,MappingBytes:8<<20,CheckpointBytes:PageSize})
	if err!=nil{t.Fatal(err)}
	var id [16]byte
	copy(id[:],"blockmesh-pool03")
	s,err:=NewDynamicSuperblock(g,id,7,11)
	if err!=nil{t.Fatal(err)}
	return s
}

func TestDynamicSuperblockRoundTrip(t *testing.T) {
	s:=testDynamicSuperblock(t)
	b,err:=s.Encode();if err!=nil{t.Fatal(err)}
	got,err:=DecodeDynamicSuperblock(b);if err!=nil{t.Fatal(err)}
	if got!=s { t.Fatalf("round trip mismatch\nwant=%+v\ngot=%+v",s,got) }
}

func TestDynamicSuperblockRejectsCorruptionAndLegacyFormat(t *testing.T) {
	s:=testDynamicSuperblock(t)
	b,err:=s.Encode();if err!=nil{t.Fatal(err)}
	corrupt:=append([]byte(nil),b...);corrupt[88]^=1
	if _,err:=DecodeDynamicSuperblock(corrupt);err==nil{t.Fatal("expected CRC failure")}
	legacy:=make([]byte,PageSize)
	if _,err:=DecodeDynamicSuperblock(legacy);err==nil{t.Fatal("expected BMPOOL02/unknown magic rejection")}
	if _,err:=DecodeDynamicSuperblock(bytes.Repeat([]byte{0},int(PageSize)-1));err==nil{t.Fatal("expected short page rejection")}
}

func TestDynamicSuperblockRejectsGeometryTamperingWithValidCRC(t *testing.T) {
	s:=testDynamicSuperblock(t)
	b,err:=s.Encode();if err!=nil{t.Fatal(err)}
	// Increase the stored AG count, then recompute the checksum. Validation
	// must detect the geometry mismatch independently of the CRC.
	binaryPut32(b[80:84],99)
	binaryPut32(b[DynamicSuperCRCOff:DynamicSuperCRCOff+4],0)
	binaryPut32(b[DynamicSuperCRCOff:DynamicSuperCRCOff+4],crc32Checksum(b))
	if _,err:=DecodeDynamicSuperblock(b);err==nil{t.Fatal("expected inconsistent geometry rejection")}
}

func binaryPut32(b []byte,v uint32){b[0]=byte(v);b[1]=byte(v>>8);b[2]=byte(v>>16);b[3]=byte(v>>24)}
func crc32Checksum(b []byte)uint32{return crc32.Checksum(b,castagnoli)}
