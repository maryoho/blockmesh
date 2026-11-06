package allocator

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"blockmesh/internal/format"
)

type bitmapMem struct{ b []byte }
func (m *bitmapMem) ReadAt(p []byte,off int64)(int,error){if off<0||off>=int64(len(m.b)){return 0,io.EOF};n:=copy(p,m.b[off:]);if n<len(p){return n,io.EOF};return n,nil}

func TestPlanBitmapAllocationContiguousAndCrossPage(t *testing.T) {
	sb,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	m:=&bitmapMem{b:make([]byte,format.MetadataSize)}
	ag:=uint32(0);base:=sb.BitmapOffset
	desc:=make([]byte,format.PageSize);binary.LittleEndian.PutUint64(desc[:8],bitmapDescriptorMagic);binary.LittleEndian.PutUint32(desc[8:12],ag)
	bitmap:=make([]byte,format.BitmapBytes)
	// Occupy all blocks before the final 3 of bitmap page zero, forcing
	// the requested run to cross the page boundary.
	for i:=uint64(0);i<uint64(format.PageSize)*8-1;i++ { bitmap[i/8]|=byte(1<<(i%8)) }
	binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
	copy(m.b[base:],desc);copy(m.b[base+format.PageSize:],bitmap)
	physical,images,err:=PlanBitmapAllocation(m,sb,ag,3);if err!=nil{t.Fatal(err)}
	if physical!=uint64(format.PageSize)*8-1 { t.Fatalf("physical=%d",physical) }
	if len(images)!=3 { t.Fatalf("images=%d, want 2 bitmap pages + descriptor",len(images)) }
	if binary.LittleEndian.Uint64(images[len(images)-1].Data[16:24])!=1 { t.Fatal("bitmap sequence not incremented") }
}

func TestPlanBitmapAllocationRejectsCorruptDescriptor(t *testing.T) {
	sb,_:=format.NewSuperblock(0)
	m:=&bitmapMem{b:make([]byte,format.MetadataSize)}
	if _,_,err:=PlanBitmapAllocation(m,sb,0,1);err==nil { t.Fatal("expected invalid descriptor") }
}
