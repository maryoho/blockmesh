package allocator

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"blockmesh/internal/format"
)

type bitmapMem struct{ pages map[int64][]byte }
func (m *bitmapMem) ReadAt(p []byte,off int64)(int,error){
	if off<0{return 0,io.EOF}
	for i:=range p {
		pos:=off+int64(i);pageOff:=pos/int64(format.PageSize)*int64(format.PageSize)
		if page:=m.pages[pageOff];page!=nil { p[i]=page[pos-pageOff] } else { p[i]=0 }
	}
	return len(p),nil
}

func TestPlanBitmapAllocationContiguousAndCrossPage(t *testing.T) {
	sb,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	m:=&bitmapMem{pages:make(map[int64][]byte)}
	ag:=uint32(0);base:=sb.BitmapOffset
	desc:=make([]byte,format.PageSize);binary.LittleEndian.PutUint64(desc[:8],bitmapDescriptorMagic);binary.LittleEndian.PutUint32(desc[8:12],ag)
	bitmap:=make([]byte,format.BitmapBytes)
	// Occupy all blocks before the final 3 of bitmap page zero, forcing
	// the requested run to cross the page boundary.
	for i:=uint64(0);i<uint64(format.PageSize)*8-1;i++ { bitmap[i/8]|=byte(1<<(i%8)) }
	binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
	m.pages[int64(base)]=desc
	for i:=uint64(0);i<uint64(format.BitmapPages);i++ {
		page:=make([]byte,format.PageSize)
		copy(page,bitmap[i*format.PageSize:(i+1)*format.PageSize])
		m.pages[int64(base+format.PageSize+i*format.PageSize)]=page
	}
	physical,images,err:=PlanBitmapAllocation(m,sb,ag,3);if err!=nil{t.Fatal(err)}
	if physical!=uint64(format.PageSize)*8-1 { t.Fatalf("physical=%d",physical) }
	if len(images)!=3 { t.Fatalf("images=%d, want 2 bitmap pages + descriptor",len(images)) }
	if binary.LittleEndian.Uint64(images[len(images)-1].Data[16:24])!=1 { t.Fatal("bitmap sequence not incremented") }
}

func TestPlanBitmapAllocationRejectsCorruptDescriptor(t *testing.T) {
	sb,_:=format.NewSuperblock(0)
	m:=&bitmapMem{pages:make(map[int64][]byte)}
	if _,_,err:=PlanBitmapAllocation(m,sb,0,1);err==nil { t.Fatal("expected invalid descriptor") }
}
