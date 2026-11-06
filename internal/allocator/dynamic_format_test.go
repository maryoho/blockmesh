package allocator

import (
	"errors"
	"io"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

type dynamicFormatMem struct{ b []byte; failWriteAt int64; failed bool }
func newDynamicFormatMem(n uint64)*dynamicFormatMem{return &dynamicFormatMem{b:make([]byte,n),failWriteAt:-1}}
func(m *dynamicFormatMem)ReadAt(p []byte,off int64)(int,error){if off<0||off>=int64(len(m.b)){return 0,io.EOF};n:=copy(p,m.b[off:]);if n<len(p){return n,io.EOF};return n,nil}
func(m *dynamicFormatMem)WriteAt(p []byte,off int64)(int,error){if m.failWriteAt>=0&&off==m.failWriteAt&&!m.failed{m.failed=true;return 0,errors.New("injected write failure")};if off<0||off+int64(len(p))>int64(len(m.b)){return 0,io.ErrShortWrite};return copy(m.b[off:],p),nil}
func(m *dynamicFormatMem)Sync()error{return nil}
func(m *dynamicFormatMem)Size()int64{return int64(len(m.b))}
func(m *dynamicFormatMem)Close()error{return nil}
var _ device.Device=(*dynamicFormatMem)(nil)

func dynamicFormatFixture(t *testing.T)(*dynamicFormatMem,format.DynamicSuperblock){
	t.Helper()
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:10*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:format.PageSize,MappingBytes:8<<20,CheckpointBytes:format.PageSize})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"bm-pool-format03")
	sb,err:=format.NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	return newDynamicFormatMem(g.MetadataBytes),sb
}

func TestFormatDynamicPoolInitializesAndPublishes(t *testing.T){
	dev,sb:=dynamicFormatFixture(t)
	if err:=FormatDynamicPool(dev,sb);err!=nil{t.Fatal(err)}
	got,err:=OpenDynamicSuperblockPair(dev);if err!=nil{t.Fatal(err)}
	if got!=sb{t.Fatalf("superblock mismatch: got=%+v want=%+v",got,sb)}
	for ag:=uint32(0);ag<sb.Geometry.AGCount;ag++{
		base:=sb.Geometry.BitmapOffset+uint64(ag)*sb.Geometry.BitmapStride
		d:=make([]byte,format.PageSize);if _,err=dev.ReadAt(d,int64(base));err!=nil{t.Fatal(err)}
		if binary.LittleEndian.Uint64(d[:8])!=bitmapDescriptorMagic||binary.LittleEndian.Uint32(d[8:12])!=ag{t.Fatalf("AG %d descriptor invalid",ag)}
		bm:=make([]byte,sb.Geometry.BitmapPaddedBytes);if _,err=dev.ReadAt(bm,int64(base+format.PageSize));err!=nil{t.Fatal(err)}
		if crc32.Checksum(bm,castagnoli)!=binary.LittleEndian.Uint32(d[24:28]){t.Fatalf("AG %d bitmap CRC mismatch",ag)}
		valid,_:=sb.Geometry.AGBlocks(ag)
		for bit:=valid;bit<uint64(len(bm))*8;bit++{if bm[bit/8]&(1<<uint(bit%8))==0{t.Fatalf("AG %d padding bit %d is allocatable",ag,bit)}}
	}
}

func TestFormatDynamicPoolRefusesNonBlankDevice(t *testing.T){
	dev,sb:=dynamicFormatFixture(t)
	dev.b[format.PageSize+10]=1
	if err:=FormatDynamicPool(dev,sb);err==nil{t.Fatal("expected nonblank device rejection")}
}

func TestFormatDynamicPoolCanRecoverInterruptedInitialization(t *testing.T){
	dev,sb:=dynamicFormatFixture(t)
	dev.failWriteAt=int64(sb.Geometry.BitmapOffset+format.PageSize)
	if err:=FormatDynamicPool(dev,sb);err==nil{t.Fatal("expected injected bitmap write failure")}
	if _,err:=OpenDynamicSuperblockPair(dev);!errors.Is(err,ErrDynamicFormatIncomplete){t.Fatalf("expected incomplete format, got %v",err)}
	if err:=RecoverDynamicFormat(dev,sb);err!=nil{t.Fatal(err)}
	if _,err:=OpenDynamicSuperblockPair(dev);err!=nil{t.Fatal(err)}
}

func TestRecoverDynamicFormatRejectsForeignGeometry(t *testing.T){
	dev,sb:=dynamicFormatFixture(t)
	dev.failWriteAt=int64(sb.Geometry.BitmapOffset+format.PageSize)
	_ = FormatDynamicPool(dev,sb)
	other:=sb;other.ViewGeneration++
	if err:=RecoverDynamicFormat(dev,other);err==nil{t.Fatal("expected foreign geometry rejection")}
}
