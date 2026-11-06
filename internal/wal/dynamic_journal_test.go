package wal

import (
	"errors"
	"io"
	"testing"

	"blockmesh/internal/format"
)

type dynamicWALDevice struct{b []byte; failAt int64; failed bool}
func newDynamicWALDevice(n uint64)*dynamicWALDevice{return &dynamicWALDevice{b:make([]byte,n),failAt:-1}}
func(d *dynamicWALDevice)ReadAt(p []byte,off int64)(int,error){if off<0||off>=int64(len(d.b)){return 0,io.EOF};n:=copy(p,d.b[off:]);if n<len(p){return n,io.EOF};return n,nil}
func(d *dynamicWALDevice)WriteAt(p []byte,off int64)(int,error){if d.failAt>=0&&off==d.failAt&&!d.failed{d.failed=true;return 0,errors.New("injected write failure")};if off<0||off+int64(len(p))>int64(len(d.b)){return 0,io.ErrShortWrite};return copy(d.b[off:],p),nil}
func(d *dynamicWALDevice)Sync()error{return nil}
func(d *dynamicWALDevice)Size()int64{return int64(len(d.b))}
func(d *dynamicWALDevice)Close()error{return nil}

func dynamicWALFixture(t *testing.T)(*dynamicWALDevice,format.DynamicSuperblock){
	t.Helper()
	g,e:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:12*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if e!=nil{t.Fatal(e)}
	var id [16]byte;copy(id[:],"dynamic-wal-001")
	sb,e:=format.NewDynamicSuperblock(g,id,1,1);if e!=nil{t.Fatal(e)}
	dev:=newDynamicWALDevice(g.MetadataBytes)
	if err=format.InitializeDynamicCheckpoints(dev,g);err!=nil{t.Fatal(err)}\n\treturn dev,sb
}

func TestDynamicJournalUsesDynamicSegmentAndAGCount(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	for ag:=uint32(0);ag<sb.Geometry.AGCount;ag++{
		j,e:=OpenDynamicJournal(dev,sb,ag);if e!=nil{t.Fatal(e)}
		if j.CapacityBytes()!=sb.Geometry.WALSegmentBytes/2||j.AG()!=ag{t.Fatalf("wrong AG journal geometry")}
	}
}

func TestDynamicJournalCommitAndRecovery(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	j,e:=OpenDynamicJournal(dev,sb,2);if e!=nil{t.Fatal(e)}
	off:=sb.Geometry.MappingOffset
	image:=make([]byte,format.PageSize);copy(image,"mapping-redo")
	if e=j.Commit([]PageUpdate{{Offset:off,Image:image}});e!=nil{t.Fatal(e)}
	for i:=range dev.b[int(off):int(off+format.PageSize)]{dev.b[int(off)+i]=0}
	j2,e:=OpenDynamicJournal(dev,sb,2);if e!=nil{t.Fatal(e)}
	got:=make([]byte,format.PageSize);if _,e=dev.ReadAt(got,int64(off));e!=nil{t.Fatal(e)}
	if string(got[:12])!="mapping-redo"{t.Fatalf("redo not replayed: %q",got[:12])}
	if j2.UsedBytes()==0{t.Fatal("expected journal tail to be recovered")}
}

func TestDynamicJournalRejectsCrossSegmentAndUnalignedUpdates(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	j,e:=OpenDynamicJournal(dev,sb,0);if e!=nil{t.Fatal(e)}
	if e=j.Commit([]PageUpdate{{Offset:sb.Geometry.WALOffset,Image:make([]byte,format.PageSize)}});e==nil{t.Fatal("expected WAL self-overwrite rejection")}
	if e=j.Commit([]PageUpdate{{Offset:sb.Geometry.MappingOffset+1,Image:make([]byte,format.PageSize)}});e==nil{t.Fatal("expected unaligned update rejection")}
}

func TestDynamicJournalLeavesUncommittedIntentUnapplied(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	j,e:=OpenDynamicJournal(dev,sb,1);if e!=nil{t.Fatal(e)}
	payload,_:=encodeUpdates([]PageUpdate{{Offset:sb.Geometry.MappingOffset,Image:make([]byte,format.PageSize)}})
	record,_:=encodeJournalRecord(journalIntent,1,payload)
	copy(dev.b[j.base:],record[:len(record)-5])
	j2,e:=OpenDynamicJournal(dev,sb,1);if e!=nil{t.Fatal(e)}
	if j2.UsedBytes()==0{t.Fatal("expected durable Intent to remain in append-only log")}; page:=make([]byte,format.PageSize);if _,e=dev.ReadAt(page,int64(sb.Geometry.MappingOffset));e!=nil{t.Fatal(e)};if !allZero(page){t.Fatal("uncommitted Intent was incorrectly applied")}
}

func TestDynamicJournalCheckpointRotatesAndReopens(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	j,err:=OpenDynamicJournal(dev,sb,0);if err!=nil{t.Fatal(err)}
	image:=make([]byte,format.PageSize);copy(image,"checkpointed")
	if err=j.Commit([]PageUpdate{{Offset:sb.Geometry.MappingOffset,Image:image}});err!=nil{t.Fatal(err)}
	if err=j.Checkpoint();err!=nil{t.Fatal(err)}
	if j.UsedBytes()!=0{t.Fatalf("expected empty active bank, got %d",j.UsedBytes())}
	j2,err:=OpenDynamicJournal(dev,sb,0);if err!=nil{t.Fatal(err)}
	if j2.UsedBytes()!=0{t.Fatal("reopened checkpoint bank is not empty")}
	cp0off,_:=format.DynamicCheckpointOffset(sb.Geometry,0,0)
	cp1off,_:=format.DynamicCheckpointOffset(sb.Geometry,0,1)
	cp0,_:=format.DecodeDynamicCheckpoint(dev.b[cp0off:cp0off+format.PageSize],0)
	cp1,_:=format.DecodeDynamicCheckpoint(dev.b[cp1off:cp1off+format.PageSize],0)
	if cp0!=cp1||cp0.Generation!=2||cp0.CheckpointTxID!=1||cp0.ActiveBank!=1{t.Fatalf("checkpoint copies not mirrored: %+v %+v",cp0,cp1)}
}
