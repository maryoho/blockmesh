package format

import (
	"io"
	"testing"
)

type checkpointTestWriter struct{ data []byte }
func(w *checkpointTestWriter)WriteAt(p []byte,off int64)(int,error){
	if off<0||off+int64(len(p))>int64(len(w.data)){return 0,io.ErrShortWrite}
	return copy(w.data[int(off):],p),nil
}

func TestDynamicCheckpointRoundTripAndOffsets(t *testing.T){
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:9*BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	for ag:=uint32(0);ag<g.AGCount;ag++{
		a,err:=DynamicCheckpointOffset(g,ag,0);if err!=nil{t.Fatal(err)}
		b,err:=DynamicCheckpointOffset(g,ag,1);if err!=nil{t.Fatal(err)}
		if b-a!=PageSize{t.Fatal("checkpoint copies are not adjacent")}
	}
	c:=DynamicCheckpoint{AG:1,Generation:9,CheckpointTxID:88,ActiveBank:1}
	encoded,err:=c.Encode();if err!=nil{t.Fatal(err)}
	got,err:=DecodeDynamicCheckpoint(encoded,1);if err!=nil{t.Fatal(err)}
	if got!=c{t.Fatalf("checkpoint mismatch: %+v",got)}
	encoded[32]^=1
	if _,err=DecodeDynamicCheckpoint(encoded,1);err==nil{t.Fatal("expected CRC rejection")}
}

func TestInitializeDynamicCheckpoints(t *testing.T){
	g,err:=PlanDynamicGeometry(DynamicGeometryConfig{DataBytes:8*BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	w:=&checkpointTestWriter{data:make([]byte,g.MetadataBytes)}
	if err=InitializeDynamicCheckpoints(w,g);err!=nil{t.Fatal(err)}
	for ag:=uint32(0);ag<g.AGCount;ag++{
		for slot:=uint32(0);slot<2;slot++{
			off,e:=DynamicCheckpointOffset(g,ag,slot);if e!=nil{t.Fatal(e)}
			got,e:=DecodeDynamicCheckpoint(w.data[off:off+PageSize],ag);if e!=nil{t.Fatal(e)}
			if got.Generation!=1||got.ActiveBank!=0||got.CheckpointTxID!=0{t.Fatalf("unexpected initial checkpoint: %+v",got)}
		}
	}
}
