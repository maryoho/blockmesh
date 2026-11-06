package format

import (
	"bytes"
	"testing"
)

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
	image:=make([]byte,g.MetadataBytes)
	if err=InitializeDynamicCheckpoints(bytes.NewReader(image),g);err==nil{t.Fatal("ReaderAt must not satisfy WriterAt")}
	type rw struct{*bytes.Reader; data []byte}
	_ = rw{}
}
