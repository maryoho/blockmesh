package uapi

import (
	"encoding/binary"
	"testing"
)
func requestFixture() Request {
	r := Request{Operation: OpLookupBatch, RequestID: 9, QueueGeneration: 3, OwnerEpoch: 7, LogicalStart: 100, BlockCount: 2}
	r.VolumeID[0], r.NodeID[0], r.BootID[0] = 1, 2, 3
	return r
}
func TestRequestRoundTrip(t *testing.T) {
	want := requestFixture(); b, err := want.MarshalBinary()
	if err != nil { t.Fatal(err) }; if len(b) != RequestSize { t.Fatalf("size=%d", len(b)) }
	got, err := UnmarshalRequest(b); if err != nil { t.Fatal(err) }
	if got != want { t.Fatalf("got %+v want %+v", got, want) }
}
func TestRequestRejectsReservedAndMissingIdentity(t *testing.T) {
	b, _ := requestFixture().MarshalBinary(); binary.LittleEndian.PutUint32(b[60:64], 1)
	if _, err := UnmarshalRequest(b); err == nil { t.Fatal("reserved field accepted") }
	r := requestFixture(); r.BootID = [16]byte{}
	if _, err := r.MarshalBinary(); err == nil { t.Fatal("missing boot ID accepted") }
}
func TestMutationRequiresEpochAndIdempotency(t *testing.T) {
	r := requestFixture(); r.Operation = OpAllocateZeroPublish; r.OwnerEpoch = 0
	if _, err := r.MarshalBinary(); err == nil { t.Fatal("missing epoch accepted") }
	r.OwnerEpoch = 7
	if _, err := r.MarshalBinary(); err == nil { t.Fatal("missing key accepted") }
	r.IdempotencyKey[0] = 1; if _, err := r.MarshalBinary(); err != nil { t.Fatal(err) }
}
func TestCompletionRoundTripAndRejectErrorPayload(t *testing.T) {
	want := Completion{Code: CodeOK, RequestID: 12, QueueGeneration: 5, OwnerEpoch: 7, RootGeneration: 8, PayloadOffset: 4096, MappingCount: 2, PayloadBytes: 2*MappingSize}
	b, err := want.MarshalBinary(); if err != nil { t.Fatal(err) }
	got, err := UnmarshalCompletion(b); if err != nil { t.Fatal(err) }
	if got != want { t.Fatalf("got %+v want %+v", got, want) }
	bad := Completion{Code: CodeFenced, RequestID: 1, QueueGeneration: 1, MappingCount: 1, PayloadBytes: MappingSize}
	if _, err := bad.MarshalBinary(); err == nil { t.Fatal("error payload accepted") }
}
func TestMappingRoundTrip(t *testing.T) {
	want := Mapping{LogicalBlock: 10, PhysicalBlock: 99, BlockCount: 4}
	b, err := want.MarshalBinary(); if err != nil { t.Fatal(err) }
	got, err := UnmarshalMapping(b); if err != nil { t.Fatal(err) }
	if got != want { t.Fatalf("got %+v want %+v", got, want) }
}

func TestCancelRequestRoundTrip(t *testing.T) {
	want := CancelRequest{QueueID: 7, RequestID: 91, QueueGeneration: 12}
	raw, err := want.MarshalBinary()
	if err != nil { t.Fatal(err) }
	if len(raw) != CancelSize { t.Fatalf("size=%d", len(raw)) }
	got, err := UnmarshalCancelRequest(raw)
	if err != nil { t.Fatal(err) }
	if got != want { t.Fatalf("got %+v want %+v", got, want) }
	raw[4] = 0
	if _, err := UnmarshalCancelRequest(raw); err == nil { t.Fatal("accepted zero queue ID") }
}

func TestUnmappedMappingRoundTripAndValidation(t *testing.T) {
	want:=Mapping{LogicalBlock:12,BlockCount:3,Unmapped:true}
	raw,err:=want.MarshalBinary();if err!=nil{t.Fatal(err)}
	got,err:=UnmarshalMapping(raw);if err!=nil{t.Fatal(err)}
	if got!=want{t.Fatalf("got=%+v want=%+v",got,want)}
	bad:=Mapping{LogicalBlock:12,PhysicalBlock:99,BlockCount:1,Unmapped:true}
	if _,err=bad.MarshalBinary();err==nil{t.Fatal("unmapped mapping with physical address accepted")}
	raw[20]=0x80
	if _,err=UnmarshalMapping(raw);err==nil{t.Fatal("unknown mapping flags accepted")}
}
