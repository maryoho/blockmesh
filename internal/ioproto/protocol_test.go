package ioproto

import "testing"

func baseRequest(op Operation) Request {
	return Request{
		Version: Version, RequestID: 1, Operation: op,
		Identity: Identity{VolumeID:"vol-1",NodeID:"node-a",BootID:"boot-a",OwnerEpoch:7},
		LogicalStart: 10, BlockCount: 2,
		IdempotencyKey: [16]byte{1},
	}
}

func TestRequestValidation(t *testing.T) {
	if err:=baseRequest(OpLookupBatch).Validate(); err!=nil { t.Fatal(err) }
	if err:=baseRequest(OpAllocateZeroPublish).Validate(); err!=nil { t.Fatal(err) }
	if err:=baseRequest(OpDiscard).Validate(); err!=nil { t.Fatal(err) }
	r:=baseRequest(OpFlush);r.BlockCount=0
	if err:=r.Validate(); err!=nil { t.Fatal(err) }
}

func TestRequestRejectsUnsafeMutations(t *testing.T) {
	r:=baseRequest(OpAllocateZeroPublish);r.Identity.OwnerEpoch=0
	if r.Validate()==nil { t.Fatal("accepted mutation without OwnerEpoch") }
	r=baseRequest(OpDiscard);r.IdempotencyKey=[16]byte{}
	if r.Validate()==nil { t.Fatal("accepted mutation without idempotency key") }
	r=baseRequest(OpLookupBatch);r.BlockCount=MaxBatchBlocks+1
	if r.Validate()==nil { t.Fatal("accepted oversized lookup batch") }
}

func TestLookupRequiresReaderGenerationOrOwnerEpoch(t *testing.T) {
	r:=baseRequest(OpLookupBatch);r.Identity.OwnerEpoch=0
	if r.Validate()==nil { t.Fatal("accepted lookup without a reader or owner identity") }
	r.Identity.ReaderEpoch=3;r.Identity.RootGeneration=8
	if err:=r.Validate();err!=nil { t.Fatal(err) }
}

func TestResponseValidation(t *testing.T) {
	req:=baseRequest(OpLookupBatch)
	resp:=Response{Version:Version,RequestID:req.RequestID,Code:ResultOK,OwnerEpoch:7,RootGeneration:9,
		Mappings:[]Mapping{{LogicalBlock:10,PhysicalBlock:100,BlockCount:1}}}
	if err:=resp.Validate(req);err!=nil { t.Fatal(err) }
	resp.Mappings[0].LogicalBlock=9
	if err:=resp.Validate(req);err==nil { t.Fatal("accepted mapping outside requested range") }
	resp.Mappings=nil;resp.Code=ResultUnmapped
	if err:=resp.Validate(req);err!=nil { t.Fatal(err) }
}
