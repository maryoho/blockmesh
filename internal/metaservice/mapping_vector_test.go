package metaservice

import (
	"context"
	"testing"

	"blockmesh/internal/uapi"
	"blockmesh/internal/btree"
)

func TestDrainPublishesMappingVector(t *testing.T) {
	q := fixture(t, uapi.OpLookupBatch, 51)
	_, produced, err := q.Drain(context.Background(), ProcessorFunc(func(context.Context, uapi.Request) (Result, error) {
		return Result{Mappings: []uapi.Mapping{
			{LogicalBlock: 40, PhysicalBlock: 900, BlockCount: 1},
			{LogicalBlock: 41, PhysicalBlock: 1200, BlockCount: 1},
		}}, nil
	}), 1)
	if err != nil || produced != 1 { t.Fatalf("produced=%d err=%v", produced, err) }
	c, err := uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize])
	if err != nil { t.Fatal(err) }
	if c.MappingCount != 2 || c.PayloadBytes != 2*uapi.MappingSize { t.Fatalf("completion=%+v", c) }
	got, err := uapi.UnmarshalMappings(q.Payload[:c.PayloadBytes], c.MappingCount)
	if err != nil { t.Fatal(err) }
	if got[0].PhysicalBlock != 900 || got[1].PhysicalBlock != 1200 { t.Fatalf("mappings=%+v", got) }
}

type mappingLookupMap map[uint64]uint64
func(m mappingLookupMap)Get(key uint64)(uint64,error){v,ok:=m[key];if !ok{return 0,btree.ErrKeyNotFound};return v,nil}

func TestLookupMappingVectorIncludesZeroExtents(t *testing.T) {
	tree:=mappingLookupMap{10:100,12:102,15:200}
	got,err:=lookupMappingVector(tree,10,7);if err!=nil{t.Fatal(err)}
	want:=[]uapi.Mapping{
		{LogicalBlock:10,PhysicalBlock:100,BlockCount:1},
		{LogicalBlock:11,BlockCount:1,Unmapped:true},
		{LogicalBlock:12,PhysicalBlock:102,BlockCount:1},
		{LogicalBlock:13,BlockCount:2,Unmapped:true},
		{LogicalBlock:15,PhysicalBlock:200,BlockCount:1},
	}
	if len(got)!=len(want){t.Fatalf("mapping count=%d want=%d: %+v",len(got),len(want),got)}
	for i:=range want{if got[i]!=want[i]{t.Fatalf("mapping[%d]=%+v want=%+v",i,got[i],want[i])}}
}
