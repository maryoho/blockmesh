package metaservice

import (
	"context"
	"testing"

	"blockmesh/internal/uapi"
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
