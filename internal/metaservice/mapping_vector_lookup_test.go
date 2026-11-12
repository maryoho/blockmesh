package metaservice

import (
	"errors"
	"testing"

	"blockmesh/internal/btree"
	"blockmesh/internal/uapi"
)

type mappingVectorTree map[uint64]uint64

func (m mappingVectorTree) Get(key uint64) (uint64, error) {
	value, ok := m[key]
	if !ok { return 0, btree.ErrKeyNotFound }
	return value, nil
}

func TestLookupMappingVectorCoalescesOnlyPhysicalAdjacency(t *testing.T) {
	tree := mappingVectorTree{10: 100, 11: 101, 12: 900, 13: 901, 14: 1500}
	got, err := lookupMappingVector(tree, 10, 5)
	if err != nil { t.Fatal(err) }
	want := []uapi.Mapping{
		{LogicalBlock: 10, PhysicalBlock: 100, BlockCount: 2},
		{LogicalBlock: 12, PhysicalBlock: 900, BlockCount: 2},
		{LogicalBlock: 14, PhysicalBlock: 1500, BlockCount: 1},
	}
	if len(got) != len(want) { t.Fatalf("got %+v, want %+v", got, want) }
	for i := range want { if got[i] != want[i] { t.Fatalf("mapping[%d]=%+v want %+v", i, got[i], want[i]) } }
}

func TestLookupMappingVectorReturnsUnmappedMarkerAndRejectsInvalidRange(t *testing.T) {
	got,err:=lookupMappingVector(mappingVectorTree{1:10},1,2)
	if err!=nil{t.Fatal(err)}
	if len(got)!=2||got[0].Unmapped||!got[1].Unmapped||got[1].PhysicalBlock!=0{
		t.Fatalf("missing logical block was not represented as zero extent: %+v",got)
	}
	if _, err := lookupMappingVector(mappingVectorTree{}, ^uint64(0), 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow range: %v", err)
	}
}
