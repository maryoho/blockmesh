package uapi

import "testing"

func TestMappingVectorRoundTrip(t *testing.T) {
	want := []Mapping{
		{LogicalBlock: 100, PhysicalBlock: 900, BlockCount: 2},
		{LogicalBlock: 102, PhysicalBlock: 1200, BlockCount: 1},
		{LogicalBlock: 103, PhysicalBlock: 1201, BlockCount: 3},
	}
	encoded, err := MarshalMappings(want)
	if err != nil { t.Fatal(err) }
	got, err := UnmarshalMappings(encoded, uint32(len(want)))
	if err != nil { t.Fatal(err) }
	if len(got) != len(want) { t.Fatalf("got %d mappings, want %d", len(got), len(want)) }
	for i := range want { if got[i] != want[i] { t.Fatalf("mapping[%d]=%+v want %+v", i, got[i], want[i]) } }
}

func TestMappingVectorRejectsHoleOverlapAndOversize(t *testing.T) {
	tests := [][]Mapping{
		{{LogicalBlock: 1, PhysicalBlock: 10, BlockCount: 1}, {LogicalBlock: 3, PhysicalBlock: 20, BlockCount: 1}},
		{{LogicalBlock: 1, PhysicalBlock: 10, BlockCount: 2}, {LogicalBlock: 2, PhysicalBlock: 20, BlockCount: 1}},
		make([]Mapping, MaxBatch+1),
	}
	for i, mappings := range tests {
		if _, err := MarshalMappings(mappings); err == nil { t.Fatalf("case %d accepted", i) }
	}
	if _, err := UnmarshalMappings(make([]byte, MappingSize), 0); err == nil { t.Fatal("zero mapping count accepted") }
}
