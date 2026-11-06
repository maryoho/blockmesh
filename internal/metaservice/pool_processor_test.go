package metaservice

import (
	"errors"
	"testing"

	"blockmesh/internal/btree"
)

type testMappingTree map[uint64]uint64

func (m testMappingTree) Get(key uint64) (uint64, error) {
	v, ok := m[key]
	if !ok { return 0, btree.ErrKeyNotFound }
	return v, nil
}

func TestLookupContiguous(t *testing.T) {
	tree := testMappingTree{10: 100, 11: 101, 12: 102}
	got, err := lookupContiguous(tree, 10, 3)
	if err != nil { t.Fatal(err) }
	if got.LogicalBlock != 10 || got.PhysicalBlock != 100 || got.BlockCount != 3 {
		t.Fatalf("unexpected mapping: %+v", got)
	}
}

func TestLookupContiguousRejectsHole(t *testing.T) {
	tree := testMappingTree{10: 100, 12: 102}
	_, err := lookupContiguous(tree, 10, 3)
	if !errors.Is(err, ErrUnmapped) { t.Fatalf("got %v, want ErrUnmapped", err) }
}

func TestLookupContiguousRejectsFragmentedRange(t *testing.T) {
	tree := testMappingTree{10: 100, 11: 900, 12: 102}
	_, err := lookupContiguous(tree, 10, 3)
	if !errors.Is(err, ErrRetry) || !errors.Is(err, ErrNonContiguousMapping) {
		t.Fatalf("got %v, want retryable non-contiguous mapping", err)
	}
}

func TestLookupContiguousRejectsInvalidCount(t *testing.T) {
	_, err := lookupContiguous(testMappingTree{}, 0, 0)
	if !errors.Is(err, ErrInvalid) { t.Fatalf("got %v, want ErrInvalid", err) }
}

func TestUUIDText(t *testing.T) {
	var id [16]byte
	for i := range id { id[i] = byte(i) }
	if got, want := uuidText(id), "00010203-0405-0607-0809-0a0b0c0d0e0f"; got != want {
		t.Fatalf("uuidText=%q, want %q", got, want)
	}
}
