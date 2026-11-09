package allocator

import (
	"reflect"
	"testing"

	"blockmesh/internal/format"
)

func TestDataRetireIndexKeyStableAcrossPoolExpansion(t *testing.T) {
	record := DataRetireRecord{AG:3, RetireGeneration:17, PhysicalStart:4096, BlockCount:8}
	oldKey, err := dataRetireIndexKey(record, 1<<20)
	if err != nil { t.Fatal(err) }
	expandedKey, err := dataRetireIndexKey(record, 1<<35)
	if err != nil { t.Fatal(err) }
	if oldKey != expandedKey { t.Fatalf("Data Retire key moved after expansion: old=%#x expanded=%#x", oldKey, expandedKey) }
	if oldKey < format.DynamicRetireIndexKeyStart || oldKey >= format.DynamicRetireIndexKeyLimit {
		t.Fatalf("Data Retire key %#x outside stable reserved namespace", oldKey)
	}
}

func TestDataRetireRangesCoalesceWithinAG(t *testing.T) {
	got, err := dataRetireRanges([]uint64{9, 1, 0, 4, 3, 8}, 7, 8)
	if err != nil { t.Fatal(err) }
	want := []DataRetireRecord{
		{AG:0, RetireGeneration:7, PhysicalStart:0, BlockCount:2},
		{AG:0, RetireGeneration:7, PhysicalStart:3, BlockCount:2},
		{AG:1, RetireGeneration:7, PhysicalStart:8, BlockCount:2},
	}
	if !reflect.DeepEqual(got, want) { t.Fatalf("ranges=%+v want=%+v", got, want) }
}

func TestDataRetireRecordPayloadRoundTrip(t *testing.T) {
	record := DataRetireRecord{
		IndexKey:format.DynamicRetireIndexKeyStart+123,
		AG:2, RetireGeneration:0, PhysicalStart:1024, BlockCount:16,
	}
	payload, err := record.EncodePayload()
	if err != nil { t.Fatal(err) }
	decoded, err := DecodeDataRetireRecordPayload(payload)
	if err != nil { t.Fatal(err) }
	if decoded != record { t.Fatalf("decoded=%+v want=%+v", decoded, record) }
	payload[len(payload)-1] ^= 0xff
	if _, err = DecodeDataRetireRecordPayload(payload); err == nil {
		t.Fatal("corrupt Data Retire record passed CRC validation")
	}
}
