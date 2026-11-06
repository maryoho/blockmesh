package allocator

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"blockmesh/internal/format"
)

func TestIdempotencyRecordRoundTrip(t *testing.T) {
	r := IdempotencyRecord{NextPage: 9, LogicalStart: 12, PhysicalStart: 34, BlockCount: 4, Operation: 2, OwnerEpoch: 7}
	r.Key[0], r.Fingerprint[0] = 1, 2
	b, err := r.Encode(); if err != nil { t.Fatal(err) }
	got, err := DecodeIdempotencyRecord(b); if err != nil { t.Fatal(err) }
	if got != r { t.Fatalf("round trip mismatch: got %+v want %+v", got, r) }
	b[200] ^= 1
	if _, err = DecodeIdempotencyRecord(b); !errors.Is(err, ErrIdempotencyRecord) { t.Fatalf("corruption error=%v", err) }
}

func TestIdempotencyRecordPagePayloadRoundTrip(t *testing.T) {
	r := IdempotencyRecord{NextPage: 17, LogicalStart: 21, PhysicalStart: 42, BlockCount: 8, Operation: 3, OwnerEpoch: 11}
	r.Key[0], r.Fingerprint[0] = 9, 7
	payload, err := r.EncodePayload()
	if err != nil { t.Fatal(err) }
	if len(payload) >= int(format.PageSize)-64 { t.Fatalf("payload unexpectedly large: %d", len(payload)) }
	got, err := DecodeIdempotencyRecordPayload(payload)
	if err != nil { t.Fatal(err) }
	if got != r { t.Fatalf("payload round trip: got %+v want %+v", got, r) }
	payload[30] ^= 1
	if _, err := DecodeIdempotencyRecordPayload(payload); !errors.Is(err, ErrIdempotencyRecord) {
		t.Fatalf("corrupt payload error=%v", err)
	}
}

func TestIdempotencyHashNamespaceAndDeterminism(t *testing.T) {
	var a, b [16]byte
	a[0], b[0] = 1, 2
	ha, hb := IdempotencyHash(a), IdempotencyHash(b)
	if ha>>63 != 1 || hb>>63 != 1 || ha == hb || ha != IdempotencyHash(a) { t.Fatalf("bad hashes: %x %x", ha, hb) }
}

type zeroTestDevice struct { data []byte; syncErr error; syncs int }
func (d *zeroTestDevice) WriteAt(p []byte, off int64) (int,error) { return copy(d.data[int(off):],p),nil }
func (d *zeroTestDevice) Sync() error { d.syncs++; return d.syncErr }
func (d *zeroTestDevice) Size() int64 { return int64(len(d.data)) }

func TestZeroBlocksWritesAndSyncs(t *testing.T) {
	d := &zeroTestDevice{data: bytes.Repeat([]byte{0xaa}, int(2*format.BlockSize))}
	if err := ZeroBlocks(context.Background(), d, 1, 1); err != nil { t.Fatal(err) }
	if d.syncs != 1 { t.Fatalf("syncs=%d",d.syncs) }
	for i,v := range d.data[format.BlockSize:2*format.BlockSize] { if v != 0 { t.Fatalf("byte %d not zero",i) } }
	if d.data[0] != 0xaa { t.Fatal("write escaped requested physical block") }
}

func TestZeroBlocksDoesNotPublishOnSyncFailure(t *testing.T) {
	d := &zeroTestDevice{data: bytes.Repeat([]byte{0xaa}, int(format.BlockSize)), syncErr: errors.New("flush failed")}
	if err := ZeroBlocks(context.Background(), d, 0, 1); err == nil { t.Fatal("expected sync failure") }
}

func TestZeroBlocksRejectsOutOfRange(t *testing.T) {
	d := &zeroTestDevice{data: make([]byte, format.BlockSize)}
	if err := ZeroBlocks(context.Background(), d, 1, 1); err == nil { t.Fatal("expected out-of-range error") }
}
