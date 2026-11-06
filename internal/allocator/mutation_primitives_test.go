package allocator

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/btree"
	"blockmesh/internal/metastore"
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


type idempotencyPageReader map[uint64]metastore.Page
func (r idempotencyPageReader) ReadPage(id uint64) (metastore.Page, error) {
	p, ok := r[id]
	if !ok { return metastore.Page{}, errors.New("page not found") }
	return p, nil
}

func TestLookupIdempotencyRecordCollisionChain(t *testing.T) {
	var keyA, keyB [16]byte
	var fpA, fpB [32]byte
	keyA[0], keyB[0], fpA[0], fpB[0] = 1, 2, 3, 4
	first := IdempotencyRecord{Key:keyA, Fingerprint:fpA, LogicalStart:1, PhysicalStart:11, BlockCount:1, OwnerEpoch:5, NextPage:8}
	second := IdempotencyRecord{Key:keyB, Fingerprint:fpB, LogicalStart:2, PhysicalStart:22, BlockCount:1, OwnerEpoch:5}
	p1, err := BuildIdempotencyRecordPage(9, 7, first); if err != nil { t.Fatal(err) }
	p2, err := BuildIdempotencyRecordPage(8, 7, second); if err != nil { t.Fatal(err) }
	reader := idempotencyPageReader{9:p1, 8:p2}
	got, found, err := LookupIdempotencyRecord(reader, 9, keyB, fpB)
	if err != nil || !found || got.PhysicalStart != 22 { t.Fatalf("lookup got=%+v found=%v err=%v", got, found, err) }
	if _, found, err = LookupIdempotencyRecord(reader, 9, keyA, [32]byte{99}); found || !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("fingerprint mismatch found=%v err=%v", found, err)
	}
	if _, found, err = LookupIdempotencyRecord(reader, 9, [16]byte{99}, fpB); err != nil || found {
		t.Fatalf("missing key found=%v err=%v", found, err)
	}
	// A corrupt chain must fail closed instead of looping forever.
	cycle := first; cycle.NextPage = 9
	cyclePage, err := BuildIdempotencyRecordPage(9, 7, cycle); if err != nil { t.Fatal(err) }
	reader[9] = cyclePage
	if _, _, err = LookupIdempotencyRecord(reader, 9, keyB, fpB); !errors.Is(err, ErrIdempotencyRecord) {
		t.Fatalf("cycle error=%v", err)
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


func TestResolveIdempotencyPagesValidatesAndPinsChains(t *testing.T) {
	var key [16]byte
	var fp [32]byte
	key[0],fp[0]=5,6
	record:=IdempotencyRecord{Key:key,Fingerprint:fp,LogicalStart:1,PhysicalStart:2,BlockCount:1,OwnerEpoch:3}
	page,err:=BuildIdempotencyRecordPage(9,4,record);if err!=nil {t.Fatal(err)}
	storeReader:=idempotencyPageReader{9:page}
	// Adapter to the resolver's Store API is exercised via a tiny formatted store below.
	dev:=newIdempotencyMemDevice(32)
	store,err:=metastore.Format(dev,metastore.Layout{VolumeID:"idem-resolver",TotalPages:32,ArenaStart:2,ArenaPages:28})
	if err!=nil {t.Fatal(err)}
	if err=store.WritePage(page);err!=nil {t.Fatal(err)}
	if err=store.Sync();err!=nil {t.Fatal(err)}
	got,err:=ResolveIdempotencyPages(store,[]btree.Item{{Key:IdempotencyHash(key),Value:9}})
	if err!=nil||len(got)!=1||got[0]!=9 {t.Fatalf("resolved pages=%v err=%v",got,err)}
	if _,err=ResolveIdempotencyPages(store,[]btree.Item{{Key:IdempotencyHash(key)^1,Value:9}});!errors.Is(err,ErrIdempotencyRecord) {
		t.Fatalf("mis-keyed record error=%v",err)
	}
	_ = storeReader
}

type idempotencyMemDevice struct { b []byte }
func newIdempotencyMemDevice(pages int) *idempotencyMemDevice { return &idempotencyMemDevice{b:make([]byte,pages*int(format.PageSize))} }
func (d *idempotencyMemDevice) ReadAt(p []byte,off int64)(int,error) {
	if off<0||off>=int64(len(d.b)){return 0,io.EOF}
	n:=copy(p,d.b[off:]);if n!=len(p){return n,io.EOF};return n,nil
}
func (d *idempotencyMemDevice) WriteAt(p []byte,off int64)(int,error) {
	if off<0||off+int64(len(p))>int64(len(d.b)){return 0,io.ErrShortWrite}
	return copy(d.b[off:],p),nil
}
func (d *idempotencyMemDevice) Sync() error { return nil }
