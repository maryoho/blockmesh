package allocator

import (
	"bytes"
	"context"
	"testing"

	"blockmesh/internal/format"
)

func TestPoolDataViewHonorsDataStartSector(t *testing.T) {
	const base = 512
	d := &zeroTestDevice{data: bytes.Repeat([]byte{0xaa}, int(base+2*format.BlockSize))}
	pool := format.Superblock{DataStartSector: 1, PoolSize: 2 * format.BlockSize}
	view, err := newPoolDataView(d, pool)
	if err != nil { t.Fatal(err) }
	if view.Size() != int64(pool.PoolSize) { t.Fatalf("view size=%d", view.Size()) }
	if err = ZeroBlocks(context.Background(), view, 1, 1); err != nil { t.Fatal(err) }
	if d.syncs != 1 { t.Fatalf("data device sync count=%d", d.syncs) }
	if !bytes.Equal(d.data[:base+int(format.BlockSize)], bytes.Repeat([]byte{0xaa}, base+int(format.BlockSize))) {
		t.Fatal("zeroing escaped the requested Pool-relative block")
	}
	for i, value := range d.data[base+int(format.BlockSize):] {
		if value != 0 { t.Fatalf("Pool block byte %d was not zeroed", i) }
	}
}

func TestPoolDataViewRejectsBackingDeviceTooSmall(t *testing.T) {
	d := &zeroTestDevice{data: make([]byte, int(format.BlockSize))}
	pool := format.Superblock{DataStartSector: 1, PoolSize: format.BlockSize}
	if _, err := newPoolDataView(d, pool); err == nil {
		t.Fatal("expected data device geometry error")
	}
}

func TestPoolDataViewRejectsOutOfRangeWrite(t *testing.T) {
	d := &zeroTestDevice{data: make([]byte, int(format.BlockSize))}
	pool := format.Superblock{PoolSize: format.BlockSize}
	view, err := newPoolDataView(d, pool)
	if err != nil { t.Fatal(err) }
	if _, err = view.WriteAt([]byte{1}, int64(format.BlockSize)); err == nil {
		t.Fatal("expected Pool-relative bounds error")
	}
}
