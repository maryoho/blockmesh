package allocator

import (
	"bytes"
	"context"
	"io"
	"testing"

	"blockmesh/internal/format"
)

type cowMemoryDevice struct { data []byte }
func (d *cowMemoryDevice) ReadAt(p []byte, off int64) (int,error) { return bytes.NewReader(d.data).ReadAt(p,off) }
func (d *cowMemoryDevice) WriteAt(p []byte, off int64) (int,error) {
	if off<0 || off>int64(len(d.data)) || int64(len(p))>int64(len(d.data))-off { return 0,io.ErrShortWrite }
	return copy(d.data[int(off):],p),nil
}
func (d *cowMemoryDevice) Sync() error { return nil }
func (d *cowMemoryDevice) Size() int64 { return int64(len(d.data)) }

func TestCopyPhysicalBlockPreservesFullBlock(t *testing.T) {
	source := bytes.Repeat([]byte{0x5a},int(format.BlockSize))
	dev := &cowMemoryDevice{data:append(append([]byte(nil),source...),bytes.Repeat([]byte{0},int(format.BlockSize))...)}
	if err:=copyPhysicalBlock(context.Background(),dev,0,1);err!=nil { t.Fatal(err) }
	if !bytes.Equal(dev.data[int(format.BlockSize):],source) { t.Fatal("COW destination differs from source block") }
}

func TestCopyPhysicalBlockHonorsCancellation(t *testing.T) {
	ctx,cancel:=context.WithCancel(context.Background());cancel()
	dev:=&cowMemoryDevice{data:make([]byte,2*int(format.BlockSize))}
	if err:=copyPhysicalBlock(ctx,dev,0,1);err==nil { t.Fatal("expected canceled COW copy") }
}
