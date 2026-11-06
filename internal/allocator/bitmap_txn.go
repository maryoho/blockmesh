package allocator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

var ErrNoContiguousBlocks = errors.New("no contiguous free data blocks in AG")

// PlanBitmapAllocation reads one AG bitmap and returns full-page WAL
// after-images for its descriptor and every bitmap page touched by a
// contiguous allocation. It does not write metadata; the caller must include
// the returned images in the same B+Tree/Volume-root WAL transaction.
func PlanBitmapAllocation(r io.ReaderAt, sb format.Superblock, ag uint32, count uint32) (uint64, []metastore.PageImage, error) {
	if r==nil || sb.Validate()!=nil || ag>=sb.AGCount || count==0 || uint64(count)>format.AGBlocks {
		return 0,nil,errors.New("invalid AG bitmap allocation request")
	}
	base:=sb.BitmapOffset+uint64(ag)*format.AGBitmapStride
	desc:=make([]byte,format.PageSize)
	if err:=readAtFull(r,desc,int64(base));err!=nil{return 0,nil,err}
	if binary.LittleEndian.Uint64(desc[:8])!=bitmapDescriptorMagic || binary.LittleEndian.Uint32(desc[8:12])!=ag {
		return 0,nil,fmt.Errorf("AG %d bitmap descriptor invalid",ag)
	}
	seq:=binary.LittleEndian.Uint64(desc[16:24])
	if seq==^uint64(0) { return 0,nil,errors.New("AG bitmap sequence exhausted") }
	bitmap:=make([]byte,format.BitmapBytes)
	if err:=readAtFull(r,bitmap,int64(base+format.PageSize));err!=nil{return 0,nil,err}
	if crc32.Checksum(bitmap,castagnoli)!=binary.LittleEndian.Uint32(desc[24:28]) { return 0,nil,fmt.Errorf("AG %d bitmap CRC32C mismatch",ag) }
	var start uint64
	run:=uint32(0)
	found:=false
	for i:=uint64(0);i<format.AGBlocks;i++ {
		if bitmap[i/8]&(1<<(i%8))==0 { run++; if run==count { start=i+1-uint64(count);found=true;break } } else { run=0 }
	}
	if !found { return 0,nil,ErrNoContiguousBlocks }
	for i:=uint64(0);i<uint64(count);i++ { bit:=start+i;bitmap[bit/8]|=byte(1<<(bit%8)) }
	firstPage:=start/(format.PageSize*8)
	lastPage:=(start+uint64(count)-1)/(format.PageSize*8)
	images:=make([]metastore.PageImage,0,int(lastPage-firstPage+2))
	for p:=firstPage;p<=lastPage;p++ {
		off:=base+format.PageSize+p*format.PageSize
		data:=append([]byte(nil),bitmap[p*format.PageSize:(p+1)*format.PageSize]...)
		images=append(images,metastore.PageImage{Offset:off,Data:data})
	}
	nextDesc:=append([]byte(nil),desc...)
	binary.LittleEndian.PutUint64(nextDesc[16:24],seq+1)
	binary.LittleEndian.PutUint32(nextDesc[24:28],crc32.Checksum(bitmap,castagnoli))
	images=append(images,metastore.PageImage{Offset:base,Data:nextDesc})
	return uint64(ag)*format.AGBlocks+start,images,nil
}

func readAtFull(r io.ReaderAt,b []byte,off int64) error {
	n,err:=r.ReadAt(b,off)
	if err!=nil && !(err==io.EOF&&n==len(b)) { return err }
	if n!=len(b) { return io.ErrUnexpectedEOF }
	return nil
}
