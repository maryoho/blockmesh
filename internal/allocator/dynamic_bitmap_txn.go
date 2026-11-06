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

// PlanDynamicBitmapAllocation creates redo after-images for a BMPOOL03 AG.
// Unlike PlanBitmapAllocation, it uses the persisted DynamicGeometry, including
// the actual size of a partial final AG and its padded bitmap length.
func PlanDynamicBitmapAllocation(r io.ReaderAt, sb format.DynamicSuperblock, ag uint32, count uint32) (uint64, []metastore.PageImage, error) {
	if r==nil || sb.Validate()!=nil || count==0 {
		return 0,nil,errors.New("invalid BMPOOL03 bitmap allocation request")
	}
	g:=sb.Geometry
	agBlocks,err:=g.AGBlocks(ag)
	if err!=nil || uint64(count)>agBlocks { return 0,nil,errors.New("BMPOOL03 allocation exceeds AG capacity") }
	startBlock,err:=g.AGStartBlock(ag)
	if err!=nil{return 0,nil,err}
	base:=g.BitmapOffset+uint64(ag)*g.BitmapStride
	desc:=make([]byte,format.PageSize)
	if err:=readAtFull(r,desc,int64(base));err!=nil{return 0,nil,err}
	if binary.LittleEndian.Uint64(desc[:8])!=bitmapDescriptorMagic || binary.LittleEndian.Uint32(desc[8:12])!=ag {
		return 0,nil,fmt.Errorf("BMPOOL03 AG %d bitmap descriptor invalid",ag)
	}
	seq:=binary.LittleEndian.Uint64(desc[16:24])
	if seq==^uint64(0){return 0,nil,errors.New("BMPOOL03 AG bitmap sequence exhausted")}
	bitmap:=make([]byte,g.BitmapPaddedBytes)
	if err:=readAtFull(r,bitmap,int64(base+format.PageSize));err!=nil{return 0,nil,err}
	if crc32.Checksum(bitmap,castagnoli)!=binary.LittleEndian.Uint32(desc[24:28]){
		return 0,nil,fmt.Errorf("BMPOOL03 AG %d bitmap CRC32C mismatch",ag)
	}
	var run uint32
	var start uint64
	found:=false
	for i:=uint64(0);i<agBlocks;i++ {
		if bitmap[i/8]&(1<<(i%8))==0 {
			run++
			if run==count {start=i+1-uint64(count);found=true;break}
		}else{run=0}
	}
	if !found{return 0,nil,ErrNoContiguousBlocks}
	for i:=uint64(0);i<uint64(count);i++ {bit:=start+i;bitmap[bit/8]|=byte(1<<(bit%8))}
	firstPage:=start/(format.PageSize*8)
	lastPage:=(start+uint64(count)-1)/(format.PageSize*8)
	images:=make([]metastore.PageImage,0,int(lastPage-firstPage+2))
	for p:=firstPage;p<=lastPage;p++ {
		off:=base+format.PageSize+p*format.PageSize
		page:=append([]byte(nil),bitmap[p*format.PageSize:(p+1)*format.PageSize]...)
		images=append(images,metastore.PageImage{Offset:off,Data:page})
	}
	next:=append([]byte(nil),desc...)
	binary.LittleEndian.PutUint64(next[16:24],seq+1)
	binary.LittleEndian.PutUint32(next[24:28],crc32.Checksum(bitmap,castagnoli))
	images=append(images,metastore.PageImage{Offset:base,Data:next})
	return startBlock+start,images,nil
}
