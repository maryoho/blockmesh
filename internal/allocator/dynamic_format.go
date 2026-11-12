package allocator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

const (
	dynamicFormatMarkerOffset = uint64(2 * format.PageSize)
	dynamicFormatMarkerMagic  = uint64(0x424D464D54303031) // BMFMT001
	dynamicFormatMarkerCRC    = 64
)

var ErrDynamicFormatIncomplete = errors.New("BMPOOL03 format is incomplete")
var ErrDynamicSuperblockUnformatted = errors.New("BMPOOL03 superblock is not formatted")

// FormatDynamicPool initializes the BMPOOL03 bitmap region and publishes the
// two superblock copies only after all bitmap pages are durable. A durable
// format marker makes an interrupted format detectable and safely resumable.
// Caller must hold the exclusive, fenced Pool Owner lease for the whole call.
func FormatDynamicPool(dev device.Device, sb format.DynamicSuperblock) error {
	if dev == nil { return errors.New("nil BMPOOL03 metadata device") }
	if err:=sb.Validate();err!=nil{return err}
	if dev.Size()<0||uint64(dev.Size())<sb.Geometry.MetadataBytes{return errors.New("MetadataView is smaller than BMPOOL03 geometry")}
	encoded,err:=sb.Encode();if err!=nil{return err}
	marker:=make([]byte,format.PageSize)
	n,readErr:=dev.ReadAt(marker,int64(dynamicFormatMarkerOffset))
	if readErr!=nil&&!(readErr==io.EOF&&n==len(marker)){return readErr}
	if n!=len(marker){return io.ErrUnexpectedEOF}
	markerValid:=decodeDynamicFormatMarker(marker,sb)==nil
	if !markerValid {
		if !allZero(marker){return errors.New("BMPOOL03 format marker is occupied or corrupt")}
		for _,off:=range []uint64{0,format.PageSize}{
			page:=make([]byte,format.PageSize)
			n,e:=dev.ReadAt(page,int64(off))
			if e!=nil&&!(e==io.EOF&&n==len(page)){return e}
			if n!=len(page){return io.ErrUnexpectedEOF}
			if !allZero(page){return errors.New("metadata device is not blank; refusing destructive BMPOOL03 format")}
		}
		marker=encodeDynamicFormatMarker(sb)
		if err=writeFullDevice(dev,marker,int64(dynamicFormatMarkerOffset));err!=nil{return err}
		if err=dev.Sync();err!=nil{return fmt.Errorf("persist BMPOOL03 format intent: %w",err)}
	}
	// A retry is allowed only for the same geometry/Pool identity. Existing
	// published copies must either be blank or byte-identical to this request.
	for _,off:=range []uint64{0,format.PageSize}{
		page:=make([]byte,format.PageSize)
		n,e:=dev.ReadAt(page,int64(off))
		if e!=nil&&!(e==io.EOF&&n==len(page)){return e}
		if n!=len(page){return io.ErrUnexpectedEOF}
		if !allZero(page)&&!equalBytes(page,encoded){return errors.New("existing BMPOOL03 superblock differs from interrupted format request")}
	}
	if err=initializeDynamicBitmaps(dev,sb);err!=nil{return err}
	if err=format.InitializeDynamicCheckpoints(dev,sb.Geometry);err!=nil{return fmt.Errorf("initialize BMPOOL03 checkpoints: %w",err)}
	if err=dev.Sync();err!=nil{return fmt.Errorf("sync initialized BMPOOL03 bitmap region: %w",err)}
	// Publish copy A, flush, then copy B, flush. Either complete copy is
	// sufficient for read-only discovery; the marker prevents RW activation
	// until the whole format protocol has completed.
	for _,off:=range []uint64{0,format.PageSize}{
		if err=writeFullDevice(dev,encoded,int64(off));err!=nil{return fmt.Errorf("publish BMPOOL03 superblock at %d: %w",off,err)}
		if err=dev.Sync();err!=nil{return fmt.Errorf("sync BMPOOL03 superblock at %d: %w",off,err)}
	}
	if err=writeFullDevice(dev,make([]byte,format.PageSize),int64(dynamicFormatMarkerOffset));err!=nil{return fmt.Errorf("clear BMPOOL03 format marker: %w",err)}
	if err=dev.Sync();err!=nil{return fmt.Errorf("sync BMPOOL03 format completion: %w",err)}
	return nil
}

// RecoverDynamicFormat resumes a format interrupted after its durable marker.
// It never starts a new format on an unmarked or unrelated device.
func RecoverDynamicFormat(dev device.Device,sb format.DynamicSuperblock)error{
	if dev==nil{return errors.New("nil BMPOOL03 metadata device")}
	marker:=make([]byte,format.PageSize)
	n,err:=dev.ReadAt(marker,int64(dynamicFormatMarkerOffset))
	if err!=nil&&!(err==io.EOF&&n==len(marker)){return err}
	if n!=len(marker){return io.ErrUnexpectedEOF}
	if err=decodeDynamicFormatMarker(marker,sb);err!=nil{return fmt.Errorf("no matching interrupted BMPOOL03 format: %w",err)}
	return FormatDynamicPool(dev,sb)
}

// OpenDynamicSuperblockPair validates both redundant copies and refuses to
// expose a Pool while a format marker is present.
func OpenDynamicSuperblockPair(dev device.Device)(format.DynamicSuperblock,error){
	var zero format.DynamicSuperblock
	if dev==nil{return zero,errors.New("nil BMPOOL03 metadata device")}
	marker:=make([]byte,format.PageSize)
	n,err:=dev.ReadAt(marker,int64(dynamicFormatMarkerOffset))
	if err!=nil&&!(err==io.EOF&&n==len(marker)){return zero,err}
	if n!=len(marker){return zero,io.ErrUnexpectedEOF}
	if !allZero(marker){return zero,ErrDynamicFormatIncomplete}
	var valid []format.DynamicSuperblock
	for _,off:=range []uint64{0,format.PageSize}{
		s,readErr:=format.ReadDynamicSuperblock(dev,int64(off))
		if readErr==nil{valid=append(valid,s)}
	}
	if len(valid)==0{
  for _,off:=range []uint64{0,format.PageSize}{
   page:=make([]byte,format.PageSize)
   n,e:=dev.ReadAt(page,int64(off))
   if e!=nil&&!(e==io.EOF&&n==len(page)){return zero,e}
   if n!=len(page){return zero,io.ErrUnexpectedEOF}
   if !allZero(page){return zero,errors.New("no valid BMPOOL03 superblock copy; metadata is occupied")}
  }
  return zero,ErrDynamicSuperblockUnformatted
 }
	if len(valid)==2{
		if valid[0].PoolUUID!=valid[1].PoolUUID{return zero,errors.New("BMPOOL03 superblock copies have different Pool UUIDs")}
		if valid[0].Generation==valid[1].Generation&&valid[0]!=valid[1]{return zero,errors.New("conflicting BMPOOL03 superblock copies at equal generation")}
		if valid[1].Generation>valid[0].Generation{return valid[1],nil}
	}
	return valid[0],nil
}

func initializeDynamicBitmaps(dev device.Device,sb format.DynamicSuperblock)error{
	g:=sb.Geometry
	var err error
	for ag:=uint32(0);ag<g.AGCapacity;ag++{
		var valid uint64
		if ag<g.AGCount { var err error; valid,err=g.AGBlocks(ag);if err!=nil{return err} }
		base:=g.BitmapOffset+uint64(ag)*g.BitmapStride
		bitmap:=make([]byte,g.BitmapPaddedBytes)
		// Bits beyond the valid block count are permanently allocated. This
		// prevents allocators from ever returning padding as a DataView block.
		for bit:=valid;bit<uint64(len(bitmap))*8;bit++{bitmap[bit/8]|=1<<uint(bit%8)}
		desc:=make([]byte,format.PageSize)
		binary.LittleEndian.PutUint64(desc[:8],bitmapDescriptorMagic)
		binary.LittleEndian.PutUint32(desc[8:12],ag)
		binary.LittleEndian.PutUint64(desc[16:24],1)
		binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
		if err=writeFullDevice(dev,bitmap,int64(base+format.PageSize));err!=nil{return fmt.Errorf("initialize AG %d bitmap: %w",ag,err)}
		if err=writeFullDevice(dev,desc,int64(base));err!=nil{return fmt.Errorf("initialize AG %d descriptor: %w",ag,err)}
	}
	return nil
}

func encodeDynamicFormatMarker(sb format.DynamicSuperblock)[]byte{
	b:=make([]byte,format.PageSize)
	binary.LittleEndian.PutUint64(b[:8],dynamicFormatMarkerMagic)
	binary.LittleEndian.PutUint32(b[8:12],1)
	copy(b[16:32],sb.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[32:40],sb.Generation)
	binary.LittleEndian.PutUint64(b[40:48],sb.ViewGeneration)
	binary.LittleEndian.PutUint64(b[48:56],sb.Geometry.DataBytes)
	binary.LittleEndian.PutUint64(b[56:64],sb.Geometry.MetadataBytes)
	binary.LittleEndian.PutUint32(b[dynamicFormatMarkerCRC:dynamicFormatMarkerCRC+4],0)
	binary.LittleEndian.PutUint32(b[dynamicFormatMarkerCRC:dynamicFormatMarkerCRC+4],crc32.Checksum(b,castagnoli))
	return b
}
func decodeDynamicFormatMarker(b []byte,sb format.DynamicSuperblock)error{
	if len(b)!=int(format.PageSize)||binary.LittleEndian.Uint64(b[:8])!=dynamicFormatMarkerMagic||binary.LittleEndian.Uint32(b[8:12])!=1{return errors.New("invalid BMPOOL03 format marker")}
	c:=append([]byte(nil),b...)
	stored:=binary.LittleEndian.Uint32(c[dynamicFormatMarkerCRC:dynamicFormatMarkerCRC+4])
	binary.LittleEndian.PutUint32(c[dynamicFormatMarkerCRC:dynamicFormatMarkerCRC+4],0)
	if crc32.Checksum(c,castagnoli)!=stored{return errors.New("BMPOOL03 format marker CRC32C mismatch")}
	if !equalBytes(b,encodeDynamicFormatMarker(sb)){return errors.New("BMPOOL03 format marker belongs to another Pool geometry")}
	return nil
}
func writeFullDevice(w io.WriterAt,p []byte,off int64)error{
	n,err:=w.WriteAt(p,off);if err!=nil{return err};if n!=len(p){return io.ErrShortWrite};return nil
}
func allZero(b []byte)bool{for _,v:=range b{if v!=0{return false}};return true}
func equalBytes(a,b []byte)bool{if len(a)!=len(b){return false};for i:=range a{if a[i]!=b[i]{return false}};return true}
