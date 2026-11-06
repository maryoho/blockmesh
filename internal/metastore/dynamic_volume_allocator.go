package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"blockmesh/internal/format"
)

const (
	dynamicReservedMagic uint64 = 0x424D525341303031 // BMRSA001
	dynamicReservedFormatMagic uint64 = 0x424D525346303031 // BMRSF001
	dynamicReservedVersion uint32 = 1
	dynamicReservedHeaderCRC = int(format.PageSize)-4
)

var ErrDynamicVolumeAllocatorUnformatted = errors.New("BMPOOL03 reserved-region allocator is not formatted")
var ErrDynamicVolumeSpace = errors.New("BMPOOL03 reserved metadata region has insufficient contiguous space")

type dynamicReservedHeader struct {
	Generation uint64
	PoolUUID [16]byte
	RegionStart uint64
	RegionPages uint64
	BitmapPages uint64
	AllocStart uint64
	BitmapCRC uint32
}

func (h dynamicReservedHeader) encode() ([]byte,error) {
	if h.Generation==0 || h.PoolUUID==([16]byte{}) || h.RegionPages==0 ||
		h.BitmapPages==0 || h.AllocStart!=2+h.BitmapPages || h.AllocStart>=h.RegionPages {
		return nil,ErrGeometry
	}
	b:=make([]byte,format.PageSize)
	binary.LittleEndian.PutUint64(b[0:8],dynamicReservedMagic)
	binary.LittleEndian.PutUint32(b[8:12],dynamicReservedVersion)
	binary.LittleEndian.PutUint64(b[16:24],h.Generation)
	copy(b[24:40],h.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[40:48],h.RegionStart)
	binary.LittleEndian.PutUint64(b[48:56],h.RegionPages)
	binary.LittleEndian.PutUint64(b[56:64],h.BitmapPages)
	binary.LittleEndian.PutUint64(b[64:72],h.AllocStart)
	binary.LittleEndian.PutUint32(b[72:76],h.BitmapCRC)
	binary.LittleEndian.PutUint32(b[dynamicReservedHeaderCRC:],crc32.Checksum(b[:dynamicReservedHeaderCRC],crcTable))
	return b,nil
}

func decodeDynamicReservedHeader(b []byte) (dynamicReservedHeader,error) {
	var h dynamicReservedHeader
	if len(b)!=int(format.PageSize) || binary.LittleEndian.Uint64(b[:8])!=dynamicReservedMagic ||
		binary.LittleEndian.Uint32(b[8:12])!=dynamicReservedVersion ||
		crc32.Checksum(b[:dynamicReservedHeaderCRC],crcTable)!=binary.LittleEndian.Uint32(b[dynamicReservedHeaderCRC:]) {
		return h,ErrCorrupt
	}
	for _,v:=range b[12:16] { if v!=0{return h,ErrCorrupt} }
	for _,v:=range b[76:dynamicReservedHeaderCRC] { if v!=0{return h,ErrCorrupt} }
	h=dynamicReservedHeader{Generation:binary.LittleEndian.Uint64(b[16:24]),RegionStart:binary.LittleEndian.Uint64(b[40:48]),
		RegionPages:binary.LittleEndian.Uint64(b[48:56]),BitmapPages:binary.LittleEndian.Uint64(b[56:64]),
		AllocStart:binary.LittleEndian.Uint64(b[64:72]),BitmapCRC:binary.LittleEndian.Uint32(b[72:76])}
	copy(h.PoolUUID[:],b[24:40])
	if _,err:=h.encode();err!=nil{return dynamicReservedHeader{},err}
	return h,nil
}

// DynamicVolumeAllocator manages contiguous per-Volume Arena reservations in
// BMPOOL03 Reserved Metadata. It must be formatted before the first dynamic
// Volume descriptor is created. Existing legacy slot-0 layouts remain readable.
type DynamicVolumeAllocator struct {
	mu sync.Mutex
	dev Device
	sb format.DynamicSuperblock
	header dynamicReservedHeader
	headerSlot uint64
	bitmap []byte
}

func dynamicReservedGeometry(sb format.DynamicSuperblock) (dynamicReservedHeader,uint64,error) {
	if err:=sb.Validate();err!=nil{return dynamicReservedHeader{},0,err}
	g:=sb.Geometry
	start:=g.ReservedOffset/format.PageSize
	pages:=(g.MetadataBytes-g.ReservedOffset)/format.PageSize
	bitmapPages:=(pages+uint64(format.PageSize)*8-1)/(uint64(format.PageSize)*8)
	h:=dynamicReservedHeader{Generation:1,PoolUUID:sb.PoolUUID,RegionStart:start,RegionPages:pages,
		BitmapPages:bitmapPages,AllocStart:2+bitmapPages}
	if _,err:=h.encode();err!=nil{return h,0,err}
	if h.AllocStart>=pages{return h,0,ErrGeometry}
	return h,bitmapPages,nil
}

// FormatDynamicVolumeAllocator initializes the Reserved-region bitmap and
// dual allocator headers. Call only on a newly formatted Pool before publishing
// any Volume Descriptor or Arena pages; it refuses a non-zero Reserved region.
func FormatDynamicVolumeAllocator(dev Device,sb format.DynamicSuperblock) error {
	if dev==nil{return errors.New("nil BMPOOL03 MetadataView")}
	h,bitmapPages,err:=dynamicReservedGeometry(sb);if err!=nil{return err}
	if devSize,ok:=dev.(interface{Size()int64});ok&&(devSize.Size()<0||uint64(devSize.Size())<sb.Geometry.MetadataBytes){return ErrGeometry}
	off:=h.RegionStart*format.PageSize
	first:=make([]byte,format.PageSize)
	if err=readFullAt(dev,first,int64(off));err!=nil{return err}
	resume:=false
	if existing,e:=decodeDynamicReservedHeader(first);e==nil&&existing.PoolUUID==sb.PoolUUID {
		return errors.New("BMPOOL03 Reserved allocator is already formatted")
	}
	if dynamicReservedFormatMarkerMatches(first,h) {
		resume=true
	} else if !dynamicVolumeAllZero(first) {
		return errors.New("BMPOOL03 Reserved allocator header is occupied or corrupt")
	}
	if !resume {
		length:=h.RegionPages*format.PageSize
		buf:=make([]byte,1<<20)
		for done:=uint64(0);done<length; {
			n:=uint64(len(buf));if length-done<n{n=length-done}
			if err=readFullAt(dev,buf[:n],int64(off+done));err!=nil{return err}
			for _,v:=range buf[:n]{if v!=0{return errors.New("refusing to format non-empty BMPOOL03 Reserved region")}}
			done+=n
		}
		marker:=encodeDynamicReservedFormatMarker(h)
		if err=writeFullAt(dev,marker,int64(off));err!=nil{return err}
		if err=dev.Sync();err!=nil{return fmt.Errorf("persist Reserved allocator format marker: %w",err)}
	}
	bitmap:=make([]byte,bitmapPages*format.PageSize)
	for bit:=uint64(0);bit<h.AllocStart;bit++{bitmap[bit/8]|=1<<uint(bit%8)}
	for bit:=h.RegionPages;bit<uint64(len(bitmap))*8;bit++{bitmap[bit/8]|=1<<uint(bit%8)}
	h.BitmapCRC=crc32.Checksum(bitmap,crcTable)
	header,err:=h.encode();if err!=nil{return err}
	if err=writeFullAt(dev,bitmap,int64((h.RegionStart+2)*format.PageSize));err!=nil{return err}
	if err=dev.Sync();err!=nil{return fmt.Errorf("sync Reserved allocator bitmap: %w",err)}
	if err=writeFullAt(dev,header,int64(h.RegionStart*format.PageSize));err!=nil{return err}
	if err=dev.Sync();err!=nil{return fmt.Errorf("publish first Reserved allocator header: %w",err)}
	if err=writeFullAt(dev,header,int64((h.RegionStart+1)*format.PageSize));err!=nil{return err}
	if err=dev.Sync();err!=nil{return fmt.Errorf("publish second Reserved allocator header: %w",err)}
	return nil
}

// RecoverDynamicVolumeAllocatorFormat resumes only a format whose matching
// durable marker was written before any Reserved-region initialization.
func RecoverDynamicVolumeAllocatorFormat(dev Device,sb format.DynamicSuperblock) error {
	if dev==nil{return errors.New("nil BMPOOL03 MetadataView")}
	h,_,err:=dynamicReservedGeometry(sb);if err!=nil{return err}
	marker:=make([]byte,format.PageSize)
	if err=readFullAt(dev,marker,int64(h.RegionStart*format.PageSize));err!=nil{return err}
	if !dynamicReservedFormatMarkerMatches(marker,h){return errors.New("no matching interrupted Reserved allocator format")}
	return FormatDynamicVolumeAllocator(dev,sb)
}

func encodeDynamicReservedFormatMarker(h dynamicReservedHeader) []byte {
	b:=make([]byte,format.PageSize)
	binary.LittleEndian.PutUint64(b[:8],dynamicReservedFormatMagic)
	binary.LittleEndian.PutUint32(b[8:12],dynamicReservedVersion)
	copy(b[16:32],h.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[32:40],h.RegionStart)
	binary.LittleEndian.PutUint64(b[40:48],h.RegionPages)
	binary.LittleEndian.PutUint64(b[48:56],h.BitmapPages)
	binary.LittleEndian.PutUint64(b[56:64],h.AllocStart)
	binary.LittleEndian.PutUint32(b[dynamicReservedHeaderCRC:],crc32.Checksum(b[:dynamicReservedHeaderCRC],crcTable))
	return b
}
func dynamicReservedFormatMarkerMatches(b []byte,h dynamicReservedHeader) bool {
	if len(b)!=int(format.PageSize){return false}
	return string(b)==string(encodeDynamicReservedFormatMarker(h))
}

func OpenDynamicVolumeAllocator(dev Device,sb format.DynamicSuperblock) (*DynamicVolumeAllocator,error) {
	if dev==nil{return nil,errors.New("nil BMPOOL03 MetadataView")}
	expected,bitmapPages,err:=dynamicReservedGeometry(sb);if err!=nil{return nil,err}
	var valid []struct{h dynamicReservedHeader;slot uint64}
	for slot:=uint64(0);slot<2;slot++{
		page:=make([]byte,format.PageSize)
		if err=readFullAt(dev,page,int64((expected.RegionStart+slot)*format.PageSize));err!=nil{return nil,err}
		h,e:=decodeDynamicReservedHeader(page);if e==nil&&h.PoolUUID==sb.PoolUUID&&h.RegionStart==expected.RegionStart&&
			h.RegionPages==expected.RegionPages&&h.BitmapPages==bitmapPages&&h.AllocStart==expected.AllocStart{valid=append(valid,struct{h dynamicReservedHeader;slot uint64}{h,slot})}
	}
	if len(valid)==0{return nil,ErrDynamicVolumeAllocatorUnformatted}
	best:=valid[0]
	if len(valid)==2{
		if valid[0].h.Generation==valid[1].h.Generation&&valid[0].h!=valid[1].h{return nil,errors.New("conflicting BMPOOL03 Reserved allocator headers")}
		if valid[1].h.Generation>valid[0].h.Generation{best=valid[1]}
	}
	bitmap:=make([]byte,best.h.BitmapPages*format.PageSize)
	if err=readFullAt(dev,bitmap,int64((best.h.RegionStart+2)*format.PageSize));err!=nil{return nil,err}
	if crc32.Checksum(bitmap,crcTable)!=best.h.BitmapCRC{return nil,errors.New("BMPOOL03 Reserved allocator bitmap CRC32C mismatch")}
	a:=&DynamicVolumeAllocator{dev:dev,sb:sb,header:best.h,headerSlot:best.slot,bitmap:bitmap}
	if len(valid)==1{
		page,e:=best.h.encode();if e!=nil{return nil,e}
		if e=writeFullAt(dev,page,int64((best.h.RegionStart+(1-best.slot))*format.PageSize));e!=nil{return nil,e}
		if e=dev.Sync();e!=nil{return nil,e}
	}
	return a,nil
}

// AllocateVolume atomically reserves Arena/Lifecycle pages and publishes both
// descriptor copies in the same cluster-safe WAL transaction. ArenaPages and
// LifecyclePages in descriptor are requested sizes; their starts are assigned.
func(a *DynamicVolumeAllocator) AllocateVolume(index uint32,d format.DynamicVolumeDescriptor,committer ClusterSafeCommitter)(format.DynamicVolumeDescriptor,error) {
	if a==nil||committer==nil||!committer.ClusterSafe(){return d,ErrClusterSafeCommitterRequired}
	a.mu.Lock();defer a.mu.Unlock()
	if d.ArenaPages==0{return d,ErrGeometry}
	if d.LifecyclePages!=0&&(d.LifecyclePages<2||d.LifecyclePages>MaxLifecyclePages){return d,ErrGeometry}
	if a.header.Generation==math.MaxUint64{return d,errors.New("Reserved allocator generation exhausted")}
	pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,index);if err!=nil{return d,err}
	// A descriptor slot is single-assignment until a fenced ReleaseVolume.
	for slot:=uint64(0);slot<2;slot++{
		page:=make([]byte,format.PageSize)
		if err=readFullAt(a.dev,page,int64(pair+slot*format.PageSize));err!=nil{return d,err}
		if !allZeroLifecycle(page){return d,errors.New("BMPOOL03 Volume Descriptor slot is already occupied")}
	}
	if err=a.ensureUniqueVolumeID(d.VolumeID,index);err!=nil{return d,err}
	need:=d.ArenaPages+d.LifecyclePages
	if need<d.ArenaPages{return d,ErrGeometry}
	start,ok:=a.findRun(need);if !ok{return d,ErrDynamicVolumeSpace}
	nextBitmap:=append([]byte(nil),a.bitmap...)
	for bit:=start;bit<start+need;bit++{nextBitmap[bit/8]|=1<<uint(bit%8)}
	nextHeader:=a.header;nextHeader.Generation++;nextHeader.BitmapCRC=crc32.Checksum(nextBitmap,crcTable)
	d.ArenaStart=nextHeader.RegionStart+start
	if d.LifecyclePages==0{d.LifecycleStart=0}else{d.LifecycleStart=d.ArenaStart+d.ArenaPages}
	if err=d.ValidatePool(a.sb);err!=nil{return d,err}
	desc,err:=d.Encode();if err!=nil{return d,err}
	images,err:=a.bitmapHeaderImages(nextBitmap,nextHeader);if err!=nil{return d,err}
	images=append(images,PageImage{Offset:pair,Data:desc},PageImage{Offset:pair+format.PageSize,Data:append([]byte(nil),desc...)})
	if len(images)>MaxMetadataTransactionPages{return d,ErrMetadataTransactionTooLarge}
	if err=committer.CommitImages(images);err!=nil{return d,err}
	a.bitmap,a.header=nextBitmap,nextHeader
	a.headerSlot=1-a.headerSlot
	return d,nil
}

 // ExpandRegion extends the Reserved page bitmap after MetadataView growth.
 // It can reuse the existing bitmap layout only while BitmapPages remains
 // unchanged; otherwise an explicit allocator-layout migration is required.
 func(a *DynamicVolumeAllocator) ExpandRegion(dev Device,nextSB format.DynamicSuperblock,committer ClusterSafeCommitter) error {
	if a==nil||dev==nil||committer==nil||!committer.ClusterSafe(){return ErrClusterSafeCommitterRequired}
	a.mu.Lock();defer a.mu.Unlock()
	if err:=nextSB.Validate();err!=nil{return err}
	if nextSB.PoolUUID!=a.sb.PoolUUID||nextSB.Generation!=a.sb.Generation+1||
		nextSB.Geometry.ReservedOffset!=a.sb.Geometry.ReservedOffset||
		nextSB.Geometry.MetadataBytes<a.sb.Geometry.MetadataBytes{return ErrGeometry}
	expected,bitmapPages,err:=dynamicReservedGeometry(nextSB);if err!=nil{return err}
	if bitmapPages!=a.header.BitmapPages||expected.AllocStart!=a.header.AllocStart||
		expected.RegionPages<a.header.RegionPages||expected.RegionPages>uint64(len(a.bitmap))*8{return errors.New("Reserved allocator bitmap capacity requires layout migration")}
	if sized,ok:=dev.(interface{Size()int64});ok&&(sized.Size()<0||uint64(sized.Size())<nextSB.Geometry.MetadataBytes){return ErrGeometry}
	if expected.RegionPages==a.header.RegionPages{a.dev=dev;a.sb=nextSB;return nil}
	nextBitmap:=append([]byte(nil),a.bitmap...)
	for bit:=a.header.RegionPages;bit<expected.RegionPages;bit++{
		mask:=byte(1<<uint(bit%8))
		if nextBitmap[bit/8]&mask==0{return ErrCorrupt}
		nextBitmap[bit/8]&^=mask
	}
	nextHeader:=a.header;nextHeader.Generation++;nextHeader.RegionPages=expected.RegionPages
	nextHeader.BitmapCRC=crc32.Checksum(nextBitmap,crcTable)
	images,err:=a.bitmapHeaderImages(nextBitmap,nextHeader);if err!=nil{return err}
	if err=committer.CommitImages(images);err!=nil{return err}
	a.dev=dev;a.sb=nextSB;a.bitmap=nextBitmap;a.header=nextHeader;a.headerSlot=1-a.headerSlot
	return nil
}

// GrowVolume expands one contiguous Arena into immediately adjacent free
// Reserved pages. Existing Lifecycle pages are copied to their new tail
// location and synced before the descriptor/bitmap transaction is committed.
func(a *DynamicVolumeAllocator) GrowVolume(index uint32,pages uint64,committer ClusterSafeCommitter)(format.DynamicVolumeDescriptor,uint64,error){
	var zero format.DynamicVolumeDescriptor
	if a==nil||committer==nil||!committer.ClusterSafe()||pages==0{return zero,0,ErrClusterSafeCommitterRequired}
	a.mu.Lock();defer a.mu.Unlock()
	pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,index);if err!=nil{return zero,0,err}
	d,slot,err:=a.readDescriptorWithSlot(pair);if err!=nil{return zero,0,err}
	if d.Generation==math.MaxUint64||d.VolumeSequence==math.MaxUint64||d.ArenaPages>math.MaxUint64-pages{return zero,0,errors.New("Volume Arena sequence or size exhausted")}
	if d.ArenaStart<a.header.RegionStart{return zero,0,ErrGeometry}
	start:=d.ArenaStart-a.header.RegionStart
	used:=d.ArenaPages+d.LifecyclePages
	if used<d.ArenaPages||start>a.header.RegionPages||used>a.header.RegionPages-start{return zero,0,ErrGeometry}
	end:=start+used
	if pages>a.header.RegionPages-end{return zero,0,ErrDynamicVolumeSpace}
	nextBitmap:=append([]byte(nil),a.bitmap...)
	for bit:=end;bit<end+pages;bit++{
		if nextBitmap[bit/8]&(1<<uint(bit%8))!=0{return zero,0,ErrDynamicVolumeSpace}
		nextBitmap[bit/8]|=1<<uint(bit%8)
	}
	next:=d;next.Generation++;next.VolumeSequence++;next.ArenaPages+=pages
	if next.LifecyclePages!=0{
		oldLifecycle:=make([]byte,next.LifecyclePages*format.PageSize)
		if err=readFullAt(a.dev,oldLifecycle,int64(d.LifecycleStart*format.PageSize));err!=nil{return zero,0,err}
		next.LifecycleStart=d.LifecycleStart+pages
		if err=writeFullAt(a.dev,oldLifecycle,int64(next.LifecycleStart*format.PageSize));err!=nil{return zero,0,fmt.Errorf("relocate Volume lifecycle pages: %w",err)}
		if err=a.dev.Sync();err!=nil{return zero,0,fmt.Errorf("sync relocated Volume lifecycle pages: %w",err)}
	}
	if err=next.ValidatePool(a.sb);err!=nil{return zero,0,err}
	desc,err:=next.Encode();if err!=nil{return zero,0,err}
	nextHeader:=a.header;nextHeader.Generation++;nextHeader.BitmapCRC=crc32.Checksum(nextBitmap,crcTable)
	images,err:=a.bitmapHeaderImages(nextBitmap,nextHeader);if err!=nil{return zero,0,err}
	newSlot:=uint64(1)-slot
	images=append(images,PageImage{Offset:pair+newSlot*format.PageSize,Data:desc})
	if len(images)>MaxMetadataTransactionPages{return zero,0,ErrMetadataTransactionTooLarge}
	if err=committer.CommitImages(images);err!=nil{return zero,0,err}
	a.bitmap=nextBitmap;a.header=nextHeader;a.headerSlot=1-a.headerSlot
	return next,newSlot,nil
}

func(a *DynamicVolumeAllocator)readDescriptorWithSlot(pair uint64)(format.DynamicVolumeDescriptor,uint64,error){
	var best format.DynamicVolumeDescriptor
	var bestSlot uint64
	found:=false
	for slot:=uint64(0);slot<2;slot++{
		b:=make([]byte,format.PageSize);if err:=readFullAt(a.dev,b,int64(pair+slot*format.PageSize));err!=nil{return best,0,err}
		d,err:=format.DecodeDynamicVolumeDescriptor(b);if err!=nil{continue}
		if err=d.ValidatePool(a.sb);err!=nil{continue}
		if found&&d.VolumeID!=best.VolumeID{return best,0,ErrCorrupt}
		if found&&d.Generation==best.Generation&&d!=best{return best,0,ErrCorrupt}
		if !found||d.Generation>best.Generation{best,bestSlot,found=d,slot,true}
	}
	if !found{return best,0,ErrUnformatted}
	return best,bestSlot,nil
}

// DynamicVolumeAllocatorPresent distinguishes a legacy Reserved region from
// a formatted or interrupted new allocator; callers must never fall back to
// the legacy slot-0 Arena grower when either marker is present.
func DynamicVolumeAllocatorPresent(dev Device,sb format.DynamicSuperblock)bool{
	if dev==nil{return false}
	off:=int64(sb.Geometry.ReservedOffset)
	page:=make([]byte,format.PageSize)
	if err:=readFullAt(dev,page,off);err!=nil{return false}
	magic:=binary.LittleEndian.Uint64(page[:8])
	return magic==dynamicReservedMagic||magic==dynamicReservedFormatMagic
}

// ReleaseVolume never trusts a timeout or a caller-provided boolean. A platform
// fencing verifier must prove that every initiator for the Volume is fenced
// before its Arena pages can be returned to the shared Reserved region.
type DynamicVolumeFencingProof interface {
	ProveVolumeFenced(context.Context,string,uint64) error
}

func(a *DynamicVolumeAllocator) ReleaseVolume(ctx context.Context,index uint32,proof DynamicVolumeFencingProof,committer ClusterSafeCommitter) error {
	if a==nil||ctx==nil||proof==nil||committer==nil||!committer.ClusterSafe(){return errors.New("allocator, context, fencing proof and cluster-safe committer are required")}
	a.mu.Lock();defer a.mu.Unlock()
	pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,index);if err!=nil{return err}
	d,err:=a.readDescriptor(pair);if err!=nil{return err}
	if err=proof.ProveVolumeFenced(ctx,d.VolumeID,d.OwnerEpoch);err!=nil{return fmt.Errorf("refuse Reserved-region reuse before Volume fencing: %w",err)}
	if a.header.Generation==math.MaxUint64{return errors.New("Reserved allocator generation exhausted")}
	if d.ArenaStart<a.header.RegionStart{return ErrGeometry}
	start:=d.ArenaStart-a.header.RegionStart
	need:=d.ArenaPages+d.LifecyclePages
	if need<d.ArenaPages||start<a.header.AllocStart||need==0||start>a.header.RegionPages||need>a.header.RegionPages-start{return ErrGeometry}
	nextBitmap:=append([]byte(nil),a.bitmap...)
	for bit:=start;bit<start+need;bit++{
		if nextBitmap[bit/8]&(1<<uint(bit%8))==0{return ErrCorrupt}
		nextBitmap[bit/8]&^=1<<uint(bit%8)
	}
	nextHeader:=a.header;nextHeader.Generation++;nextHeader.BitmapCRC=crc32.Checksum(nextBitmap,crcTable)
	zero:=make([]byte,format.PageSize)
	images:=[]PageImage{{Offset:pair,Data:zero},{Offset:pair+format.PageSize,Data:append([]byte(nil),zero...)}}
	bitmapImages,err:=a.bitmapHeaderImages(nextBitmap,nextHeader);if err!=nil{return err}
	images=append(images,bitmapImages...)
	if len(images)>MaxMetadataTransactionPages{return ErrMetadataTransactionTooLarge}
	if err=committer.CommitImages(images);err!=nil{return err}
	a.bitmap,a.header=nextBitmap,nextHeader;a.headerSlot=1-a.headerSlot
	return nil
}

func(a *DynamicVolumeAllocator) ensureUniqueVolumeID(id string,except uint32)error{
	if id==""{return errors.New("Volume ID is required")}
	count:=uint32(a.sb.Geometry.NodeTableSize/(2*format.PageSize))
	for i:=uint32(0);i<count;i++{
		if i==except{continue}
		pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,i);if err!=nil{return err}
		d,err:=a.readDescriptor(pair)
		if err==nil&&d.VolumeID==id{return errors.New("BMPOOL03 Volume ID is already allocated")}
		if err!=nil&&!errors.Is(err,ErrUnformatted){return fmt.Errorf("inspect Volume descriptor slot %d: %w",i,err)}
	}
	return nil
}
func(a *DynamicVolumeAllocator) readDescriptor(pair uint64)(format.DynamicVolumeDescriptor,error){
	var best format.DynamicVolumeDescriptor
	found:=false
	for slot:=uint64(0);slot<2;slot++{
		b:=make([]byte,format.PageSize);if err:=readFullAt(a.dev,b,int64(pair+slot*format.PageSize));err!=nil{return best,err}
		d,err:=format.DecodeDynamicVolumeDescriptor(b);if err!=nil{continue}
		if err=d.ValidatePool(a.sb);err!=nil{continue}
		if found&&d.VolumeID!=best.VolumeID{return best,ErrCorrupt}
		if !found||d.Generation>best.Generation{best=d;found=true}
	}
	if !found{return best,ErrUnformatted}
	return best,nil
}
func(a *DynamicVolumeAllocator)findRun(need uint64)(uint64,bool){
	if need==0||need>a.header.RegionPages-a.header.AllocStart{return 0,false}
	for start:=a.header.AllocStart;start+need<=a.header.RegionPages;start++{
		free:=true
		for bit:=start;bit<start+need;bit++{if a.bitmap[bit/8]&(1<<uint(bit%8))!=0{free=false;start=bit;break}}
		if free{return start,true}
	}
	return 0,false
}
func(a *DynamicVolumeAllocator)bitmapHeaderImages(bitmap []byte,h dynamicReservedHeader)([]PageImage,error){
	var images []PageImage
	for i:=uint64(0);i<h.BitmapPages;i++{
		start:=i*format.PageSize;end:=start+format.PageSize
		if string(bitmap[start:end])==string(a.bitmap[start:end]){continue}
		images=append(images,PageImage{Offset:(h.RegionStart+2)*format.PageSize+start,Data:append([]byte(nil),bitmap[start:end]...)})
	}
	page,err:=h.encode();if err!=nil{return nil,err}
	images=append(images,PageImage{Offset:(h.RegionStart+(1-a.headerSlot))*format.PageSize,Data:page})
	return images,nil
}


func dynamicVolumeAllZero(b []byte) bool {
	for _,v:=range b { if v!=0{return false} }
	return true
}
