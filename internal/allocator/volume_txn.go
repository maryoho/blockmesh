package allocator

import (
 "context"
 "encoding/binary"
 "errors"
 "fmt"
 "hash/crc32"

 "blockmesh/internal/format"
 "blockmesh/internal/lock"
 "blockmesh/internal/wal"
)

const VolumeDescriptorPairSize = format.VolumeDescriptorPairSize
const MaxVolumeDescriptors = format.MaxVolumeDescriptors

var errVolumeDescriptor = format.ErrVolumeDescriptor

type VolumeDescriptor = format.VolumeDescriptor

func VolumeDescriptorPairOffset(sb format.Superblock,index uint32)(uint64,error) {
	return format.VolumeDescriptorPairOffset(sb,index)
}
func EncodeVolumeDescriptor(d VolumeDescriptor)([]byte,error) { return d.Encode() }
func DecodeVolumeDescriptor(b []byte)(VolumeDescriptor,error) { return format.DecodeVolumeDescriptor(b) }

func readVolumeDescriptorPair(store MetadataStore,pairOffset uint64,volumeID string)(VolumeDescriptor,uint64,error){
 sb:=store.Superblock();nodeEnd:=sb.NodeTableOffset+format.NodeTableSize
 if pairOffset<sb.NodeTableOffset||(pairOffset-sb.NodeTableOffset)%VolumeDescriptorPairSize!=0||pairOffset>nodeEnd||VolumeDescriptorPairSize>nodeEnd-pairOffset{return VolumeDescriptor{},0,fmt.Errorf("Volume Descriptor pair outside Node Table region")}
 var best VolumeDescriptor;var bestSlot uint64;found:=false
 for slot:=uint64(0);slot<2;slot++{
  b:=make([]byte,format.PageSize);if err:=readDeviceAtFull(store.Device(),b,int64(pairOffset+slot*format.PageSize));err!=nil{return VolumeDescriptor{},0,err}
  if allZeroPage(b){continue}
  d,err:=DecodeVolumeDescriptor(b);if err!=nil{continue}
  if d.VolumeID!=volumeID{return VolumeDescriptor{},0,fmt.Errorf("Volume Descriptor belongs to %q, not %q",d.VolumeID,volumeID)}
  if found&&d.Generation==best.Generation&&d!=best{return VolumeDescriptor{},0,fmt.Errorf("conflicting Volume Descriptor copies at generation %d",d.Generation)}
  if !found||d.Generation>best.Generation{best,bestSlot,found=d,slot,true}
 }
 if !found{return VolumeDescriptor{},0,fmt.Errorf("no valid Volume Descriptor copy")}
 return best,bestSlot,nil
}

func allZeroPage(b []byte)bool{for _,v:=range b{if v!=0{return false}};return true}

// AllocateOneTxn atomically commits the AG bitmap descriptor, changed bitmap
// page, Mapping page, and inactive Volume Descriptor. The descriptor is the
// final WAL after-image and publishes the new mapping root and sequences.
func (a *Allocator) AllocateOneTxn(ctx context.Context,lease lock.Lease,volumeID string,ownerEpoch uint64,ag uint32,mappingOffset,pairOffset uint64,buildMapping func([]byte,uint64)([]byte,error))(uint64,error){
 sb:=a.Store.Superblock()
 if volumeID==""||ownerEpoch==0{return 0,errors.New("Volume ID and non-zero owner epoch are required")}
 if lease==nil||!lease.Valid()||lease.Resource()!="volume:"+volumeID{return 0,errors.New("valid exclusive Volume Owner Lease required")}
 if ag>=sb.AGCount{return 0,fmt.Errorf("AG %d out of range",ag)}
 if mappingOffset%format.PageSize!=0||mappingOffset<sb.MappingOffset||mappingOffset>sb.CheckpointOffset||format.PageSize>sb.CheckpointOffset-mappingOffset{return 0,errors.New("mapping page offset outside Mapping region")}
 if buildMapping==nil{return 0,errors.New("mapping page builder is nil")}
 unlockVD,err:=a.Locks.Lock(ctx,fmt.Sprintf("volume-descriptor:%d",pairOffset));if err!=nil{return 0,fmt.Errorf("acquire Volume Descriptor lock: %w",err)};defer unlockVD()
 unlockAG,err:=a.Locks.Lock(ctx,fmt.Sprintf("ag:%03d",ag));if err!=nil{return 0,fmt.Errorf("acquire AG lock: %w",err)};defer unlockAG()
 unlockMapping,err:=a.Locks.Lock(ctx,fmt.Sprintf("mapping-page:%d",mappingOffset));if err!=nil{return 0,fmt.Errorf("acquire Mapping lock: %w",err)};defer unlockMapping()
 // Journal.New performs mandatory recovery. It must run before reading
 // the Volume Descriptor or any mutable AG metadata.
 journal,err:=wal.New(a.Store.Device(),sb,ag);if err!=nil{return 0,err}
 vd,activeSlot,err:=readVolumeDescriptorPair(a.Store,pairOffset,volumeID);if err!=nil{return 0,err}
 if vd.OwnerEpoch!=ownerEpoch{return 0,errors.New("Volume Descriptor OwnerEpoch mismatch")}
 if vd.AG>=sb.AGCount||vd.BitmapSequence==0{return 0,errors.New("Volume Descriptor AG/BitmapSequence is invalid")}
 if vd.MappingRootOffset%format.PageSize!=0||vd.MappingRootOffset<sb.MappingOffset||vd.MappingRootOffset>sb.CheckpointOffset||format.PageSize>sb.CheckpointOffset-vd.MappingRootOffset{return 0,errors.New("current Mapping root is outside Mapping region")}
 if mappingOffset==vd.MappingRootOffset{return 0,errors.New("new Mapping page must be a fresh COW page")}
 if vd.Generation==^uint64(0)||vd.VolumeSequence==^uint64(0){return 0,errors.New("Volume Descriptor sequence exhausted")}
 base:=sb.BitmapOffset+uint64(ag)*format.AGBitmapStride
 desc:=make([]byte,format.PageSize);if err=readDeviceAtFull(a.Store.Device(),desc,int64(base));err!=nil{return 0,err}
 if binary.LittleEndian.Uint64(desc[:8])!=bitmapDescriptorMagic||binary.LittleEndian.Uint32(desc[8:12])!=ag{return 0,fmt.Errorf("AG %d bitmap descriptor invalid",ag)}
 bitmapSeq:=binary.LittleEndian.Uint64(desc[16:24]);if bitmapSeq==^uint64(0){return 0,errors.New("bitmap sequence exhausted")}
 bitmap:=make([]byte,format.BitmapBytes);if err=readDeviceAtFull(a.Store.Device(),bitmap,int64(base+format.PageSize));err!=nil{return 0,err}
 if crc32.Checksum(bitmap,castagnoli)!=binary.LittleEndian.Uint32(desc[24:28]){return 0,fmt.Errorf("AG %d bitmap CRC32C mismatch",ag)}
 local,ok:=findFree(bitmap);if !ok{return 0,fmt.Errorf("AG %d is full",ag)}
 bitmap[local/8]|=byte(1<<(local%8));physical:=uint64(ag)*format.AGBlocks+local
 pageIndex:=local/(format.PageSize*8);bitmapPageOffset:=base+format.PageSize+pageIndex*format.PageSize
 bitmapPage:=append([]byte(nil),bitmap[pageIndex*format.PageSize:(pageIndex+1)*format.PageSize]...)
 newBitmapDesc:=append([]byte(nil),desc...);binary.LittleEndian.PutUint64(newBitmapDesc[16:24],bitmapSeq+1)
 binary.LittleEndian.PutUint32(newBitmapDesc[24:28],crc32.Checksum(bitmap,castagnoli))
 oldMapping,err:=a.Store.ReadPage(vd.MappingRootOffset);if err!=nil{return 0,err}
 newMapping,err:=buildMapping(oldMapping,physical);if err!=nil{return 0,fmt.Errorf("build Mapping page: %w",err)}
 if len(newMapping)!=int(format.PageSize){return 0,errors.New("Mapping builder must return exactly 4KiB")}
 next:=vd;next.AG=ag;next.Generation++;next.VolumeSequence++;next.MappingRootOffset=mappingOffset;next.BitmapSequence=bitmapSeq+1;next.OwnerEpoch=ownerEpoch
 vdImage,err:=EncodeVolumeDescriptor(next);if err!=nil{return 0,err}
 inactive:=uint64(1)-activeSlot
 updates:=[]wal.PageUpdate{{Offset:base,Image:newBitmapDesc},{Offset:bitmapPageOffset,Image:bitmapPage},{Offset:mappingOffset,Image:newMapping},{Offset:pairOffset+inactive*format.PageSize,Image:vdImage}}
 if err=journal.Commit(updates);err!=nil{return 0,fmt.Errorf("commit Bitmap/Mapping/Volume Descriptor transaction: %w",err)}
 return physical,nil
}
