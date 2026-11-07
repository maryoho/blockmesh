package allocator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/lock"
	"context"
)

var ErrDynamicGeometryRequiresMigration=errors.New("BMPOOL03 growth exceeds reserved AG geometry; metadata layout migration is required")

// BuildDynamicGeometryUpdate replans a new logical view generation while
// preserving every existing metadata-region offset. AGCapacity reserves stable
// WAL, Bitmap and Checkpoint slots so AGCount can grow without moving metadata.
func BuildDynamicGeometryUpdate(current format.DynamicSuperblock,metadataBytes,dataBytes,viewGeneration uint64)(format.DynamicSuperblock,error){
	var zero format.DynamicSuperblock
	if err:=current.Validate();err!=nil{return zero,err}
	if metadataBytes<current.Geometry.MetadataBytes||dataBytes<current.Geometry.DataBytes||viewGeneration<=current.ViewGeneration{
		return zero,errors.New("BMPOOL03 geometry update cannot shrink views or regress ViewGeneration")
	}
	if current.Generation==math.MaxUint64{return zero,errors.New("BMPOOL03 superblock generation exhausted")}
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{
		DataBytes:dataBytes,MetadataBytes:metadataBytes,AGSizeBytes:current.Geometry.AGSizeBlocks*format.BlockSize,
		WALSegmentBytes:current.Geometry.WALSegmentBytes,MappingBytes:current.Geometry.MappingSize,
		CheckpointBytes:current.Geometry.CheckpointSize, AGCapacity:current.Geometry.AGCapacity,
	})
	if err!=nil{return zero,err}
	old:=current.Geometry
	if g.AGCapacity!=old.AGCapacity||g.AGSizeBlocks!=old.AGSizeBlocks||
		g.NodeTableOffset!=old.NodeTableOffset||g.NodeTableSize!=old.NodeTableSize||
		g.WALOffset!=old.WALOffset||g.WALSize!=old.WALSize||
		g.BitmapOffset!=old.BitmapOffset||g.BitmapSize!=old.BitmapSize||g.BitmapStride!=old.BitmapStride||
		g.BitmapPaddedBytes!=old.BitmapPaddedBytes||g.MappingOffset!=old.MappingOffset||
		g.MappingSize!=old.MappingSize||g.CheckpointOffset!=old.CheckpointOffset||
		g.CheckpointSize!=old.CheckpointSize||g.ReservedOffset!=old.ReservedOffset{
		return zero,ErrDynamicGeometryRequiresMigration
	}
	next,err:=format.NewDynamicSuperblock(g,current.PoolUUID,current.Generation+1,viewGeneration)
	return next,err
}

// PlanDynamicBitmapGrow clears only the bitmap bits that were padding in the
// old final AG and become valid DataView blocks in the new geometry. The
// caller must commit these after-images before publishing the new superblock.
func PlanDynamicBitmapGrow(r interface{ReadAt([]byte,int64)(int,error)},oldSB,newSB format.DynamicSuperblock)([]metastore.PageImage,error){
	if r==nil||oldSB.Validate()!=nil||newSB.Validate()!=nil{return nil,errors.New("invalid BMPOOL03 bitmap growth request")}
	oldG,newG:=oldSB.Geometry,newSB.Geometry
	if newG.AGCount<oldG.AGCount||oldG.AGCapacity!=newG.AGCapacity||oldG.BitmapOffset!=newG.BitmapOffset||oldG.BitmapStride!=newG.BitmapStride||
		oldG.BitmapPaddedBytes!=newG.BitmapPaddedBytes||newG.AGSizeBlocks!=oldG.AGSizeBlocks||
		newG.TotalBlocks<oldG.TotalBlocks{return nil,ErrDynamicGeometryRequiresMigration}
	ag:=oldG.AGCount-1
	newValid,err:=newG.AGBlocks(ag);if err!=nil{return nil,err}
	if newValid<oldG.LastAGBlocks{return nil,ErrDynamicGeometryRequiresMigration}
	if newValid==oldG.LastAGBlocks{return nil,nil}
	base:=oldG.BitmapOffset+uint64(ag)*oldG.BitmapStride
	desc:=make([]byte,format.PageSize)
	if err:=readAtFull(r,desc,int64(base));err!=nil{return nil,err}
	if binary.LittleEndian.Uint64(desc[:8])!=bitmapDescriptorMagic||binary.LittleEndian.Uint32(desc[8:12])!=ag{return nil,errors.New("BMPOOL03 final AG bitmap descriptor invalid")}
	seq:=binary.LittleEndian.Uint64(desc[16:24])
	bitmap:=make([]byte,oldG.BitmapPaddedBytes)
	if err:=readAtFull(r,bitmap,int64(base+format.PageSize));err!=nil{return nil,err}
	if crc32.Checksum(bitmap,castagnoli)!=binary.LittleEndian.Uint32(desc[24:28]){return nil,errors.New("BMPOOL03 final AG bitmap CRC32C mismatch")}
	changed:=false
	for bit:=oldG.LastAGBlocks;bit<newValid;bit++{
		mask:=byte(1<<uint(bit%8))
		// These bits were padding under oldG and could never have been
		// allocated. A clear bit therefore means an earlier growth attempt
		// durably committed this transition before crashing. Treat it as
		// already initialized so publication can be retried idempotently.
		if bitmap[bit/8]&mask!=0{bitmap[bit/8]&^=mask;changed=true}
	}
	if !changed{return nil,nil}
	if seq==math.MaxUint64{return nil,errors.New("BMPOOL03 bitmap sequence exhausted")}
	images:=make([]metastore.PageImage,0,int(oldG.BitmapPaddedBytes/format.PageSize)+1)
	for p:=uint64(0);p<oldG.BitmapPaddedBytes/format.PageSize;p++{
		off:=base+format.PageSize+p*format.PageSize
		page:=append([]byte(nil),bitmap[p*format.PageSize:(p+1)*format.PageSize]...) 
		images=append(images,metastore.PageImage{Offset:off,Data:page})
	}
	binary.LittleEndian.PutUint64(desc[16:24],seq+1)
	binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
	images=append(images,metastore.PageImage{Offset:base,Data:desc})
	return images,nil
}
// PublishDynamicGeometry commits newly exposed final-AG bitmap bits first,
// then publishes the new geometry to the older superblock copy and finally
// mirrors it to the previous active copy. A crash at any point leaves either
// the old valid geometry or at least one complete new copy.
func PublishDynamicGeometry(dev device.Device,current,next format.DynamicSuperblock,bitmapCommitter metastore.Committer)error{
	return publishDynamicGeometryChecked(dev,current,next,bitmapCommitter,nil)
}

func publishDynamicGeometryChecked(dev device.Device,current,next format.DynamicSuperblock,bitmapCommitter metastore.Committer,guard func()error)error{
	if dev==nil{return errors.New("nil BMPOOL03 MetadataView")}
	if err:=current.Validate();err!=nil{return err};if err:=next.Validate();err!=nil{return err}
	if next.PoolUUID!=current.PoolUUID||next.Generation!=current.Generation+1||
		next.ViewGeneration<=current.ViewGeneration||next.Geometry.MetadataBytes<current.Geometry.MetadataBytes||
		next.Geometry.DataBytes<current.Geometry.DataBytes||next.Geometry.AGCount<current.Geometry.AGCount||
		next.Geometry.AGCapacity!=current.Geometry.AGCapacity||next.Geometry.AGSizeBlocks!=current.Geometry.AGSizeBlocks{return errors.New("BMPOOL03 geometry publication identity/generation mismatch")}
	if uint64(dev.Size())<next.Geometry.MetadataBytes{return errors.New("expanded MetadataView is smaller than new geometry")}
	if !sameDynamicMetadataLayout(current.Geometry,next.Geometry){return ErrDynamicGeometryRequiresMigration}
	if guard!=nil{if err:=guard();err!=nil{return err}}
	if next.Geometry.DataBytes>current.Geometry.DataBytes{
		if bitmapCommitter==nil{return errors.New("bitmap WAL committer required before publishing Data growth")}
		images,err:=PlanDynamicBitmapGrow(dev,current,next);if err!=nil{return err}
		if len(images)>0{if err=bitmapCommitter.CommitImages(images);err!=nil{return fmt.Errorf("publish newly exposed Data bitmap bits: %w",err)}}
	}
	if next.Geometry.AGCount>current.Geometry.AGCount {
		if err:=initializeDynamicAGSlots(dev,current.Geometry,next.Geometry);err!=nil{return fmt.Errorf("initialize newly reserved AG slots: %w",err)}
	}
	if _,err:=OpenDynamicSuperblockPair(dev);err!=nil{return err}
	selected,slot,err:=readDynamicSuperblockPairWithSlot(dev);if err!=nil{return err}
	if selected!=current{return errors.New("stale BMPOOL03 geometry publisher: current on-disk superblock changed")}
	encoded,err:=next.Encode();if err!=nil{return err}
	if guard!=nil{if err=guard();err!=nil{return err}}
	inactive:=uint64(1)-slot
	if err=writeDynamicDeviceFull(dev,encoded,int64(inactive*format.PageSize));err!=nil{return fmt.Errorf("publish first BMPOOL03 superblock copy: %w",err)}
	if err=dev.Sync();err!=nil{return fmt.Errorf("sync first BMPOOL03 superblock copy: %w",err)}
	if guard!=nil{if err=guard();err!=nil{return err}}
	if err=writeDynamicDeviceFull(dev,encoded,int64(slot*format.PageSize));err!=nil{return fmt.Errorf("publish second BMPOOL03 superblock copy: %w",err)}
	if err=dev.Sync();err!=nil{return fmt.Errorf("sync second BMPOOL03 superblock copy: %w",err)}
	return nil
}


// PublishDynamicGeometryUnderLease is the shared-SAN entry point. It holds the
// final AG lock across bitmap-tail WAL commit and both superblock publications,
// and refuses to proceed without a live Owner lease and distributed locker.
func PublishDynamicGeometryUnderLease(ctx context.Context,dev device.Device,current,next format.DynamicSuperblock,
	ownerLease lock.Lease,locks lock.DistributedLocker,bitmapCommitter metastore.Committer)error{
	if ctx==nil||ownerLease==nil||!ownerLease.Valid()||ownerLease.Resource()==""||locks==nil||!locks.Distributed(){
		return errors.New("live Pool Owner lease and distributed AG locker are required")
	}
	if view,ok:=dev.(interface{Generation()uint64});ok&&view.Generation()!=0&&view.Generation()!=next.ViewGeneration{return errors.New("MetadataView Extent Map generation does not match new BMPOOL03 geometry")}
	if current.Geometry.AGCount==0{return format.ErrInvalidDynamicGeometry}
	ag:=current.Geometry.AGCount-1
	unlock,err:=locks.Lock(ctx,fmt.Sprintf("ag:%03d",ag));if err!=nil{return fmt.Errorf("lock final AG %d for geometry growth: %w",ag,err)}
	defer unlock()
	if err=ctx.Err();err!=nil{return err}
	if !ownerLease.Valid(){return errors.New("Pool Owner lease lost before geometry publication")}
	guard:=func()error{if !ownerLease.Valid(){return errors.New("Pool Owner lease lost during geometry publication")};return nil}
	err=publishDynamicGeometryChecked(dev,current,next,bitmapCommitter,guard)
	if err!=nil{return err}
	if !ownerLease.Valid(){return errors.New("Pool Owner lease lost during geometry publication; fence and inspect both superblock copies")}
	return nil
}

func sameDynamicMetadataLayout(a,b format.DynamicGeometry)bool{
	return a.AGCapacity==b.AGCapacity&&a.AGSizeBlocks==b.AGSizeBlocks&&a.NodeTableOffset==b.NodeTableOffset&&
		a.NodeTableSize==b.NodeTableSize&&a.WALOffset==b.WALOffset&&a.WALSize==b.WALSize&&
		a.WALSegmentBytes==b.WALSegmentBytes&&a.BitmapOffset==b.BitmapOffset&&a.BitmapSize==b.BitmapSize&&
		a.BitmapStride==b.BitmapStride&&a.BitmapPaddedBytes==b.BitmapPaddedBytes&&a.MappingOffset==b.MappingOffset&&
		a.MappingSize==b.MappingSize&&a.CheckpointOffset==b.CheckpointOffset&&a.CheckpointSize==b.CheckpointSize&&
		a.ReservedOffset==b.ReservedOffset
}

func readDynamicSuperblockPairWithSlot(dev device.Device)(format.DynamicSuperblock,uint64,error){
	var zero format.DynamicSuperblock
	type candidate struct{sb format.DynamicSuperblock;slot uint64}
	var valid []candidate
	for slot:=uint64(0);slot<2;slot++{
		sb,err:=format.ReadDynamicSuperblock(dev,int64(slot*format.PageSize));if err==nil{valid=append(valid,candidate{sb,slot})}
	}
	if len(valid)==0{return zero,0,errors.New("no valid BMPOOL03 superblock copy")}
	if len(valid)==2{
		if valid[0].sb.PoolUUID!=valid[1].sb.PoolUUID{return zero,0,errors.New("BMPOOL03 superblock copies have different Pool UUIDs")}
		if valid[0].sb.Generation==valid[1].sb.Generation&&valid[0].sb!=valid[1].sb{return zero,0,errors.New("conflicting BMPOOL03 superblock copies")}
		if valid[1].sb.Generation>valid[0].sb.Generation{return valid[1].sb,valid[1].slot,nil}
	}
	return valid[0].sb,valid[0].slot,nil
}

func writeDynamicDeviceFull(dev device.Device,b []byte,off int64)error{
	n,err:=dev.WriteAt(b,off);if err!=nil{return err};if n!=len(b){return io.ErrShortWrite};return nil
}


// initializeDynamicAGSlots initializes previously reserved AG slots while they
// are still unreachable through the old Superblock. It is idempotent: after a
// crash, the old AGCount remains authoritative and retry may reinitialize any
// slot that was not yet made visible.
func initializeDynamicAGSlots(dev device.Device, oldG, newG format.DynamicGeometry) error {
	for ag:=oldG.AGCount; ag<newG.AGCount; ag++ {
		seg:=newG.WALOffset+uint64(ag)*newG.WALSegmentBytes
		if err:=zeroDynamicRangeDevice(dev,seg,newG.WALSegmentBytes); err!=nil{return fmt.Errorf("zero AG %d WAL slot: %w",ag,err)}
		valid,err:=newG.AGBlocks(ag); if err!=nil{return err}
		bitmap:=make([]byte,newG.BitmapPaddedBytes)
		for bit:=valid; bit<uint64(len(bitmap))*8; bit++ { bitmap[bit/8] |= 1<<uint(bit%8) }
		base:=newG.BitmapOffset+uint64(ag)*newG.BitmapStride
		desc:=make([]byte,format.PageSize)
		binary.LittleEndian.PutUint64(desc[:8],bitmapDescriptorMagic)
		binary.LittleEndian.PutUint32(desc[8:12],ag)
		binary.LittleEndian.PutUint64(desc[16:24],1)
		binary.LittleEndian.PutUint32(desc[24:28],crc32.Checksum(bitmap,castagnoli))
		if err=writeDynamicDeviceFull(dev,bitmap,int64(base+format.PageSize));err!=nil{return err}
		if err=writeDynamicDeviceFull(dev,desc,int64(base));err!=nil{return err}
		cp:=(format.DynamicCheckpoint{AG:ag,Generation:1,ActiveBank:0})
		page,err:=cp.Encode();if err!=nil{return err}
		for slot:=uint32(0);slot<2;slot++ {
			off,err:=format.DynamicCheckpointOffset(newG,ag,slot);if err!=nil{return err}
			if err=writeDynamicDeviceFull(dev,page,int64(off));err!=nil{return err}
		}
	}
	return dev.Sync()
}

func zeroDynamicRangeDevice(dev device.Device, off, length uint64) error {
	zero:=make([]byte,1<<20)
	for done:=uint64(0);done<length; {
		n:=uint64(len(zero));if length-done<n{n=length-done}
		if err:=writeDynamicDeviceFull(dev,zero[:int(n)],int64(off+done));err!=nil{return err}
		done+=n
	}
	return nil
}
