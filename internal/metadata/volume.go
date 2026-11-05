package metadata

import (
	"errors"
	"fmt"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

var ErrVolumeSlotOccupied = errors.New("Volume Descriptor slot is already initialized")

// InitVolume reserves the deterministic extent assigned to one Node Table
// index and writes its redundant BMDVOL02 descriptor copies. The caller must
// hold an external Pool administration lock and ensure no node is using it.
func InitVolume(dev device.Device, sb format.Superblock, index uint32, volumeID string, ownerEpoch uint64, lifecyclePages uint64) (format.VolumeDescriptor, error) {
	var zero format.VolumeDescriptor
	if dev == nil || volumeID == "" || ownerEpoch == 0 {
		return zero, errors.New("device, Volume ID and non-zero Owner Epoch are required")
	}
	if err := sb.Validate(); err != nil { return zero, fmt.Errorf("invalid Pool Superblock: %w", err) }
	if dev.Size()<0 || uint64(dev.Size())<sb.MetadataSize { return zero,errors.New("metadata device is smaller than Pool geometry") }
	if lifecyclePages == 0 { lifecyclePages = 32 }
	if lifecyclePages < 2 || lifecyclePages > 32 { return zero, errors.New("lifecycle pages must be in [2,32]") }
	pair, err := format.VolumeDescriptorPairOffset(sb,index)
	if err != nil { return zero,err }
	// The Reserved region is divided into equal, page-aligned extents. This is
	// a bootstrap allocator; later Pool extent management may replace it.
	reservedBytes:=sb.MetadataSize-sb.ReservedOffset
	slotBytes:=(reservedBytes/uint64(format.MaxVolumeDescriptors))/format.PageSize*format.PageSize
	if slotBytes/format.PageSize<=lifecyclePages+format.ReaderTablePages+format.OwnerRecordPages+1 { return zero,errors.New("reserved Pool metadata is too small for OwnerRecord, Arena, Lifecycle and Reader Table extents") }
	start:=sb.ReservedOffset+uint64(index)*slotBytes
	ownerRecordStart:=start/format.PageSize
	arenaPages:=slotBytes/format.PageSize-lifecyclePages-format.ReaderTablePages-format.OwnerRecordPages
	arenaStart:=ownerRecordStart+format.OwnerRecordPages
	lifecycleStart:=arenaStart+arenaPages
	readerTableStart:=lifecycleStart+lifecyclePages
	for slot:=uint64(0);slot<2;slot++ {
		page:=make([]byte,format.PageSize)
		if err:=readAtFull(dev,page,int64(pair+slot*format.PageSize));err!=nil{return zero,err}
		for _,b:=range page { if b!=0 { return zero,ErrVolumeSlotOccupied } }
	}
	d:=format.VolumeDescriptor{
		VolumeID:volumeID,AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:sb.MappingOffset,BitmapSequence:1,OwnerEpoch:ownerEpoch,
		ArenaStart:arenaStart,ArenaPages:arenaPages,RootPage:^uint64(0),
		LifecycleStart:lifecycleStart,LifecyclePages:lifecyclePages,
	}
	if err=d.ValidatePool(sb);err!=nil{return zero,fmt.Errorf("invalid Volume Descriptor geometry: %w",err)}
	if err=validateVolumeDescriptorSlot(sb,index,d);err!=nil{return zero,fmt.Errorf("invalid Volume slot geometry: %w",err)}
	image,err:=d.Encode();if err!=nil{return zero,err}
	// Initialize both SAN OwnerRecord pages before making the Volume visible.
	// A zero pair represents "no previous Owner" for a newly initialized Volume.
	if err=zeroRegion(dev,ownerRecordStart*format.PageSize,format.OwnerRecordPages*format.PageSize);err!=nil{return zero,err}
	// Initialize the lifecycle extent before making the descriptor visible.
	if err=zeroRegion(dev,lifecycleStart*format.PageSize,lifecyclePages*format.PageSize);err!=nil{return zero,err}
	// Reader slots are one durable page each and must start Empty even when
	// this Node Table slot is being initialized on a reused Pool.
	if err=zeroRegion(dev,readerTableStart*format.PageSize,format.ReaderTablePages*format.PageSize);err!=nil{return zero,err}
	if err=writeAtFull(dev,image,int64(pair));err!=nil{return zero,err}
	if err=dev.Sync();err!=nil{return zero,err}
	if err=writeAtFull(dev,image,int64(pair+format.PageSize));err!=nil{return zero,err}
	if err=dev.Sync();err!=nil{return zero,err}
	return d,nil
}

// ReadVolumeDescriptor returns the newest valid redundant descriptor copy.
// A valid but conflicting same-generation pair is treated as corruption.
func ReadVolumeDescriptor(dev device.Device, sb format.Superblock, index uint32) (format.VolumeDescriptor,error) {
	var zero format.VolumeDescriptor
	if dev==nil{return zero,errors.New("nil metadata device")}
	if dev.Size()<0 || uint64(dev.Size())<sb.MetadataSize { return zero,errors.New("metadata device is smaller than Pool geometry") }
	pair,err:=format.VolumeDescriptorPairOffset(sb,index);if err!=nil{return zero,err}
	var best format.VolumeDescriptor
	found:=false
	for slot:=uint64(0);slot<2;slot++ {
		b:=make([]byte,format.PageSize)
		if err:=readAtFull(dev,b,int64(pair+slot*format.PageSize));err!=nil{return zero,err}
		d,decodeErr:=format.DecodeVolumeDescriptor(b)
		if decodeErr!=nil { continue }
		if found && d.VolumeID!=best.VolumeID { return zero,errors.New("Volume Descriptor copies contain different Volume IDs") }
		if found && d.Generation==best.Generation && d!=best { return zero,fmt.Errorf("conflicting Volume Descriptor copies at generation %d",d.Generation) }
		if !found || d.Generation>best.Generation { best=d;found=true }
	}
	if !found{return zero,format.ErrVolumeDescriptor}
	if err:=best.ValidatePool(sb);err!=nil{return zero,fmt.Errorf("Volume Descriptor does not fit Pool geometry: %w",err)}
	if err:=validateVolumeDescriptorSlot(sb,index,best);err!=nil{return zero,fmt.Errorf("Volume Descriptor crosses its reserved slot: %w",err)}
	return best,nil
}

func validateVolumeDescriptorSlot(sb format.Superblock,index uint32,d format.VolumeDescriptor) error {
	if uint64(index)>=format.MaxVolumeDescriptors { return format.ErrVolumeDescriptor }
	reservedBytes:=sb.MetadataSize-sb.ReservedOffset
	slotBytes:=(reservedBytes/uint64(format.MaxVolumeDescriptors))/format.PageSize*format.PageSize
	slotPages:=slotBytes/format.PageSize
	slotStart:=sb.ReservedOffset/format.PageSize+uint64(index)*slotPages
	slotEnd:=slotStart+slotPages
	ownerEnd:=slotStart+format.OwnerRecordPages
	readerStart:=slotEnd-format.ReaderTablePages
	if ownerEnd>slotEnd || d.ArenaStart<ownerEnd || d.ArenaStart>readerStart || d.ArenaPages>readerStart-d.ArenaStart{return format.ErrVolumeDescriptor}
	if d.LifecyclePages!=0&&(d.LifecycleStart<d.ArenaStart+d.ArenaPages||d.LifecycleStart>readerStart||d.LifecyclePages>readerStart-d.LifecycleStart){return format.ErrVolumeDescriptor}
	return nil
}
