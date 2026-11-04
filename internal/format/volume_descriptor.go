package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
)

const (
	VolumeDescriptorMagic = "BMDVOL02"
	VolumeDescriptorVersion = uint32(2)
	VolumeDescriptorPairSize = 2 * PageSize
	MaxVolumeDescriptors = NodeTableSize / VolumeDescriptorPairSize
	volumeDescriptorCRCOffset = int(PageSize) - 4
)

var ErrVolumeDescriptor = errors.New("invalid Volume Descriptor")

// VolumeDescriptor is the sole per-volume root and private metadata geometry
// record. Pool geometry remains exclusively in Pool Superblock LBA 0/1.
type VolumeDescriptor struct {
	VolumeID string
	AG uint32
	Generation uint64
	VolumeSequence uint64
	MappingRootOffset uint64
	BitmapSequence uint64
	OwnerEpoch uint64
	ArenaStart uint64
	ArenaPages uint64
	RootPage uint64
	MappingSequence uint64
	LifecycleStart uint64
	LifecyclePages uint64
}

func (d VolumeDescriptor) Encode() ([]byte, error) {
	if strings.TrimSpace(d.VolumeID)=="" || len(d.VolumeID)>64 || d.Generation==0 || d.VolumeSequence==0 || d.OwnerEpoch==0 || d.ArenaPages==0 || d.ArenaStart<2 || d.RootPage!=^uint64(0) && (d.RootPage<d.ArenaStart || d.RootPage-d.ArenaStart>=d.ArenaPages) {
		return nil, ErrVolumeDescriptor
	}
	if d.LifecyclePages==0 && d.LifecycleStart!=0 { return nil, ErrVolumeDescriptor }
	if d.LifecyclePages!=0 {
		if d.LifecyclePages<2 || d.LifecyclePages>32 || d.LifecycleStart<2 { return nil, ErrVolumeDescriptor }
		ae,le:=d.ArenaStart+d.ArenaPages,d.LifecycleStart+d.LifecyclePages
		if ae<d.ArenaStart || le<d.LifecycleStart || d.LifecycleStart<ae && le>d.ArenaStart { return nil, ErrVolumeDescriptor }
	}
	b:=make([]byte,PageSize)
	copy(b[:8],VolumeDescriptorMagic)
	binary.LittleEndian.PutUint32(b[8:12],VolumeDescriptorVersion)
	binary.LittleEndian.PutUint32(b[12:16],d.AG)
	binary.LittleEndian.PutUint64(b[16:24],d.Generation)
	binary.LittleEndian.PutUint64(b[24:32],d.VolumeSequence)
	binary.LittleEndian.PutUint64(b[32:40],d.MappingRootOffset)
	binary.LittleEndian.PutUint64(b[40:48],d.BitmapSequence)
	binary.LittleEndian.PutUint64(b[48:56],d.OwnerEpoch)
	copy(b[56:120],d.VolumeID)
	binary.LittleEndian.PutUint64(b[120:128],d.ArenaStart)
	binary.LittleEndian.PutUint64(b[128:136],d.ArenaPages)
	binary.LittleEndian.PutUint64(b[136:144],d.RootPage)
	binary.LittleEndian.PutUint64(b[144:152],d.MappingSequence)
	binary.LittleEndian.PutUint64(b[152:160],d.LifecycleStart)
	binary.LittleEndian.PutUint64(b[160:168],d.LifecyclePages)
	binary.LittleEndian.PutUint32(b[volumeDescriptorCRCOffset:],crc32.Checksum(b[:volumeDescriptorCRCOffset],castagnoli))
	return b,nil
}

func DecodeVolumeDescriptor(b []byte) (VolumeDescriptor,error) {
	var d VolumeDescriptor
	if len(b)!=int(PageSize) || string(b[:8])!=VolumeDescriptorMagic || binary.LittleEndian.Uint32(b[8:12])!=VolumeDescriptorVersion || crc32.Checksum(b[:volumeDescriptorCRCOffset],castagnoli)!=binary.LittleEndian.Uint32(b[volumeDescriptorCRCOffset:]) { return d,ErrVolumeDescriptor }
	name:=b[56:120]; end:=len(name)
	for i,c:=range name { if c==0 { end=i; break } }
	for _,c:=range name[end:] { if c!=0 { return d,ErrVolumeDescriptor } }
	for _,c:=range b[168:volumeDescriptorCRCOffset] { if c!=0 { return d,ErrVolumeDescriptor } }
	d=VolumeDescriptor{VolumeID:string(name[:end]),AG:binary.LittleEndian.Uint32(b[12:16]),Generation:binary.LittleEndian.Uint64(b[16:24]),VolumeSequence:binary.LittleEndian.Uint64(b[24:32]),MappingRootOffset:binary.LittleEndian.Uint64(b[32:40]),BitmapSequence:binary.LittleEndian.Uint64(b[40:48]),OwnerEpoch:binary.LittleEndian.Uint64(b[48:56]),ArenaStart:binary.LittleEndian.Uint64(b[120:128]),ArenaPages:binary.LittleEndian.Uint64(b[128:136]),RootPage:binary.LittleEndian.Uint64(b[136:144]),MappingSequence:binary.LittleEndian.Uint64(b[144:152]),LifecycleStart:binary.LittleEndian.Uint64(b[152:160]),LifecyclePages:binary.LittleEndian.Uint64(b[160:168])}
	encoded,err:=d.Encode()
	if err!=nil { return VolumeDescriptor{},err }
	_ = encoded
	return d,nil
}

func VolumeDescriptorPairOffset(sb Superblock,index uint32)(uint64,error) {
	if err:=sb.Validate();err!=nil{return 0,err}
	if uint64(index)>=MaxVolumeDescriptors { return 0, errors.New("Volume Descriptor index exceeds Node Table capacity") }
	if sb.NodeTableOffset!=NodeTableOffset { return 0, errors.New("unsupported Node Table offset") }
	return sb.NodeTableOffset+uint64(index)*VolumeDescriptorPairSize,nil
}

// ValidatePool verifies that descriptor offsets are contained in the Pool
// regions described by the authoritative BMPOOL02 superblock.
func (d VolumeDescriptor) ValidatePool(sb Superblock) error {
	if err:=sb.Validate();err!=nil{return err}
	if d.AG>=sb.AGCount || d.BitmapSequence==0 { return ErrVolumeDescriptor }
	if d.MappingRootOffset%PageSize!=0 || d.MappingRootOffset<sb.MappingOffset ||
		d.MappingRootOffset>sb.MappingOffset+sb.MappingSize ||
		PageSize>sb.MappingOffset+sb.MappingSize-d.MappingRootOffset {
		return ErrVolumeDescriptor
	}
	reservedStart,reservedEnd:=sb.ReservedOffset/PageSize,sb.MetadataSize/PageSize
	if d.ArenaStart<reservedStart || d.ArenaStart>reservedEnd || d.ArenaPages>reservedEnd-d.ArenaStart {
		return ErrVolumeDescriptor
	}
	arenaEnd:=d.ArenaStart+d.ArenaPages
	if d.LifecyclePages==0 {
		if d.LifecycleStart!=0 { return ErrVolumeDescriptor }
	} else if d.LifecycleStart<arenaEnd || d.LifecycleStart>reservedEnd || d.LifecyclePages>reservedEnd-d.LifecycleStart {
		return ErrVolumeDescriptor
	}
	return nil
}
