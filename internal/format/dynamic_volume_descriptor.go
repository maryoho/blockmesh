package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	DynamicVolumeDescriptorMagic="BMDVOL03"
	DynamicVolumeDescriptorVersion=uint32(3)
	dynamicVolumeDescriptorCRC=int(PageSize)-4
)

type DynamicVolumeDescriptor struct {
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
	OwnerRecordStart uint64
}

func(d DynamicVolumeDescriptor)Encode()([]byte,error){
	if d.VolumeID==""||len(d.VolumeID)>64||d.Generation==0||d.VolumeSequence==0||d.OwnerEpoch==0||d.ArenaPages==0||
		(d.RootPage!=^uint64(0)&&(d.RootPage<d.ArenaStart||d.RootPage-d.ArenaStart>=d.ArenaPages))||
		(d.LifecyclePages==0&&d.LifecycleStart!=0)||(d.LifecyclePages!=0&&(d.LifecyclePages<2||d.LifecyclePages>32)){
		return nil,ErrVolumeDescriptor
	}
	b:=make([]byte,PageSize);copy(b[:8],DynamicVolumeDescriptorMagic)
	binary.LittleEndian.PutUint32(b[8:12],DynamicVolumeDescriptorVersion)
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
	binary.LittleEndian.PutUint64(b[168:176],d.OwnerRecordStart)
	binary.LittleEndian.PutUint32(b[dynamicVolumeDescriptorCRC:],crc32.Checksum(b[:dynamicVolumeDescriptorCRC],castagnoli))
	return b,nil
}

func DecodeDynamicVolumeDescriptor(b []byte)(DynamicVolumeDescriptor,error){
	var d DynamicVolumeDescriptor
	if len(b)!=int(PageSize)||string(b[:8])!=DynamicVolumeDescriptorMagic||binary.LittleEndian.Uint32(b[8:12])!=DynamicVolumeDescriptorVersion||
		crc32.Checksum(b[:dynamicVolumeDescriptorCRC],castagnoli)!=binary.LittleEndian.Uint32(b[dynamicVolumeDescriptorCRC:]){return d,ErrVolumeDescriptor}
	name:=b[56:120];end:=len(name);for i,c:=range name{if c==0{end=i;break}}
	for _,c:=range name[end:]{if c!=0{return d,ErrVolumeDescriptor}}
	for _,c:=range b[176:dynamicVolumeDescriptorCRC]{if c!=0{return d,ErrVolumeDescriptor}}
	d=DynamicVolumeDescriptor{VolumeID:string(name[:end]),AG:binary.LittleEndian.Uint32(b[12:16]),Generation:binary.LittleEndian.Uint64(b[16:24]),
		VolumeSequence:binary.LittleEndian.Uint64(b[24:32]),MappingRootOffset:binary.LittleEndian.Uint64(b[32:40]),
		BitmapSequence:binary.LittleEndian.Uint64(b[40:48]),OwnerEpoch:binary.LittleEndian.Uint64(b[48:56]),
		ArenaStart:binary.LittleEndian.Uint64(b[120:128]),ArenaPages:binary.LittleEndian.Uint64(b[128:136]),
		RootPage:binary.LittleEndian.Uint64(b[136:144]),MappingSequence:binary.LittleEndian.Uint64(b[144:152]),
		LifecycleStart:binary.LittleEndian.Uint64(b[152:160]),LifecyclePages:binary.LittleEndian.Uint64(b[160:168]),OwnerRecordStart:binary.LittleEndian.Uint64(b[168:176])}
	if _,err:=d.Encode();err!=nil{return DynamicVolumeDescriptor{},err}
	return d,nil
}

func(d DynamicVolumeDescriptor)ValidatePool(sb DynamicSuperblock)error{
	if err:=sb.Validate();err!=nil{return err}
	g:=sb.Geometry
	if d.AG>=g.AGCount||d.BitmapSequence==0{return ErrVolumeDescriptor}
	if d.MappingRootOffset%PageSize!=0||d.MappingRootOffset<g.MappingOffset||d.MappingRootOffset>g.CheckpointOffset||
		PageSize>g.CheckpointOffset-d.MappingRootOffset{return ErrVolumeDescriptor}
	reservedStart:=g.ReservedOffset/PageSize
	totalPages:=g.MetadataBytes/PageSize
	if d.ArenaStart<reservedStart||d.ArenaStart>totalPages||d.ArenaPages>totalPages-d.ArenaStart{return ErrVolumeDescriptor}
	if d.LifecyclePages==0{if d.LifecycleStart!=0{return ErrVolumeDescriptor}}else{
		if d.LifecycleStart<d.ArenaStart+d.ArenaPages||d.LifecycleStart>totalPages||d.LifecyclePages>totalPages-d.LifecycleStart{return ErrVolumeDescriptor}
	}
	if d.RootPage!=^uint64(0)&&(d.RootPage<d.ArenaStart||d.RootPage-d.ArenaStart>=d.ArenaPages){return ErrVolumeDescriptor}
	if d.OwnerRecordStart!=0{
		if d.OwnerRecordStart<reservedStart||d.OwnerRecordStart>totalPages||
			OwnerRecordPages>totalPages-d.OwnerRecordStart||
			ReaderTablePages>totalPages-d.OwnerRecordStart-OwnerRecordPages{return ErrVolumeDescriptor}
		expected:=d.ArenaStart+d.ArenaPages
		if d.LifecyclePages!=0{expected=d.LifecycleStart+d.LifecyclePages}
		if d.OwnerRecordStart!=expected{return ErrVolumeDescriptor}
	}
	return nil
}

func DynamicVolumeDescriptorPairOffset(sb DynamicSuperblock,index uint32)(uint64,error){
	if err:=sb.Validate();err!=nil{return 0,err}
	pairSize:=2*PageSize
	count:=sb.Geometry.NodeTableSize/pairSize
	if uint64(index)>=count{return 0,errors.New("BMPOOL03 Volume Descriptor index exceeds Node Table capacity")}
	off:=sb.Geometry.NodeTableOffset+uint64(index)*pairSize
	if off<sb.Geometry.NodeTableOffset||off>sb.Geometry.MetadataBytes||pairSize>sb.Geometry.MetadataBytes-off{return 0,ErrVolumeDescriptor}
	return off,nil
}

func InitializeDynamicVolumeDescriptor(w io.WriterAt,sb DynamicSuperblock,index uint32,d DynamicVolumeDescriptor)error{
	if w==nil{return errors.New("nil BMPOOL03 Volume Descriptor writer")}
	// Reserve the two durable OwnerRecord pages followed by the fixed Reader
	// Table extent for every newly initialized BMPOOL03 Volume. Older v3
	// descriptors with OwnerRecordStart==0 remain readable but cannot join RO.
	if d.OwnerRecordStart==0 {
		d.OwnerRecordStart=d.ArenaStart+d.ArenaPages
		if d.LifecyclePages!=0 { d.OwnerRecordStart=d.LifecycleStart+d.LifecyclePages }
	}
	if err:=d.ValidatePool(sb);err!=nil{return err}
	pair,err:=DynamicVolumeDescriptorPairOffset(sb,index);if err!=nil{return err}
	a,err:=d.Encode();if err!=nil{return err}
	for slot:=uint64(0);slot<2;slot++{n,e:=w.WriteAt(a,int64(pair+slot*PageSize));if e!=nil{return e};if n!=len(a){return io.ErrShortWrite}}
	return nil
}
