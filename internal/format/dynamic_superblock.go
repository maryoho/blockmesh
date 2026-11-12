package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	DynamicSuperMagic   = uint64(0x424D504F4F4C3033) // BMPOOL03
	DynamicSuperVersion = uint32(3)
	DynamicSuperCRCOff  = 160
)

// DynamicSuperblock is the versioned on-disk geometry record for BMPOOL03.
// It describes logical views; physical parent-LUN extent translation remains
// owned by the bootstrap Extent Map.
type DynamicSuperblock struct {
	Generation       uint64
	ViewGeneration   uint64
	PoolUUID         [16]byte
	Geometry         DynamicGeometry
}

func NewDynamicSuperblock(g DynamicGeometry, poolUUID [16]byte, generation, viewGeneration uint64) (DynamicSuperblock,error) {
	s:=DynamicSuperblock{Generation:generation,ViewGeneration:viewGeneration,PoolUUID:poolUUID,Geometry:g}
	if err:=s.Validate();err!=nil{return DynamicSuperblock{},err}
	return s,nil
}

func (s DynamicSuperblock) Validate() error {
	if s.Generation==0 || s.ViewGeneration==0 || s.PoolUUID==([16]byte{}) {
		return fmt.Errorf("%w: invalid superblock identity or generation",ErrInvalidDynamicGeometry)
	}
	g:=s.Geometry
	if g.AGSizeBlocks==0 || g.AGSizeBlocks > ^uint64(0)/BlockSize {
		return fmt.Errorf("%w: AG size overflows byte geometry",ErrInvalidDynamicGeometry)
	}
	planned,err:=PlanDynamicGeometry(DynamicGeometryConfig{
		DataBytes:g.DataBytes,MetadataBytes:g.MetadataBytes,
		AGSizeBytes:g.AGSizeBlocks*BlockSize,WALSegmentBytes:g.WALSegmentBytes,
		MappingBytes:g.MappingSize,CheckpointBytes:g.CheckpointSize, AGCapacity:g.AGCapacity,
	})
	if err!=nil{return err}
	if planned!=g { return fmt.Errorf("%w: persisted geometry is inconsistent (planned=%+v persisted=%+v)",ErrInvalidDynamicGeometry,planned,g) }
	return nil
}

func (s DynamicSuperblock) Encode() ([]byte,error) {
	if err:=s.Validate();err!=nil{return nil,err}
	g:=s.Geometry
	b:=make([]byte,PageSize)
	binary.LittleEndian.PutUint64(b[0:8],DynamicSuperMagic)
	binary.LittleEndian.PutUint32(b[8:12],DynamicSuperVersion)
	binary.LittleEndian.PutUint32(b[12:16],uint32(PageSize))
	binary.LittleEndian.PutUint32(b[16:20],uint32(BlockSize))
	copy(b[24:40],s.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[40:48],s.Generation)
	binary.LittleEndian.PutUint64(b[48:56],s.ViewGeneration)
	binary.LittleEndian.PutUint64(b[56:64],g.DataBytes)
	binary.LittleEndian.PutUint64(b[64:72],g.MetadataBytes)
	binary.LittleEndian.PutUint64(b[72:80],g.AGSizeBlocks)
	binary.LittleEndian.PutUint32(b[80:84],g.AGCount)
	binary.LittleEndian.PutUint64(b[88:96],g.LastAGBlocks)
	binary.LittleEndian.PutUint64(b[96:104],g.BitmapBytesPerAG)
	binary.LittleEndian.PutUint64(b[104:112],g.BitmapPaddedBytes)
	binary.LittleEndian.PutUint64(b[112:120],g.BitmapStride)
	binary.LittleEndian.PutUint64(b[120:128],g.BitmapOffset)
	binary.LittleEndian.PutUint64(b[128:136],g.BitmapSize)
	binary.LittleEndian.PutUint64(b[136:144],g.WALOffset)
	binary.LittleEndian.PutUint64(b[144:152],g.WALSize)
	binary.LittleEndian.PutUint64(b[152:160],g.WALSegmentBytes)
	binary.LittleEndian.PutUint32(b[DynamicSuperCRCOff:DynamicSuperCRCOff+4],0)
	binary.LittleEndian.PutUint64(b[168:176],g.NodeTableOffset)
	binary.LittleEndian.PutUint64(b[176:184],g.NodeTableSize)
	binary.LittleEndian.PutUint64(b[184:192],g.MappingOffset)
	binary.LittleEndian.PutUint64(b[192:200],g.MappingSize)
	binary.LittleEndian.PutUint64(b[200:208],g.CheckpointOffset)
	binary.LittleEndian.PutUint64(b[208:216],g.CheckpointSize)
	binary.LittleEndian.PutUint64(b[216:224],g.ReservedOffset)
	binary.LittleEndian.PutUint32(b[224:228],g.AGCapacity)
	binary.LittleEndian.PutUint32(b[DynamicSuperCRCOff:DynamicSuperCRCOff+4],crc32.Checksum(b,castagnoli))
	return b,nil
}

func DecodeDynamicSuperblock(b []byte) (DynamicSuperblock,error) {
	var s DynamicSuperblock
	if len(b)!=int(PageSize){return s,errors.New("BMPOOL03 superblock must be 4KiB")}
	if binary.LittleEndian.Uint64(b[0:8])!=DynamicSuperMagic{return s,errors.New("bad BMPOOL03 superblock magic")}
	if binary.LittleEndian.Uint32(b[8:12])!=DynamicSuperVersion{return s,errors.New("unsupported BMPOOL03 version")}
	if binary.LittleEndian.Uint32(b[12:16])!=uint32(PageSize)||binary.LittleEndian.Uint32(b[16:20])!=uint32(BlockSize){return s,errors.New("unsupported BMPOOL03 page/block size")}
	stored:=binary.LittleEndian.Uint32(b[DynamicSuperCRCOff:DynamicSuperCRCOff+4])
	c:=append([]byte(nil),b...)
	binary.LittleEndian.PutUint32(c[DynamicSuperCRCOff:DynamicSuperCRCOff+4],0)
	if crc32.Checksum(c,castagnoli)!=stored{return s,errors.New("BMPOOL03 superblock CRC32C mismatch")}
	copy(s.PoolUUID[:],b[24:40])
	s.Generation=binary.LittleEndian.Uint64(b[40:48])
	s.ViewGeneration=binary.LittleEndian.Uint64(b[48:56])
	g:=DynamicGeometry{
		DataBytes:binary.LittleEndian.Uint64(b[56:64]),MetadataBytes:binary.LittleEndian.Uint64(b[64:72]),
		TotalBlocks:binary.LittleEndian.Uint64(b[56:64])/BlockSize,
		AGSizeBlocks:binary.LittleEndian.Uint64(b[72:80]),AGCount:binary.LittleEndian.Uint32(b[80:84]),
		LastAGBlocks:binary.LittleEndian.Uint64(b[88:96]),BitmapBytesPerAG:binary.LittleEndian.Uint64(b[96:104]),
		BitmapPaddedBytes:binary.LittleEndian.Uint64(b[104:112]),BitmapStride:binary.LittleEndian.Uint64(b[112:120]),
		BitmapOffset:binary.LittleEndian.Uint64(b[120:128]),BitmapSize:binary.LittleEndian.Uint64(b[128:136]),
		WALOffset:binary.LittleEndian.Uint64(b[136:144]),WALSize:binary.LittleEndian.Uint64(b[144:152]),
		WALSegmentBytes:binary.LittleEndian.Uint64(b[152:160]),
		NodeTableOffset:binary.LittleEndian.Uint64(b[168:176]),NodeTableSize:binary.LittleEndian.Uint64(b[176:184]),
		MappingOffset:binary.LittleEndian.Uint64(b[184:192]),MappingSize:binary.LittleEndian.Uint64(b[192:200]),
		CheckpointOffset:binary.LittleEndian.Uint64(b[200:208]),CheckpointSize:binary.LittleEndian.Uint64(b[208:216]),
		ReservedOffset:binary.LittleEndian.Uint64(b[216:224]),AGCapacity:binary.LittleEndian.Uint32(b[224:228]),}
	// BMPOOL03 v3 superblocks created before AG slot reservation left this
	// formerly reserved field zero. Interpret them as capacity == AGCount.
	if g.AGCapacity==0 { g.AGCapacity=g.AGCount }
	s.Geometry=g
	if err:=s.Validate();err!=nil{return DynamicSuperblock{},err}
	return s,nil
}

// ReadDynamicSuperblock reads and validates one 4KiB BMPOOL03 superblock copy.
func ReadDynamicSuperblock(r io.ReaderAt, offset int64) (DynamicSuperblock,error) {
	var zero DynamicSuperblock
	if r==nil||offset<0{return zero,errors.New("invalid BMPOOL03 superblock reader or offset")}
	b:=make([]byte,PageSize)
	n,err:=r.ReadAt(b,offset)
	if err!=nil&&!(err==io.EOF&&n==len(b)){return zero,err}
	if n!=len(b){return zero,io.ErrUnexpectedEOF}
	return DecodeDynamicSuperblock(b)
}
