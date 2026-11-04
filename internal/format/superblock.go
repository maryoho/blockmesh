package format

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	SuperMagic     = uint64(0x424D504F4F4C3032) // BMPOOL02
	SuperVersion   = uint32(2)
	SuperCRCOffset = 160
)

type Superblock struct {
	Generation       uint64
	PoolUUID         [16]byte
	PoolSize         uint64
	DataStartSector  uint64
	TotalBlocks      uint64
	AGSizeBlocks     uint32
	AGCount          uint32
	MetadataSize     uint64
	NodeTableOffset  uint64
	WALOffset        uint64
	WALSize          uint64
	BitmapOffset     uint64
	BitmapSize       uint64
	MappingOffset    uint64
	MappingSize      uint64
	CheckpointOffset uint64
	CheckpointSize   uint64
	ReservedOffset   uint64
}

func NewSuperblock(dataStartSector uint64) (Superblock, error) {
	var s Superblock
	if _, err := io.ReadFull(rand.Reader, s.PoolUUID[:]); err != nil {
		return s, err
	}
	s.Generation = 1
	s.PoolSize = PoolSize
	s.DataStartSector = dataStartSector
	s.TotalBlocks = PoolSize / BlockSize
	s.AGSizeBlocks = uint32(AGBlocks)
	s.AGCount = AGCount
	s.MetadataSize = MetadataSize
	s.NodeTableOffset = NodeTableOffset
	s.WALOffset = WALOffset
	s.WALSize = WALSize
	s.BitmapOffset = BitmapOffset
	s.BitmapSize = BitmapSize
	s.MappingOffset = MappingOffset
	s.MappingSize = MappingSize
	s.CheckpointOffset = CheckpointOffset
	s.CheckpointSize = CheckpointSize
	s.ReservedOffset = ReservedOffset
	return s, nil
}

func (s Superblock) Encode() []byte {
	b := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(b[0:8], SuperMagic)
	binary.LittleEndian.PutUint32(b[8:12], SuperVersion)
	binary.LittleEndian.PutUint32(b[12:16], 168)
	binary.LittleEndian.PutUint32(b[16:20], uint32(PageSize))
	binary.LittleEndian.PutUint32(b[20:24], uint32(BlockSize))
	copy(b[24:40], s.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[40:48], s.PoolSize)
	binary.LittleEndian.PutUint64(b[48:56], s.DataStartSector)
	binary.LittleEndian.PutUint64(b[56:64], s.TotalBlocks)
	binary.LittleEndian.PutUint32(b[64:68], s.AGSizeBlocks)
	binary.LittleEndian.PutUint32(b[68:72], s.AGCount)
	binary.LittleEndian.PutUint64(b[72:80], s.MetadataSize)
	binary.LittleEndian.PutUint64(b[80:88], s.NodeTableOffset)
	binary.LittleEndian.PutUint64(b[88:96], s.WALOffset)
	binary.LittleEndian.PutUint64(b[96:104], s.WALSize)
	binary.LittleEndian.PutUint64(b[104:112], s.BitmapOffset)
	binary.LittleEndian.PutUint64(b[112:120], s.BitmapSize)
	binary.LittleEndian.PutUint64(b[120:128], s.MappingOffset)
	binary.LittleEndian.PutUint64(b[128:136], s.MappingSize)
	binary.LittleEndian.PutUint64(b[136:144], s.CheckpointOffset)
	binary.LittleEndian.PutUint64(b[144:152], s.CheckpointSize)
	binary.LittleEndian.PutUint64(b[152:160], s.ReservedOffset)
	binary.LittleEndian.PutUint64(b[168:176], s.Generation)
	binary.LittleEndian.PutUint32(b[SuperCRCOffset:SuperCRCOffset+4], 0)
	binary.LittleEndian.PutUint32(b[SuperCRCOffset:SuperCRCOffset+4], crc32.Checksum(b, castagnoli))
	return b
}

func DecodeSuperblock(b []byte) (Superblock, error) {
	var s Superblock
	if len(b) != int(PageSize) {
		return s, fmt.Errorf("superblock must be 4KiB")
	}
	if binary.LittleEndian.Uint64(b[0:8]) != SuperMagic {
		return s, fmt.Errorf("bad superblock magic")
	}
	if binary.LittleEndian.Uint32(b[8:12]) != SuperVersion {
		return s, fmt.Errorf("unsupported superblock version")
	}
	if binary.LittleEndian.Uint32(b[16:20]) != uint32(PageSize) || binary.LittleEndian.Uint32(b[20:24]) != uint32(BlockSize) {
		return s, fmt.Errorf("unsupported page/block size")
	}
	stored := binary.LittleEndian.Uint32(b[SuperCRCOffset : SuperCRCOffset+4])
	c := append([]byte(nil), b...)
	binary.LittleEndian.PutUint32(c[SuperCRCOffset:SuperCRCOffset+4], 0)
	if crc32.Checksum(c, castagnoli) != stored {
		return s, fmt.Errorf("superblock CRC32C mismatch")
	}
	copy(s.PoolUUID[:], b[24:40])
	s.PoolSize = binary.LittleEndian.Uint64(b[40:48])
	s.DataStartSector = binary.LittleEndian.Uint64(b[48:56])
	s.TotalBlocks = binary.LittleEndian.Uint64(b[56:64])
	s.AGSizeBlocks = binary.LittleEndian.Uint32(b[64:68])
	s.AGCount = binary.LittleEndian.Uint32(b[68:72])
	s.MetadataSize = binary.LittleEndian.Uint64(b[72:80])
	s.NodeTableOffset = binary.LittleEndian.Uint64(b[80:88])
	s.WALOffset = binary.LittleEndian.Uint64(b[88:96])
	s.WALSize = binary.LittleEndian.Uint64(b[96:104])
	s.BitmapOffset = binary.LittleEndian.Uint64(b[104:112])
	s.BitmapSize = binary.LittleEndian.Uint64(b[112:120])
	s.MappingOffset = binary.LittleEndian.Uint64(b[120:128])
	s.MappingSize = binary.LittleEndian.Uint64(b[128:136])
	s.CheckpointOffset = binary.LittleEndian.Uint64(b[136:144])
	s.CheckpointSize = binary.LittleEndian.Uint64(b[144:152])
	s.ReservedOffset = binary.LittleEndian.Uint64(b[152:160])
	s.Generation = binary.LittleEndian.Uint64(b[168:176])
	if err := s.Validate(); err != nil { return s, err }
	return s, nil

}

// Validate rejects malformed or overlapping Pool metadata regions before any
// offset from the on-disk superblock is used for I/O.
func (s Superblock) Validate() error {
	if s.Generation == 0 || s.PoolUUID == ([16]byte{}) || s.PoolSize == 0 ||
		s.TotalBlocks != s.PoolSize/BlockSize || s.PoolSize%BlockSize != 0 ||
		s.AGCount == 0 || s.AGSizeBlocks == 0 ||
		uint64(s.AGSizeBlocks)*BlockSize == 0 ||
		uint64(s.AGCount)*uint64(s.AGSizeBlocks) != s.TotalBlocks ||
		s.MetadataSize < 2*PageSize || s.MetadataSize > uint64(^uint64(0)>>1) || s.NodeTableOffset < 2*PageSize {
		return fmt.Errorf("invalid Pool geometry")
	}
	regions := [][3]uint64{
		{NodeTableOffset, s.NodeTableOffset, NodeTableSize},
		{WALOffset, s.WALOffset, s.WALSize},
		{BitmapOffset, s.BitmapOffset, s.BitmapSize},
		{MappingOffset, s.MappingOffset, s.MappingSize},
		{CheckpointOffset, s.CheckpointOffset, s.CheckpointSize},
		{ReservedOffset, s.ReservedOffset, s.MetadataSize-s.ReservedOffset},
	}
	for _, r := range regions {
		if r[1]%PageSize != 0 || r[1] > s.MetadataSize || r[2] > s.MetadataSize-r[1] {
			return fmt.Errorf("metadata region %d outside or misaligned", r[0])
		}
	}
	for i:=0;i<len(regions);i++ { for k:=i+1;k<len(regions);k++ {
		a,b:=regions[i],regions[k]
		if a[2]!=0 && b[2]!=0 && a[1]<b[1]+b[2] && b[1]<a[1]+a[2] {
			return fmt.Errorf("metadata regions %d and %d overlap",a[0],b[0])
		}
	}}
	if s.WALSize < uint64(s.AGCount)*WALSegmentSize ||
		s.BitmapSize < uint64(s.AGCount)*AGBitmapStride ||
		s.CheckpointSize < uint64(s.AGCount)*2*PageSize {
		return fmt.Errorf("Pool metadata regions are undersized")
	}
	return nil
}
