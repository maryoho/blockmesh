package format

import "fmt"

const (
	PageSize        = uint64(4096)
	BlockSize       = uint64(4 * 1024 * 1024)
	SectorsPerBlock = uint64(BlockSize / 512)
	PoolSize        = uint64(100) * 1024 * 1024 * 1024 * 1024
	AGSize          = uint64(1) * 1024 * 1024 * 1024 * 1024
	AGBlocks        = AGSize / BlockSize
	AGCount         = uint32(PoolSize / AGSize)
	BitmapBytes     = AGBlocks / 8
	BitmapPages     = BitmapBytes / PageSize
	MetadataSize    = uint64(16) * 1024 * 1024 * 1024
	NodeTableSize   = uint64(1) * 1024 * 1024
	// ReaderTablePages reserves one durable 4KiB slot per supported reader
	// in every bootstrap Volume slot. Its location is derived from slot geometry.
	ReaderTablePages = uint64(128)
	// OwnerRecordPages reserves two redundant checksummed 4KiB pages per Volume.
	OwnerRecordPages = uint64(2)
	WALSegmentSize  = uint64(16) * 1024 * 1024
	MappingSize     = uint64(5) * 1024 * 1024 * 1024
	CheckpointSize  = uint64(64) * 1024 * 1024
)

const (
	SuperAOffset     = uint64(0)
	SuperBOffset     = PageSize
	NodeTableOffset  = 2 * PageSize
	WALOffset        = uint64(2) * 1024 * 1024
	WALSize          = uint64(AGCount) * WALSegmentSize
	BitmapOffset     = WALOffset + WALSize
	AGBitmapStride   = PageSize + BitmapBytes // one descriptor page + eight raw bitmap pages
	BitmapSize       = uint64(AGCount) * AGBitmapStride
	MappingOffset    = BitmapOffset + BitmapSize
	CheckpointOffset = MappingOffset + MappingSize
	ReservedOffset   = CheckpointOffset + CheckpointSize
)

func ValidateLayout() error {
	if PoolSize%AGSize != 0 || AGSize%BlockSize != 0 || BitmapBytes%PageSize != 0 {
		return fmt.Errorf("invalid geometry")
	}
	if uint64(AGCount)*AGSize != PoolSize {
		return fmt.Errorf("AG geometry does not cover pool")
	}
	if ReservedOffset > MetadataSize {
		return fmt.Errorf("metadata layout needs %d bytes, configured size is %d", ReservedOffset, MetadataSize)
	}
	return nil
}
