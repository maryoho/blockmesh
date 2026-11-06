package format

import (
	"errors"
	"fmt"
	"math"
)

// DynamicGeometryConfig describes a BMPOOL03 logical DataView and MetadataView.
// All sizes are bytes. DataBytes is the logical capacity exposed by DataView,
// not the physical parent-LUN size. MetadataBytes excludes the bootstrap area.
type DynamicGeometryConfig struct {
	DataBytes       uint64
	MetadataBytes   uint64
	AGSizeBytes     uint64
	WALSegmentBytes uint64
	MappingBytes    uint64
	CheckpointBytes uint64
}

// DynamicGeometry is a checked geometry plan for BMPOOL03. It supports a
// partial final AG, unlike the legacy BMPOOL02 fixed geometry.
type DynamicGeometry struct {
	DataBytes         uint64
	TotalBlocks       uint64
	AGSizeBlocks      uint64
	AGCount           uint32
	LastAGBlocks      uint64
	BitmapBytesPerAG  uint64
	BitmapPaddedBytes uint64
	BitmapStride      uint64
	BitmapOffset      uint64
	BitmapSize        uint64
	WALOffset         uint64
	WALSize           uint64
	WALSegmentBytes   uint64
	NodeTableOffset   uint64
	NodeTableSize     uint64
	MappingOffset     uint64
	MappingSize       uint64
	CheckpointOffset  uint64
	CheckpointSize    uint64
	ReservedOffset    uint64
	MetadataBytes     uint64
}

var ErrInvalidDynamicGeometry = errors.New("invalid BMPOOL03 dynamic geometry")

// PlanDynamicGeometry derives AG and bitmap dimensions from the actual
// DataView capacity. Regions follow the 2MiB fixed metadata header:
// Node Table, per-AG WAL, per-AG bitmap, Mapping, Checkpoint, Reserved.
func PlanDynamicGeometry(c DynamicGeometryConfig) (DynamicGeometry, error) {
	var g DynamicGeometry
	if c.DataBytes < BlockSize || c.DataBytes%BlockSize != 0 ||
		c.MetadataBytes < 2*1024*1024 || c.MetadataBytes%PageSize != 0 ||
		c.AGSizeBytes < BlockSize || c.AGSizeBytes%BlockSize != 0 ||
		c.WALSegmentBytes < PageSize || c.WALSegmentBytes%PageSize != 0 ||
		c.MappingBytes == 0 || c.MappingBytes%PageSize != 0 ||
		c.CheckpointBytes == 0 || c.CheckpointBytes%PageSize != 0 {
		return g, ErrInvalidDynamicGeometry
	}
	total := c.DataBytes / BlockSize
	agSize := c.AGSizeBytes / BlockSize
	agCount := ceilDivGeometry(total, agSize)
	if agCount == 0 || agCount > math.MaxUint32 {
		return g, fmt.Errorf("%w: AG count out of range", ErrInvalidDynamicGeometry)
	}
	last := total - (agCount-1)*agSize
	bitmapRaw := ceilDivGeometry(agSize, 8)
	bitmapPadded, ok := alignUpGeometry(bitmapRaw, PageSize)
	if !ok { return g, ErrInvalidDynamicGeometry }
	stride, ok := addGeometry(PageSize, bitmapPadded)
	if !ok { return g, ErrInvalidDynamicGeometry }
	walSize, ok := mulGeometry(agCount, c.WALSegmentBytes)
	if !ok { return g, ErrInvalidDynamicGeometry }
	bitmapSize, ok := mulGeometry(agCount, stride)
	if !ok { return g, ErrInvalidDynamicGeometry }

	const headerEnd = uint64(2 * 1024 * 1024)
	nodeOffset := headerEnd
	nodeSize := uint64(NodeTableSize)
	walOffset, ok := addGeometry(nodeOffset, nodeSize); if !ok { return g, ErrInvalidDynamicGeometry }
	bitmapOffset, ok := addGeometry(walOffset, walSize); if !ok { return g, ErrInvalidDynamicGeometry }
	mappingOffset, ok := addGeometry(bitmapOffset, bitmapSize); if !ok { return g, ErrInvalidDynamicGeometry }
	checkpointOffset, ok := addGeometry(mappingOffset, c.MappingBytes); if !ok { return g, ErrInvalidDynamicGeometry }
	reservedOffset, ok := addGeometry(checkpointOffset, c.CheckpointBytes); if !ok { return g, ErrInvalidDynamicGeometry }
	if reservedOffset > c.MetadataBytes {
		return g, fmt.Errorf("%w: metadata requires %d bytes, have %d", ErrInvalidDynamicGeometry, reservedOffset, c.MetadataBytes)
	}
	g = DynamicGeometry{
		DataBytes:c.DataBytes, TotalBlocks:total, AGSizeBlocks:agSize,
		AGCount:uint32(agCount), LastAGBlocks:last,
		BitmapBytesPerAG:bitmapRaw, BitmapPaddedBytes:bitmapPadded, BitmapStride:stride,
		BitmapOffset:bitmapOffset, BitmapSize:bitmapSize,
		WALOffset:walOffset, WALSize:walSize, WALSegmentBytes:c.WALSegmentBytes,
		NodeTableOffset:nodeOffset, NodeTableSize:nodeSize,
		MappingOffset:mappingOffset, MappingSize:c.MappingBytes,
		CheckpointOffset:checkpointOffset, CheckpointSize:c.CheckpointBytes,
		ReservedOffset:reservedOffset, MetadataBytes:c.MetadataBytes,
	}
	return g,nil
}

// AGBlocks returns the valid block count for an AG, including a partial final AG.
func (g DynamicGeometry) AGBlocks(ag uint32) (uint64,error) {
	if ag>=g.AGCount || g.AGCount==0 { return 0,fmt.Errorf("%w: AG %d out of range",ErrInvalidDynamicGeometry,ag) }
	if ag==g.AGCount-1 { return g.LastAGBlocks,nil }
	return g.AGSizeBlocks,nil
}

// AGStartBlock returns the first DataView-relative 4MiB block in an AG.
func (g DynamicGeometry) AGStartBlock(ag uint32) (uint64,error) {
	if _,err:=g.AGBlocks(ag);err!=nil{return 0,err}
	return uint64(ag)*g.AGSizeBlocks,nil
}

func ceilDivGeometry(a,b uint64) uint64 { if a==0{return 0}; return 1+(a-1)/b }
func addGeometry(a,b uint64)(uint64,bool){if math.MaxUint64-a<b{return 0,false};return a+b,true}
func mulGeometry(a,b uint64)(uint64,bool){if a!=0&&b>math.MaxUint64/a{return 0,false};return a*b,true}
func alignUpGeometry(v,a uint64)(uint64,bool){n:=ceilDivGeometry(v,a);return mulGeometry(n,a)}
