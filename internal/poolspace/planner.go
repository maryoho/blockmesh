package poolspace

import (
	"errors"
	"fmt"
	"math"
)

const (
	PageSize       uint64 = 4096
	AllocationSize uint64 = 4 << 20
	ExtentSize     uint64 = 1 << 30
	BootstrapSize  uint64 = 4 << 20
)

var ErrInvalidGeometry = errors.New("invalid Pool device geometry")

// Plan is the deterministic result of sizing a Pool against the current
// backing-device capacity. All values are bytes and extent-aligned unless
// explicitly documented otherwise.
type Plan struct {
	DeviceBytes       uint64 `json:"device_bytes"`
	BootstrapBytes   uint64 `json:"bootstrap_bytes"`
	InitialMetadata  uint64 `json:"initial_metadata_bytes"`
	DataCapacity     uint64 `json:"data_capacity_bytes"`
	AllocationSize   uint64 `json:"allocation_block_bytes"`
	ExtentSize       uint64 `json:"extent_size_bytes"`
	VolumeSlots      uint32 `json:"volume_slots"`
	EstimatedBitmap  uint64 `json:"estimated_bitmap_bytes"`
	EstimatedMapping uint64 `json:"estimated_mapping_bytes"`
	EstimatedWAL     uint64 `json:"estimated_wal_bytes"`
}

// PlanDevice computes metadata sizing from the actual device capacity and the
// supported workload envelope. The estimate reserves two mapping-page
// generations for COW, bitmap bytes, bounded WAL space, per-volume bootstrap
// records, and 30% free headroom. It does not hard-code a metadata percentage.
func PlanDevice(deviceBytes uint64, volumeSlots uint32) (Plan, error) {
	if deviceBytes < 2*ExtentSize || volumeSlots == 0 || volumeSlots > 4096 {
		return Plan{}, ErrInvalidGeometry
	}
	usable := deviceBytes - BootstrapSize
	// First pass: reserve at least one extent for metadata and determine the
	// number of 4MiB data blocks. Iterate because metadata depends on capacity.
	meta := ExtentSize
	for i := 0; i < 8; i++ {
		if meta >= usable { return Plan{}, ErrInvalidGeometry }
		data := usable - meta
		blocks := data / AllocationSize
		bitmap := ceilDiv(blocks, 8)
		// A mapping record is budgeted at 24 bytes. 70% B+Tree page
		// occupancy and two generations cover COW/retire overlap.
		mapping := alignUp(ceilDiv(blocks*24*2*100, 70*PageSize)*PageSize, ExtentSize)
		wal := maxU64(64<<20, minU64(2<<30, data/100))
		// 64MiB per Volume for descriptor/reader/owner/lifecycle metadata.
		perVolume := uint64(volumeSlots) * (64 << 20)
		raw := BootstrapSize + bitmap + mapping + wal + perVolume
		// 30% headroom above estimated live/transactional structures.
		required := alignUp(ceilDiv(raw*100, 70), ExtentSize)
		if required == meta { break }
		meta = required
	}
	if meta >= usable { return Plan{}, fmt.Errorf("%w: calculated metadata %d consumes device", ErrInvalidGeometry, meta) }
	data := (usable - meta) / AllocationSize * AllocationSize
	blocks := data / AllocationSize
	bitmap := ceilDiv(blocks, 8)
	mapping := alignUp(ceilDiv(blocks*24*2*100, 70*PageSize)*PageSize, ExtentSize)
	wal := maxU64(64<<20, minU64(2<<30, data/100))
	if meta > math.MaxInt64 || data == 0 { return Plan{}, ErrInvalidGeometry }
	return Plan{DeviceBytes:deviceBytes, BootstrapBytes:BootstrapSize, InitialMetadata:meta,
		DataCapacity:data, AllocationSize:AllocationSize, ExtentSize:ExtentSize,
		VolumeSlots:volumeSlots, EstimatedBitmap:bitmap, EstimatedMapping:mapping, EstimatedWAL:wal},nil
}

// MetadataExpansionTarget computes the target metadata capacity. Expansion is
// explicitly requested by the operator, but its size is never user supplied.
// The target uses observed use plus a growth forecast and a 30% headroom.
func MetadataExpansionTarget(currentCapacity, usedBytes, growthBytes, extentSize uint64) (uint64,error) {
	if extentSize==0 || extentSize&(extentSize-1)!=0 || currentCapacity%extentSize!=0 || usedBytes>currentCapacity {
		return 0,ErrInvalidGeometry
	}
	need,ok:=addChecked(usedBytes,growthBytes); if !ok { return 0,ErrInvalidGeometry }
	need,ok=mulDivCeil(need,130,100); if !ok { return 0,ErrInvalidGeometry }
	target:=alignUp(need,extentSize)
	if target<=currentCapacity { target=currentCapacity+extentSize }
	return target,nil
}

func ceilDiv(a,b uint64) uint64 { if a==0{return 0}; return 1+(a-1)/b }
func alignUp(v,a uint64) uint64 { if v==0{return 0}; return ceilDiv(v,a)*a }
func maxU64(a,b uint64)uint64{if a>b{return a};return b}
func minU64(a,b uint64)uint64{if a<b{return a};return b}
func addChecked(a,b uint64)(uint64,bool){if math.MaxUint64-a<b{return 0,false};return a+b,true}
func mulDivCeil(v,m,d uint64)(uint64,bool){if v>math.MaxUint64/m{return 0,false};return ceilDiv(v*m,d),true}
