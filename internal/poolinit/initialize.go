// Package poolinit implements the fenced, idempotent first-time Pool
// initialization sequence. It owns the transition from a blank LUN to a
// discoverable BMPOOL03 Pool with MetadataView, DataView and Reserved catalog
// allocator ready for Volume creation.
package poolinit

import (
	"context"
	"errors"
	"fmt"

	"blockmesh/internal/allocator"
	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/poolspace"
)

type Config struct {
	VolumeSlots uint32
	LogicalSectorBytes uint32
	AGSizeBytes uint64
	AGCapacity uint32
	WALSegmentBytes uint64
	MappingBytes uint64
	CheckpointBytes uint64
}

type Result struct {
	PoolName string `json:"pool_name"`
	PoolUUID string `json:"pool_uuid"`
	BootstrapGeneration uint64 `json:"bootstrap_generation"`
	ViewGeneration uint64 `json:"view_generation"`
	DeviceBytes uint64 `json:"device_bytes"`
	MetadataBytes uint64 `json:"metadata_bytes"`
	DataBytes uint64 `json:"data_bytes"`
	AGCount uint32 `json:"ag_count"`
	AGCapacity uint32 `json:"ag_capacity"`
	WALSegmentBytes uint64 `json:"wal_segment_bytes"`
	ReservedAllocator bool `json:"reserved_allocator"`
}

type OwnerLease interface {
	Epoch() uint64
	Valid() bool
}

var ErrOwnerLeaseLost = errors.New("Pool Owner Lease lost during initialization")

type ownerGuardedDevice struct { device.Device; owner OwnerLease; logicalSector int }
func (d ownerGuardedDevice) WriteAt(p []byte,off int64)(int,error) {
	if d.owner==nil || !d.owner.Valid() { return 0,ErrOwnerLeaseLost }
	return d.Device.WriteAt(p,off)
}
func (d ownerGuardedDevice) Sync() error {
	if d.owner==nil || !d.owner.Valid() { return ErrOwnerLeaseLost }
	return d.Device.Sync()
}
func (d ownerGuardedDevice) LogicalSectorSize() int { return d.logicalSector }

// Initialize is safe to retry after a process crash. The caller must acquire
// an exclusive, cluster-wide Owner Lease and SCSI PR reservation before calling
// it. lockResource must be stable across nodes (normally derived from device
// WWID, not a host-local /dev path).
func Initialize(ctx context.Context, dev device.Device, name, lockResource string, locker poolspace.BootstrapLocker, owner OwnerLease, cfg Config) (result Result, retErr error) {
	if ctx==nil || dev==nil || dev.Size()<=0 { return result,poolspace.ErrInvalidGeometry }
	if name=="" || len(name)>23 { return result,fmt.Errorf("Pool name must contain 1-23 ASCII letters, digits, dot, dash or underscore") }
	if locker==nil || !locker.Distributed() { return result,errors.New("Pool initialization requires a distributed SAN lock") }
	if owner==nil || !owner.Valid() || owner.Epoch()==0 { return result,errors.New("Pool initialization requires a live fenced Owner Lease") }
	logicalSector:=int(cfg.LogicalSectorBytes)
	if v,ok:=dev.(interface{LogicalSectorSize()int});ok { logicalSector=v.LogicalSectorSize() }
	if logicalSector==0 { logicalSector=512 }
	guardedDev:=ownerGuardedDevice{Device:dev,owner:owner,logicalSector:logicalSector}
	if lockResource=="" { return result,errors.New("stable Pool initialization lock resource is required") }
	if cfg.VolumeSlots==0 { cfg.VolumeSlots=64 }
	if cfg.AGSizeBytes==0 { cfg.AGSizeBytes=1<<40 }
	if cfg.AGCapacity==0 { cfg.AGCapacity=8 }
	if cfg.WALSegmentBytes==0 { cfg.WALSegmentBytes=4<<20 }
	if cfg.MappingBytes==0 { cfg.MappingBytes=8<<20 }
	if cfg.CheckpointBytes==0 { cfg.CheckpointBytes=uint64(cfg.AGCapacity)*2*format.PageSize }
	plan,err:=poolspace.PlanDevice(uint64(dev.Size()),cfg.VolumeSlots)
	if err!=nil { return result,fmt.Errorf("plan Pool geometry from device capacity: %w",err) }
	release,err:=locker.Lock(ctx,lockResource)
	if err!=nil { return result,fmt.Errorf("acquire Pool initialization lock: %w",err) }
	defer func(){ if releaseErr:=release(); releaseErr!=nil { retErr=errors.Join(retErr,fmt.Errorf("release Pool initialization lock: %w",releaseErr)) } }()
	if !owner.Valid() { return result,errors.New("Owner Lease lost before Pool initialization") }

	var bootstrap *poolspace.BootstrapStore
	version,detectErr:=poolspace.DetectFormat(guardedDev)
	switch {
	case detectErr==nil && version==poolspace.FormatBMPOOL03:
		bootstrap,err=poolspace.OpenBootstrap(guardedDev)
		if errors.Is(err,poolspace.ErrBootstrapFormatIncomplete) { bootstrap,err=poolspace.FormatBootstrapNamed(guardedDev,name,cfg.VolumeSlots,cfg.LogicalSectorBytes) }
		if err!=nil { return result,fmt.Errorf("open or resume existing BMPOOL03 Pool: %w",err) }
		if bootstrap.Record().PoolName!=name { return result,fmt.Errorf("device already belongs to Pool %q, not %q",bootstrap.Record().PoolName,name) }
	case detectErr==nil && version==poolspace.FormatBMPOOL02:
		return result,poolspace.ErrLegacyPoolFormat
	case errors.Is(detectErr,poolspace.ErrUnknownPoolFormat):
		bootstrap,err=poolspace.FormatBootstrapNamed(dev,name,cfg.VolumeSlots,cfg.LogicalSectorBytes)
		if err!=nil { return result,fmt.Errorf("format Pool Bootstrap and catalog: %w",err) }
	default:
		if detectErr!=nil { return result,fmt.Errorf("detect existing Pool format: %w",detectErr) }
		return result,poolspace.ErrInvalidGeometry
	}
	if err=bootstrap.SetCommitLocker(locker);err!=nil { return result,err }
	if err=bootstrap.SetOwnerLease(owner);err!=nil { return result,err }
	if !owner.Valid() { return result,errors.New("Owner Lease lost after Bootstrap discovery") }

	// The first DataView consumes the automatically calculated remaining
	// capacity. It is still physically on the same LUN as the MetadataView.
	var dataBytes uint64
	for _,extent:=range bootstrap.Extents() { if extent.Kind==poolspace.Data { dataBytes+=extent.Length } }
	if dataBytes==0 {
		if _,err=bootstrap.AllocateDataExtents(plan.DataCapacity);err!=nil { return result,fmt.Errorf("publish initial Data extents: %w",err) }
		dataBytes=plan.DataCapacity
	}
	if !owner.Valid() { return result,errors.New("Owner Lease lost after Data extent publication") }
	metadata,data,viewGeneration,err:=bootstrap.OpenViews()
	if err!=nil { return result,fmt.Errorf("open Pool MetadataView/DataView: %w",err) }
	if data==nil || uint64(data.Size())!=dataBytes { return result,fmt.Errorf("DataView size mismatch: extents=%d view=%v",dataBytes,data) }

	geometryCfg:=format.DynamicGeometryConfig{
		DataBytes:uint64(data.Size()), MetadataBytes:uint64(metadata.Size()),
		AGSizeBytes:cfg.AGSizeBytes, AGCapacity:cfg.AGCapacity,
		WALSegmentBytes:cfg.WALSegmentBytes, MappingBytes:cfg.MappingBytes,
		CheckpointBytes:cfg.CheckpointBytes,
	}
	geometry,err:=format.PlanDynamicGeometry(geometryCfg)
	if err!=nil { return result,fmt.Errorf("plan BMPOOL03 metadata geometry: %w",err) }
	record:=bootstrap.Record()
	sb,err:=format.NewDynamicSuperblock(geometry,record.PoolUUID,1,viewGeneration)
	if err!=nil { return result,err }
	if !owner.Valid() { return result,errors.New("Owner Lease lost before BMPOOL03 format") }
	existing,openErr:=allocator.OpenDynamicSuperblockPair(metadata)
	switch {
	case errors.Is(openErr,allocator.ErrDynamicSuperblockUnformatted):
		err=allocator.FormatDynamicPool(ownerGuardedDevice{Device:metadata,owner:owner},sb)
	case errors.Is(openErr,allocator.ErrDynamicFormatIncomplete):
		err=allocator.RecoverDynamicFormat(ownerGuardedDevice{Device:metadata,owner:owner},sb)
	case openErr==nil:
		if existing.PoolUUID!=record.PoolUUID || existing.ViewGeneration!=viewGeneration {
			return result,errors.New("existing BMPOOL03 superblock identity/view generation differs from Bootstrap")
		}
		if existing.Geometry.MetadataBytes>uint64(metadata.Size()) || existing.Geometry.DataBytes>uint64(data.Size()) {
			return result,errors.New("existing BMPOOL03 geometry exceeds the current MetadataView/DataView")
		}
		sb=existing
	default:
		return result,fmt.Errorf("inspect BMPOOL03 format state: %w",openErr)
	}
	if err!=nil { return result,fmt.Errorf("initialize BMPOOL03 bitmaps/checkpoints/superblocks: %w",err) }
	if !owner.Valid() { return result,errors.New("Owner Lease lost after BMPOOL03 superblock publication") }

	// Reserved-region format has its own durable intent marker. Calling format
	// again resumes only a matching interrupted initialization; an unrelated
	// occupied region is rejected by the formatter.
	if metastore.DynamicVolumeAllocatorPresent(metadata,sb) {
		_,err=metastore.OpenDynamicVolumeAllocator(metadata,sb)
		if errors.Is(err,metastore.ErrDynamicVolumeAllocatorUnformatted) {
			err=metastore.RecoverDynamicVolumeAllocatorFormat(ownerGuardedDevice{Device:metadata,owner:owner},sb)
			if err==nil { _,err=metastore.OpenDynamicVolumeAllocator(metadata,sb) }
		}
	} else {
		err=metastore.FormatDynamicVolumeAllocator(ownerGuardedDevice{Device:metadata,owner:owner},sb)
		if err==nil { _,err=metastore.OpenDynamicVolumeAllocator(metadata,sb) }
	}
	if err!=nil { return result,fmt.Errorf("initialize Pool Reserved allocator: %w",err) }
	if !owner.Valid() { return result,errors.New("Owner Lease lost after Pool catalog initialization") }

	return Result{
		PoolName:record.PoolName,PoolUUID:fmt.Sprintf("%x",record.PoolUUID),
		BootstrapGeneration:bootstrap.Record().Generation,ViewGeneration:viewGeneration,
		DeviceBytes:record.DeviceBytes,MetadataBytes:uint64(metadata.Size()),DataBytes:uint64(data.Size()),
		AGCount:sb.Geometry.AGCount,AGCapacity:sb.Geometry.AGCapacity,
		WALSegmentBytes:sb.Geometry.WALSegmentBytes,ReservedAllocator:true,
	},nil
}
