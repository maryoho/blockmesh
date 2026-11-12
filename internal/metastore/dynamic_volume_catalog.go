package metastore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"blockmesh/internal/format"
	"blockmesh/internal/lock"
)

var (
	ErrVolumeCatalogNotFound = errors.New("Volume not found in persistent Pool catalog")
	ErrVolumeCatalogNameExists = errors.New("Volume name already exists in Pool")
	ErrVolumeCatalogIDExists = errors.New("Volume ID already exists in Pool")
	ErrVolumeCreateTokenConflict = errors.New("Volume create token was already used for a different request")
)

// VolumeCatalogEntry is reconstructed from the fixed-LBA Volume Descriptor
// table on the shared LUN. No local database or in-memory index is authoritative.
type VolumeCatalogEntry struct {
	Index uint32 `json:"index"`
	Descriptor format.DynamicVolumeDescriptor `json:"descriptor"`
}

type VolumeCreateRequest struct {
	VolumeID string
	Name string
	SizeBytes uint64
	CreateToken [16]byte
	OwnerEpoch uint64
	ArenaPages uint64
	LifecyclePages uint64
}

type VolumeCreateResult struct {
	Entry VolumeCatalogEntry
	Replayed bool
}

type DynamicVolumeDeleteProof interface {
	// AcquireVolumeDeleteGuard atomically prevents new Reader registration,
	// proves no active Reader/local Runtime remains, fences the previous Owner
	// on every SAN path, and holds the cluster-wide guard until Close.
	AcquireVolumeDeleteGuard(context.Context, string, uint64) (lock.VolumeFencingLease, error)
	// RetireVolumeData must remove every logical Mapping and reclaim its
	// physical DataView blocks through the persistent Data Retire Queue. The
	// delete guard is held for the entire operation, so no Reader can pin the
	// old Mapping root. It must be idempotent after an uncertain WAL outcome.
	RetireVolumeData(context.Context, lock.VolumeFencingLease, string, uint64) error
}

type dynamicDeleteProofAdapter struct { proof DynamicVolumeDeleteProof }
func (p dynamicDeleteProofAdapter) AcquireVolumeFencing(ctx context.Context,id string,epoch uint64)(lock.VolumeFencingLease,error) {
	return p.proof.AcquireVolumeDeleteGuard(ctx,id,epoch)
}


func (a *DynamicVolumeAllocator) ensureUniqueVolumeName(name string,except uint32) error {
	entries,err:=a.listVolumesLocked()
	if err!=nil{return err}
	for _,entry:=range entries {
		if entry.Index!=except && entry.Descriptor.VolumeName==name {
			return ErrVolumeCatalogNameExists
		}
	}
	return nil
}

// CreateVolume persists the Volume name, UUID, logical size, create token,
// descriptor pair and Reserved Arena allocation in one cluster-safe WAL
// transaction. A retry with the same token and identical arguments returns
// the original descriptor instead of allocating another slot.
func (a *DynamicVolumeAllocator) CreateVolume(req VolumeCreateRequest, committer ClusterSafeCommitter) (VolumeCreateResult,error) {
	var result VolumeCreateResult
	if a==nil || committer==nil || !committer.ClusterSafe() { return result,ErrClusterSafeCommitterRequired }
	if req.VolumeID=="" || len(req.VolumeID)>64 || !format.ValidDynamicVolumeName(req.Name) ||
		req.SizeBytes==0 || req.SizeBytes%format.BlockSize!=0 ||
		req.CreateToken==([16]byte{}) || req.ArenaPages==0 {
		return result,fmt.Errorf("invalid Volume create request: %w",ErrGeometry)
	}
	if req.LifecyclePages==0 { req.LifecyclePages=8 }
	if req.OwnerEpoch==0 { req.OwnerEpoch=1 }

	a.mu.Lock()
	defer a.mu.Unlock()
	
	entries,err:=a.listVolumesLocked()
	if err!=nil{return result,err}
	for _,entry:=range entries {
		d:=entry.Descriptor
		if d.CreateToken==req.CreateToken {
			if d.VolumeID==req.VolumeID && d.VolumeName==req.Name && d.LogicalSizeBytes==req.SizeBytes {
				return VolumeCreateResult{Entry:entry,Replayed:true},nil
			}
			return result,ErrVolumeCreateTokenConflict
		}
		if d.VolumeName==req.Name { return result,ErrVolumeCatalogNameExists }
		if d.VolumeID==req.VolumeID { return result,ErrVolumeCatalogIDExists }
	}
	count:=uint32(a.sb.Geometry.NodeTableSize/(2*format.PageSize))
	for index:=uint32(0);index<count;index++ {
		pair,offsetErr:=format.DynamicVolumeDescriptorPairOffset(a.sb,index)
		if offsetErr!=nil{return result,offsetErr}
		first:=make([]byte,format.PageSize)
		second:=make([]byte,format.PageSize)
		if err=readFullAt(a.dev,first,int64(pair));err!=nil{return result,err}
		if err=readFullAt(a.dev,second,int64(pair+format.PageSize));err!=nil{return result,err}
		if !allZeroLifecycle(first)||!allZeroLifecycle(second) { continue }
		ag:=index%a.sb.Geometry.AGCount
		agBlocks,agErr:=a.sb.Geometry.AGBlocks(ag)
		if agErr!=nil{return result,agErr}
		if req.SizeBytes>agBlocks*format.BlockSize {
			return result,fmt.Errorf("Volume logical size %d exceeds AG %d address capacity %d: %w",req.SizeBytes,ag,agBlocks*format.BlockSize,ErrGeometry)
		}
		d:=format.DynamicVolumeDescriptor{
			VolumeID:req.VolumeID,VolumeName:req.Name,LogicalSizeBytes:req.SizeBytes,CreateToken:req.CreateToken,
			AG:ag,Generation:1,VolumeSequence:1,MappingRootOffset:a.sb.Geometry.MappingOffset,
			BitmapSequence:1,OwnerEpoch:req.OwnerEpoch,ArenaPages:req.ArenaPages,
			RootPage:^uint64(0),LifecyclePages:req.LifecyclePages,
		}
		created,createErr:=a.allocateVolumeLocked(index,d,committer)
		if createErr!=nil{return result,createErr}
		return VolumeCreateResult{Entry:VolumeCatalogEntry{Index:index,Descriptor:created}},nil
	}
	return result,ErrDynamicVolumeSpace
}

// ListVolumes reads the persistent descriptor table every time. The returned
// slice is sorted by Volume name and then UUID for stable CLI output.
func (a *DynamicVolumeAllocator) ListVolumes() ([]VolumeCatalogEntry,error) {
	if a==nil{return nil,ErrGeometry}
	a.mu.Lock();defer a.mu.Unlock()
	return a.listVolumesLocked()
}

func (a *DynamicVolumeAllocator) listVolumesLocked() ([]VolumeCatalogEntry,error) {
	count:=uint32(a.sb.Geometry.NodeTableSize/(2*format.PageSize))
	entries:=make([]VolumeCatalogEntry,0)
	for index:=uint32(0);index<count;index++ {
		pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,index)
		if err!=nil{return nil,err}
		d,err:=a.readDescriptor(pair)
		if errors.Is(err,ErrUnformatted) {
			first:=make([]byte,format.PageSize);second:=make([]byte,format.PageSize)
			if readErr:=readFullAt(a.dev,first,int64(pair));readErr!=nil{return nil,readErr}
			if readErr:=readFullAt(a.dev,second,int64(pair+format.PageSize));readErr!=nil{return nil,readErr}
			if allZeroLifecycle(first)&&allZeroLifecycle(second) { continue }
			return nil,fmt.Errorf("Volume descriptor slot %d is non-zero but neither copy is valid: %w",index,ErrCorrupt)
		}
		if err!=nil{return nil,fmt.Errorf("read Volume descriptor slot %d: %w",index,err)}
		if err=d.ValidatePool(a.sb);err!=nil{return nil,fmt.Errorf("validate Volume descriptor slot %d: %w",index,err)}
		entries=append(entries,VolumeCatalogEntry{Index:index,Descriptor:d})
	}
	sort.Slice(entries,func(i,j int)bool{
		a,b:=entries[i].Descriptor,entries[j].Descriptor
		if a.VolumeName!=b.VolumeName{return a.VolumeName<b.VolumeName}
		return a.VolumeID<b.VolumeID
	})
	return entries,nil
}

func (a *DynamicVolumeAllocator) FindVolumeByID(id string)(VolumeCatalogEntry,error) {
	entries,err:=a.ListVolumes();if err!=nil{return VolumeCatalogEntry{},err}
	for _,entry:=range entries { if entry.Descriptor.VolumeID==id{return entry,nil} }
	return VolumeCatalogEntry{},ErrVolumeCatalogNotFound
}

func (a *DynamicVolumeAllocator) FindVolumeByName(name string)(VolumeCatalogEntry,error) {
	if !format.ValidDynamicVolumeName(name){return VolumeCatalogEntry{},ErrVolumeCatalogNotFound}
	entries,err:=a.ListVolumes();if err!=nil{return VolumeCatalogEntry{},err}
	for _,entry:=range entries { if entry.Descriptor.VolumeName==name{return entry,nil} }
	return VolumeCatalogEntry{},ErrVolumeCatalogNotFound
}

func (a *DynamicVolumeAllocator) FindVolumeByCreateToken(token [16]byte)(VolumeCatalogEntry,error) {
	if token==([16]byte{}){return VolumeCatalogEntry{},ErrVolumeCatalogNotFound}
	entries,err:=a.ListVolumes();if err!=nil{return VolumeCatalogEntry{},err}
	for _,entry:=range entries { if entry.Descriptor.CreateToken==token{return entry,nil} }
	return VolumeCatalogEntry{},ErrVolumeCatalogNotFound
}

// DeleteVolume is the production deletion entry point. The supplied guard
// must serialize against Reader registration and prove the Volume inactive on
// every node; ReleaseVolume then clears descriptor copies and frees its
// Reserved metadata pages in one cluster-safe WAL transaction.
type heldVolumeDeleteProof struct { lease lock.VolumeFencingLease }
func (p heldVolumeDeleteProof) AcquireVolumeFencing(context.Context,string,uint64)(lock.VolumeFencingLease,error) {
	if p.lease==nil{return nil,errors.New("missing held Volume delete lease")}
	return p.lease,nil
}
func (a *DynamicVolumeAllocator) DeleteVolume(ctx context.Context,index uint32,proof DynamicVolumeDeleteProof,committer ClusterSafeCommitter) error {
	if a==nil||ctx==nil||proof==nil||committer==nil||!committer.ClusterSafe(){return errors.New("allocator, context, cluster-wide delete proof and cluster-safe committer are required")}
	a.mu.Lock()
	pair,err:=format.DynamicVolumeDescriptorPairOffset(a.sb,index)
	if err!=nil{a.mu.Unlock();return err}
	d,err:=a.readDescriptor(pair)
	a.mu.Unlock()
	if err!=nil{return err}
	lease,err:=proof.AcquireVolumeDeleteGuard(ctx,d.VolumeID,d.OwnerEpoch)
	if err!=nil{return fmt.Errorf("acquire cluster-wide inactive-Volume delete guard: %w",err)}
	if lease==nil{return errors.New("Volume delete proof returned a nil fencing lease")}
	if !lease.Valid()||lease.VolumeID()!=d.VolumeID||lease.Epoch()!=d.OwnerEpoch {
		return errors.Join(errors.New("Volume delete lease identity/epoch does not match descriptor"),lease.Close())
	}
	if err=proof.RetireVolumeData(ctx,lease,d.VolumeID,d.OwnerEpoch);err!=nil{
		return errors.Join(fmt.Errorf("retire all Volume mappings and reclaim physical DataView blocks: %w",err),lease.Close())
	}
	if !lease.Valid(){return errors.Join(errors.New("Volume delete lease lost after Data Retire collection"),lease.Close())}
	return a.ReleaseVolume(ctx,index,heldVolumeDeleteProof{lease:lease},committer)
}
