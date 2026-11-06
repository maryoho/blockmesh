package metastore

import (
	"context"
	"errors"
	"fmt"
	"math"

	"blockmesh/internal/lock"
)

// ExpandDynamicArena grows the BMPOOL03 PersistentArena into newly added
// MetadataView capacity. Lifecycle pages are copied to the new tail and synced
// before the BMDVOL03 descriptor is WAL-published. Existing roots and arena
// page IDs remain unchanged.
func(s *Store)expandDynamicArenaLegacy(committer Committer)error{
	if s==nil||committer==nil{return errors.New("metastore and WAL committer are required")}
	cluster,ok:=committer.(ClusterSafeCommitter);if !ok||!cluster.ClusterSafe(){return ErrClusterSafeCommitterRequired}
	s.mu.Lock();defer s.mu.Unlock()
	if !s.dynamicPool{return errors.New("Arena expansion requires BMPOOL03")}
	if s.volumeIndex!=0{return errors.New("BMPOOL03 multi-Volume reserved-region allocation is not implemented")}
	d:=s.dynamicDescriptor
	if d.Generation==math.MaxUint64||d.VolumeSequence==math.MaxUint64{return errors.New("BMPOOL03 Volume Descriptor sequence exhausted")}
	totalPages:=s.dynamicSB.Geometry.MetadataBytes/PageSize
	next:=d
	next.Generation++;next.VolumeSequence++
	if d.LifecyclePages==0{
		newArenaPages:=totalPages-d.ArenaStart
		if newArenaPages<=d.ArenaPages{return errors.New("expanded MetadataView has no additional Arena pages")}
		next.ArenaPages=newArenaPages
	}else{
		if d.LifecyclePages>totalPages{return ErrGeometry}
		newLifecycleStart:=totalPages-d.LifecyclePages
		if newLifecycleStart<=d.LifecycleStart{return errors.New("expanded MetadataView does not move lifecycle tail forward")}
		if newLifecycleStart<d.ArenaStart{return ErrGeometry}
		// Buffer the complete lifecycle set before writing: a small metadata
		// expansion can make source and destination ranges overlap.
		pages:=make([][]byte,d.LifecyclePages)
		for i:=uint64(0);i<d.LifecyclePages;i++{
			pages[i]=make([]byte,PageSize)
			if err:=readFullAt(s.dev,pages[i],int64((d.LifecycleStart+i)*PageSize));err!=nil{return fmt.Errorf("read lifecycle page %d: %w",d.LifecycleStart+i,err)}
		}
		for i:=uint64(0);i<d.LifecyclePages;i++{
			if err:=writeFullAt(s.dev,pages[i],int64((newLifecycleStart+i)*PageSize));err!=nil{return fmt.Errorf("copy lifecycle page %d: %w",newLifecycleStart+i,err)}
		}
		if err:=s.dev.Sync();err!=nil{return fmt.Errorf("sync relocated lifecycle pages: %w",err)}
		next.LifecycleStart=newLifecycleStart
		next.ArenaPages=newLifecycleStart-d.ArenaStart
	}
	if err:=next.ValidatePool(s.dynamicSB);err!=nil{return err}
	image,err:=next.Encode();if err!=nil{return err}
	slot:=uint64(1)-s.sbSlot
	if err=committer.CommitImages([]PageImage{{Offset:s.descriptorPair+slot*PageSize,Data:image}});err!=nil{return fmt.Errorf("publish expanded BMPOOL03 Arena descriptor: %w",err)}
	s.dynamicDescriptor=next
	s.sb.Generation=next.Generation
	s.sb.ArenaPages=next.ArenaPages
	s.sb.LifecycleStart=next.LifecycleStart
	s.sbSlot=slot
	return nil
}

 // ExpandDynamicArena preserves the legacy BMPOOL03 slot-0 behavior for
 // Pools without the Reserved allocator. New multi-Volume Pools grow through
 // the persistent Reserved bitmap and per-Volume descriptor.
 func(s *Store)ExpandDynamicArena(committer Committer)error{
	if s!=nil&&s.dynamicPool&&DynamicVolumeAllocatorPresent(s.dev,s.dynamicSB){
		return s.ExpandDynamicArenaBy(256,committer)
	}
	return s.expandDynamicArenaLegacy(committer)
 }

 // ExpandDynamicArenaBy grows this Volume by an explicit number of 4KiB Arena
 // pages. The Reserved allocator must already have been expanded to the
 // current MetadataView geometry.
 func(s *Store)ExpandDynamicArenaBy(pages uint64,committer Committer)error{
	if s==nil||committer==nil||pages==0{return errors.New("metastore, positive Arena growth and WAL committer are required")}
	cluster,ok:=committer.(ClusterSafeCommitter);if !ok||!cluster.ClusterSafe(){return ErrClusterSafeCommitterRequired}
	s.mu.Lock();defer s.mu.Unlock()
	if !s.dynamicPool{return errors.New("Arena expansion requires BMPOOL03")}
	if !DynamicVolumeAllocatorPresent(s.dev,s.dynamicSB){return ErrDynamicVolumeAllocatorUnformatted}
	allocator,err:=OpenDynamicVolumeAllocator(s.dev,s.dynamicSB);if err!=nil{return err}
	next,slot,err:=allocator.GrowVolume(s.volumeIndex,pages,cluster);if err!=nil{return err}
	s.dynamicDescriptor=next
	s.sb.Generation=next.Generation
	s.sb.ArenaStart=next.ArenaStart
	s.sb.ArenaPages=next.ArenaPages
	s.sb.LifecycleStart=next.LifecycleStart
	s.sb.LifecyclePages=next.LifecyclePages
	s.sbSlot=slot
	return nil
 }

// ExpandDynamicArenaUnderLease is the shared-SAN entry point. It holds the
// Volume AG lock and requires a live Owner lease while relocating lifecycle
// pages and publishing the new Arena range.
func(s *Store)ExpandDynamicArenaUnderLease(ctx context.Context,ownerLease lock.Lease,locks lock.DistributedLocker,committer Committer)error{
	if ctx==nil||ownerLease==nil||!ownerLease.Valid()||ownerLease.Resource()==""||locks==nil||!locks.Distributed(){
		return errors.New("live Volume Owner lease and distributed AG locker are required")
	}
	d,err:=s.DynamicPoolDescriptor();if err!=nil{return err}
	if identity,ok:=ownerLease.(interface{VolumeID()string});ok {
		if identity.VolumeID()!=d.VolumeID{return errors.New("Owner lease Volume ID does not match BMPOOL03 Volume Descriptor")}
	} else if ownerLease.Resource()!="volume:"+d.VolumeID {
		return errors.New("Owner lease does not match BMPOOL03 Volume Descriptor")
	}
	unlock,err:=locks.Lock(ctx,fmt.Sprintf("ag:%03d",d.AG));if err!=nil{return fmt.Errorf("lock Volume AG %d for Arena expansion: %w",d.AG,err)}
	defer unlock()
	if err=ctx.Err();err!=nil{return err}
	if !ownerLease.Valid(){return errors.New("Volume Owner lease lost before Arena expansion")}
	if err=s.ExpandDynamicArena(committer);err!=nil{return err}
	if !ownerLease.Valid(){return errors.New("Volume Owner lease lost during Arena expansion; fence and inspect descriptor")}
	return nil
}
