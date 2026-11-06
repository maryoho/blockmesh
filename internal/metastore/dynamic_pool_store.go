package metastore

import (
	"errors"
	"fmt"
	"math"

	"blockmesh/internal/format"
)

// OpenDynamicPoolVolume opens a Volume on a BMPOOL03 MetadataView. The device
// must be the logical MetadataView, not the parent LUN: all offsets in the
// DynamicSuperblock and Volume Descriptor are MetadataView-relative.
func OpenDynamicPoolVolume(dev Device,pool format.DynamicSuperblock,index uint32,expectedVolumeID string)(*Store,error){
	if dev==nil{return nil,errors.New("nil BMPOOL03 MetadataView")}
	if err:=pool.Validate();err!=nil{return nil,fmt.Errorf("invalid BMPOOL03 superblock: %w",err)}
	if sized,ok:=dev.(interface{Size()int64});ok{
		if sized.Size()<0||uint64(sized.Size())<pool.Geometry.MetadataBytes{return nil,errors.New("MetadataView is smaller than BMPOOL03 geometry")}
	}
	pair,err:=format.DynamicVolumeDescriptorPairOffset(pool,index);if err!=nil{return nil,err}
	d,slot,err:=readDynamicVolumeDescriptorPair(dev,pool,pair,expectedVolumeID);if err!=nil{return nil,err}
	sb:=Superblock{VolumeID:d.VolumeID,Generation:d.Generation,TotalPages:pool.Geometry.MetadataBytes/PageSize,
		ArenaStart:d.ArenaStart,ArenaPages:d.ArenaPages,RootPage:d.RootPage,MappingSequence:d.MappingSequence,
		LifecycleStart:d.LifecycleStart,LifecyclePages:d.LifecyclePages}
	if err=validateDynamicStoreGeometry(sb,pool,index);err!=nil{return nil,err}
	return &Store{dev:dev,sb:sb,sbSlot:slot,poolBacked:true,dynamicPool:true,dynamicSB:pool,
		dynamicDescriptor:d,descriptorPair:pair,volumeIndex:index},nil
}

func validateDynamicStoreGeometry(sb Superblock,pool format.DynamicSuperblock,index uint32)error{
	if sb.TotalPages!=pool.Geometry.MetadataBytes/PageSize||sb.ArenaStart<pool.Geometry.ReservedOffset/PageSize||
		sb.ArenaStart>sb.TotalPages||sb.ArenaPages>sb.TotalPages-sb.ArenaStart{return ErrGeometry}
	if sb.LifecyclePages!=0{
		if sb.LifecycleStart<sb.ArenaStart+sb.ArenaPages||sb.LifecycleStart>sb.TotalPages||
			sb.LifecyclePages>sb.TotalPages-sb.LifecycleStart{return ErrGeometry}
	}
	if sb.RootPage!=NoPage&&(sb.RootPage<sb.ArenaStart||sb.RootPage-sb.ArenaStart>=sb.ArenaPages){return ErrGeometry}
	if _,err:=format.DynamicVolumeDescriptorPairOffset(pool,index);err!=nil{return err}
	return nil
}

func readDynamicVolumeDescriptorPair(dev Device,pool format.DynamicSuperblock,pair uint64,expected string)(format.DynamicVolumeDescriptor,uint64,error){
	var best format.DynamicVolumeDescriptor
	var bestSlot uint64
	found:=false
	for slot:=uint64(0);slot<2;slot++{
		b:=make([]byte,PageSize);if err:=readFullAt(dev,b,int64(pair+slot*PageSize));err!=nil{return format.DynamicVolumeDescriptor{},0,err}
		d,err:=format.DecodeDynamicVolumeDescriptor(b);if err!=nil{continue}
		if err=d.ValidatePool(pool);err!=nil{continue}
		if expected!=""&&d.VolumeID!=expected{return format.DynamicVolumeDescriptor{},0,fmt.Errorf("BMPOOL03 Volume Descriptor belongs to %q, not %q",d.VolumeID,expected)}
		if found&&d.VolumeID!=best.VolumeID{return format.DynamicVolumeDescriptor{},0,errors.New("BMPOOL03 Volume Descriptor copies contain different Volume IDs")}
		if found&&d.Generation==best.Generation&&d!=best{return format.DynamicVolumeDescriptor{},0,errors.New("conflicting BMPOOL03 Volume Descriptor copies at equal generation")}
		if !found||d.Generation>best.Generation{best,bestSlot,found=d,slot,true}
	}
	if !found{return format.DynamicVolumeDescriptor{},0,ErrUnformatted}
	return best,bestSlot,nil
}

func(s *Store)refreshDynamicPoolLocked()error{
	d,slot,err:=readDynamicVolumeDescriptorPair(s.dev,s.dynamicSB,s.descriptorPair,s.sb.VolumeID);if err!=nil{return err}
	if d.ArenaStart!=s.dynamicDescriptor.ArenaStart||d.ArenaPages!=s.dynamicDescriptor.ArenaPages||
		d.LifecycleStart!=s.dynamicDescriptor.LifecycleStart||d.LifecyclePages!=s.dynamicDescriptor.LifecyclePages{return ErrGeometry}
	candidate:=Superblock{VolumeID:d.VolumeID,Generation:d.Generation,TotalPages:s.dynamicSB.Geometry.MetadataBytes/PageSize,
		ArenaStart:d.ArenaStart,ArenaPages:d.ArenaPages,RootPage:d.RootPage,MappingSequence:d.MappingSequence,
		LifecycleStart:d.LifecycleStart,LifecyclePages:d.LifecyclePages}
	if err=validateDynamicStoreGeometry(candidate,s.dynamicSB,s.volumeIndex);err!=nil{return err}
	if d.Generation>s.dynamicDescriptor.Generation{s.dynamicDescriptor=d;s.sb=candidate;s.sbSlot=slot}
	return nil
}

func(s *Store)dynamicDescriptorAfterImage(rootPage,mappingSequence uint64)(format.DynamicVolumeDescriptor,uint64,[]byte,error){
	if s.dynamicDescriptor.Generation==math.MaxUint64||s.dynamicDescriptor.VolumeSequence==math.MaxUint64{
		return format.DynamicVolumeDescriptor{},0,nil,errors.New("BMPOOL03 Volume Descriptor sequence exhausted")
	}
	next:=s.dynamicDescriptor;next.Generation++;next.VolumeSequence++;next.RootPage=rootPage;next.MappingSequence=mappingSequence
	if err:=next.ValidatePool(s.dynamicSB);err!=nil{return format.DynamicVolumeDescriptor{},0,nil,err}
	candidate:=Superblock{VolumeID:next.VolumeID,Generation:next.Generation,TotalPages:s.dynamicSB.Geometry.MetadataBytes/PageSize,
		ArenaStart:next.ArenaStart,ArenaPages:next.ArenaPages,RootPage:next.RootPage,MappingSequence:next.MappingSequence,
		LifecycleStart:next.LifecycleStart,LifecyclePages:next.LifecyclePages}
	if err:=validateDynamicStoreGeometry(candidate,s.dynamicSB,s.volumeIndex);err!=nil{return format.DynamicVolumeDescriptor{},0,nil,err}
	image,err:=next.Encode();if err!=nil{return format.DynamicVolumeDescriptor{},0,nil,err}
	return next,uint64(1)-s.sbSlot,image,nil
}

func(s *Store)DynamicPoolDescriptor()(format.DynamicVolumeDescriptor,error){
	if s==nil{return format.DynamicVolumeDescriptor{},errors.New("nil metastore")}
	s.mu.Lock();defer s.mu.Unlock()
	if !s.dynamicPool{return format.DynamicVolumeDescriptor{},errors.New("Volume is not BMPOOL03-backed")}
	if err:=s.refreshLocked();err!=nil{return format.DynamicVolumeDescriptor{},err}
	return s.dynamicDescriptor,nil
}

func(s *Store)DynamicPoolSuperblock()(format.DynamicSuperblock,error){
	if s==nil{return format.DynamicSuperblock{},errors.New("nil metastore")}
	s.mu.RLock();defer s.mu.RUnlock()
	if !s.dynamicPool{return format.DynamicSuperblock{},errors.New("Volume is not BMPOOL03-backed")}
	return s.dynamicSB,nil
}
