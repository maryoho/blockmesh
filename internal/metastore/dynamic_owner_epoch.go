package metastore

import (
	"errors"

	"blockmesh/internal/format"
)

// DynamicOwnerEpochReader reads the authoritative OwnerEpoch from the
// redundant BMPOOL03 Volume Descriptor pair without depending on a Store
// runtime lock. It is suitable for WAL commit guards called while Store.mu is
// already held by a metadata transaction.
type DynamicOwnerEpochReader struct {
	dev Device
	pool format.DynamicSuperblock
	index uint32
	volumeID string
}

func NewDynamicOwnerEpochReader(dev Device,pool format.DynamicSuperblock,index uint32,volumeID string)(*DynamicOwnerEpochReader,error){
	if dev==nil||volumeID==""{return nil,errors.New("MetadataView and Volume ID are required")}
	if err:=pool.Validate();err!=nil{return nil,err}
	if _,err:=format.DynamicVolumeDescriptorPairOffset(pool,index);err!=nil{return nil,err}
	return &DynamicOwnerEpochReader{dev:dev,pool:pool,index:index,volumeID:volumeID},nil
}

func(r *DynamicOwnerEpochReader)ReadOwnerEpoch()(uint64,error){
	if r==nil||r.dev==nil{return 0,errors.New("nil Dynamic OwnerEpoch reader")}
	pair,err:=format.DynamicVolumeDescriptorPairOffset(r.pool,r.index);if err!=nil{return 0,err}
	d,_,err:=readDynamicVolumeDescriptorPair(r.dev,r.pool,pair,r.volumeID);if err!=nil{return 0,err}
	if d.OwnerEpoch==0{return 0,errors.New("BMPOOL03 Volume OwnerEpoch is zero")}
	return d.OwnerEpoch,nil
}
