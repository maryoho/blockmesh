package metastore

import (
	"blockmesh/internal/format"
	"blockmesh/internal/volume"
)

// NewDynamicSANOwnerRecordStore binds the existing dual-slot SAN OwnerRecord
// implementation to the two pages reserved by a BMPOOL03 Volume descriptor.
// OwnerCoordinator must still hold the exclusive sanlock Owner resource for
// every CompareAndSwap.
func NewDynamicSANOwnerRecordStore(dev Device,pool format.DynamicSuperblock,index uint32,volumeID string)(*volume.SANOwnerRecordStore,error){
	store,err:=OpenDynamicPoolVolume(dev,pool,index,volumeID);if err!=nil{return nil,err}
	a,b,err:=store.OwnerRecordRange();if err!=nil{return nil,err}
	return volume.NewSANOwnerRecordStore(volume.SANOwnerRecordStoreConfig{Device:dev,VolumeID:volumeID,SlotAOffset:a,SlotBOffset:b})
}
