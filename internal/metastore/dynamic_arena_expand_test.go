package metastore

import (
	"testing"

	"blockmesh/internal/format"
)

type dynamicArenaCommitter struct{dev Device}
func(c dynamicArenaCommitter)CommitImages(images []PageImage)error{
	for _,image:=range images{if err:=writeFullAt(c.dev,image.Data,int64(image.Offset));err!=nil{return err}}
	return c.dev.Sync()
}
func(dynamicArenaCommitter)ClusterSafe()bool{return true}

func TestExpandDynamicArenaRelocatesLifecycleAndPublishesDescriptor(t *testing.T){
	oldMeta:=uint64(64<<20);extra:=uint64(format.PageSize)
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:oldMeta,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var uuid [16]byte;copy(uuid[:],"dynamic-arena-grow")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil{t.Fatal(err)}
	dev:=newMemdev((oldMeta+extra)/PageSize)
	arenaStart:=g.ReservedOffset/PageSize
	lifecycleStart:=oldMeta/PageSize-8
	desc:=format.DynamicVolumeDescriptor{VolumeID:"dynamic-arena-vol",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:3,ArenaStart:arenaStart,ArenaPages:lifecycleStart-arenaStart,
		RootPage:NoPage,LifecycleStart:lifecycleStart,LifecyclePages:8}
	if err=format.InitializeDynamicVolumeDescriptor(dev,sb,0,desc);err!=nil{t.Fatal(err)}
	oldLifecycle:=make([]byte,desc.LifecyclePages*PageSize)
	for i:=range oldLifecycle{oldLifecycle[i]=byte(i*31+7)}
	if _,err=dev.WriteAt(oldLifecycle,int64(desc.LifecycleStart*PageSize));err!=nil{t.Fatal(err)}
	store,err:=OpenDynamicPoolVolume(dev,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if err=store.ExpandDynamicArena(dynamicArenaCommitter{dev});err!=nil{t.Fatal(err)}
	got,err:=store.DynamicPoolDescriptor();if err!=nil{t.Fatal(err)}
	if got.ArenaPages<=desc.ArenaPages||got.LifecycleStart<=desc.LifecycleStart{t.Fatalf("Arena did not grow: old=%+v new=%+v",desc,got)}
	copied:=make([]byte,len(oldLifecycle))
	if _,err=dev.ReadAt(copied,int64(got.LifecycleStart*PageSize));err!=nil{t.Fatal(err)}
	if string(copied)!=string(oldLifecycle){t.Fatal("lifecycle pages changed during relocation")}
	reopened,err:=OpenDynamicPoolVolume(dev,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if reopened.Superblock().ArenaPages!=got.ArenaPages||reopened.Superblock().LifecycleStart!=got.LifecycleStart{t.Fatal("reopened Store lost expanded Arena geometry")}
}
