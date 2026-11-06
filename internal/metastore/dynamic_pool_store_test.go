package metastore

import (
	"testing"

	"blockmesh/internal/format"
)

func TestOpenDynamicPoolVolumeAndPersistentArena(t *testing.T){
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var id [16]byte;copy(id[:],"bm03-metastore01")
	pool,err:=format.NewDynamicSuperblock(g,id,1,1);if err!=nil{t.Fatal(err)}
	dev:=newMemdev(int(g.MetadataBytes/PageSize))
	arenaStart:=g.ReservedOffset/PageSize
	desc:=format.DynamicVolumeDescriptor{VolumeID:"dynamic-vol-1",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:7,ArenaStart:arenaStart,ArenaPages:128,
		RootPage:NoPage,LifecycleStart:arenaStart+128,LifecyclePages:8}
	if err=format.InitializeDynamicVolumeDescriptor(dev,pool,1,desc);err!=nil{t.Fatal(err)}
	if err=dev.Sync();err!=nil{t.Fatal(err)}
	store,err:=OpenDynamicPoolVolume(dev,pool,1,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if store.Superblock().ArenaStart!=arenaStart{t.Fatal("wrong dynamic Arena start")}
	if err=FormatLifecycle(store);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:dev}
	pa,err:=NewPersistentArena(store,nil,committer);if err!=nil{t.Fatal(err)}
	r,err:=pa.Reserve(1);if err!=nil{t.Fatal(err)}
	root:=r.Pages()[0]
	page:=Page{ID:root,Type:7,Generation:1,Payload:[]byte("BMPOOL03 persistent root")}
	if err=pa.CommitPagesAndPublish(r,[]Page{page},[]uint64{root},root,1,0,nil);err!=nil{t.Fatal(err)}
	got,err:=store.DynamicPoolDescriptor();if err!=nil{t.Fatal(err)}
	if got.RootPage!=root||got.MappingSequence!=1||got.Generation!=2{t.Fatalf("unexpected descriptor: %+v",got)}
	reopened,err:=OpenDynamicPoolVolume(dev,pool,1,desc.VolumeID);if err!=nil{t.Fatal(err)}
	pa2,err:=NewPersistentArena(reopened,[]uint64{root},committer);if err!=nil{t.Fatal(err)}
	if stats:=pa2.Stats();stats.LivePages!=1{t.Fatalf("reopened live pages=%d",stats.LivePages)}
}
