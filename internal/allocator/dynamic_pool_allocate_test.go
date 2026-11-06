package allocator

import (
	"bytes"
	"context"
	"testing"

	"blockmesh/internal/btree"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/wal"
)

func TestBMPOOL03AllocateZeroPublishUsesDataViewBlockIDs(t *testing.T){
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:64<<10,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	parentBytes:=poolspace.BootstrapSize+g.MetadataBytes+g.DataBytes
	parent:=newDynamicFormatMem(parentBytes)
	dataBase:=poolspace.BootstrapSize+g.MetadataBytes
	for i:=dataBase;i<parentBytes;i++{parent.b[i]=0xa5}
	metadata,err:=poolspace.NewMetadataView(parent,[]poolspace.Extent{
		{Start:0,Length:poolspace.BootstrapSize,Kind:poolspace.Metadata},
		{Start:poolspace.BootstrapSize,Length:g.MetadataBytes,Kind:poolspace.Metadata},
	})
	if err!=nil{t.Fatal(err)}
	data,err:=poolspace.NewDataView(parent,[]poolspace.Extent{
		{Start:dataBase,Length:g.DataBytes,Kind:poolspace.Data},
	})
	if err!=nil{t.Fatal(err)}
	var uuid [16]byte;copy(uuid[:],"bm03-pool-alloc")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil{t.Fatal(err)}
	if err=FormatDynamicPool(metadata,sb);err!=nil{t.Fatal(err)}
	arenaStart:=g.ReservedOffset/format.PageSize
	desc:=format.DynamicVolumeDescriptor{VolumeID:"bm03-alloc-volume",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:7,ArenaStart:arenaStart,ArenaPages:512,
		RootPage:metastore.NoPage,LifecycleStart:arenaStart+512,LifecyclePages:8}
	if err=format.InitializeDynamicVolumeDescriptor(metadata,sb,0,desc);err!=nil{t.Fatal(err)}
	if err=metadata.Sync();err!=nil{t.Fatal(err)}
	store,err:=metastore.OpenDynamicPoolVolume(metadata,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
	locks:=&crashDistributedLocker{}
	committer,err:=wal.NewDynamicSharedCommitter(metadata,sb,0,locks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil{t.Fatal(err)}
	tree,err:=btree.OpenPersistentWithExternalPages(store,committer,ResolveIdempotencyPages)
	if err!=nil{t.Fatal(err)}
	allocator,err:=NewDynamicPoolAllocator(store,tree,locks,data);if err!=nil{t.Fatal(err)}
	lease:=&crashOwnerLease{resource:"volume:"+desc.VolumeID,epoch:desc.OwnerEpoch}
	var key [16]byte;var fingerprint [32]byte;key[0]=0x91;fingerprint[0]=0x27
	req:=AllocateRequest{IdempotencyKey:key,Fingerprint:fingerprint,LogicalStart:3,BlockCount:1,Operation:1}
	result,err:=allocator.AllocateZeroPublishTxn(context.Background(),lease,req)
	if err!=nil{t.Fatal(err)}
	if result.PhysicalStart!=0||result.OwnerEpoch!=desc.OwnerEpoch{t.Fatalf("unexpected allocation result: %+v",result)}
	physical,err:=tree.Get(req.LogicalStart);if err!=nil||physical!=0{t.Fatalf("mapping=%d err=%v",physical,err)}
	bitmap:=make([]byte,format.PageSize)
	if _,err=metadata.ReadAt(bitmap,int64(g.BitmapOffset+format.PageSize));err!=nil{t.Fatal(err)}
	if bitmap[0]&1==0{t.Fatal("bitmap block zero was not allocated")}
	zeroed:=make([]byte,format.BlockSize)
	if !bytes.Equal(parent.b[int(dataBase):int(dataBase+format.BlockSize)],zeroed){t.Fatal("DataView block zero was not cleared before metadata publication")}
	replayed,err:=allocator.AllocateZeroPublishTxn(context.Background(),lease,req)
	if err!=nil||!replayed.Replayed||replayed.PhysicalStart!=0{t.Fatalf("idempotent retry=%+v err=%v",replayed,err)}
}
