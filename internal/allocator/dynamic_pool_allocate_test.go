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


func TestBMPOOL03AllocateZeroPublishPerformsCOWAndDiscard(t *testing.T) {
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:1<<20,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil { t.Fatal(err) }
	parentBytes:=poolspace.BootstrapSize+g.MetadataBytes+g.DataBytes
	parent:=newDynamicFormatMem(parentBytes)
	dataBase:=poolspace.BootstrapSize+g.MetadataBytes
	metadata,err:=poolspace.NewMetadataView(parent,[]poolspace.Extent{
		{Start:0,Length:poolspace.BootstrapSize,Kind:poolspace.Metadata},
		{Start:poolspace.BootstrapSize,Length:g.MetadataBytes,Kind:poolspace.Metadata},
	})
	if err!=nil { t.Fatal(err) }
	data,err:=poolspace.NewDataView(parent,[]poolspace.Extent{{Start:dataBase,Length:g.DataBytes,Kind:poolspace.Data}})
	if err!=nil { t.Fatal(err) }
	var uuid [16]byte;copy(uuid[:],"bm03-pool-cow")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil { t.Fatal(err) }
	if err=FormatDynamicPool(metadata,sb);err!=nil { t.Fatal(err) }
	arenaStart:=g.ReservedOffset/format.PageSize
	desc:=format.DynamicVolumeDescriptor{VolumeID:"bm03-cow-volume",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:7,ArenaStart:arenaStart,ArenaPages:512,
		RootPage:metastore.NoPage,LifecycleStart:arenaStart+512,LifecyclePages:8}
	if err=format.InitializeDynamicVolumeDescriptor(metadata,sb,0,desc);err!=nil { t.Fatal(err) }
	if err=metadata.Sync();err!=nil { t.Fatal(err) }
	store,err:=metastore.OpenDynamicPoolVolume(metadata,sb,0,desc.VolumeID);if err!=nil { t.Fatal(err) }
	if err=metastore.FormatLifecycle(store);err!=nil { t.Fatal(err) }
	locks:=&crashDistributedLocker{}
	walLocks:=&crashDistributedLocker{}
	committer,err:=wal.NewDynamicSharedCommitter(metadata,sb,0,walLocks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil { t.Fatal(err) }
	tree,err:=btree.OpenPersistentWithExternalPages(store,committer,ResolveIdempotencyPages);if err!=nil { t.Fatal(err) }
	pa,err:=NewDynamicPoolAllocator(store,tree,locks,data);if err!=nil { t.Fatal(err) }
	lease:=&crashOwnerLease{resource:"volume:"+desc.VolumeID,epoch:desc.OwnerEpoch}
	var key1, key2, key3 [16]byte
	var fp1, fp2, fp3 [32]byte
	key1[0],key2[0],key3[0]=1,2,3
	fp1[0],fp2[0],fp3[0]=11,22,33
	first,err:=pa.AllocateZeroPublishTxn(context.Background(),lease,AllocateRequest{
		IdempotencyKey:key1,Fingerprint:fp1,LogicalStart:3,BlockCount:1,Operation:2})
	if err!=nil { t.Fatal(err) }
	if first.PhysicalStart!=0 { t.Fatalf("initial physical=%d",first.PhysicalStart) }
	// Simulate existing user data in the old physical block.
	oldData:=bytes.Repeat([]byte{0x6d},int(format.BlockSize))
	if n,e:=data.WriteAt(oldData,0);e!=nil||n!=len(oldData) { t.Fatalf("seed old data n=%d err=%v",n,e) }
	if err=data.Sync();err!=nil { t.Fatal(err) }
	second,err:=pa.AllocateZeroPublishTxn(context.Background(),lease,AllocateRequest{
		IdempotencyKey:key2,Fingerprint:fp2,LogicalStart:3,BlockCount:1,Operation:2})
	if err!=nil { t.Fatal(err) }
	if second.PhysicalStart==first.PhysicalStart { t.Fatal("COW reused the old physical block") }
	mapped,err:=tree.Get(3);if err!=nil||mapped!=second.PhysicalStart { t.Fatalf("published mapping=%d err=%v",mapped,err) }
	newData:=make([]byte,format.BlockSize)
	if n,e:=data.ReadAt(newData,int64(second.PhysicalStart*format.BlockSize));e!=nil||n!=len(newData) { t.Fatalf("read COW data n=%d err=%v",n,e) }
	if !bytes.Equal(newData,oldData) { t.Fatal("COW did not preserve untouched bytes from old physical block") }

	discard,err:=pa.DiscardTxn(context.Background(),lease,AllocateRequest{
		IdempotencyKey:key3,Fingerprint:fp3,LogicalStart:3,BlockCount:1,Operation:3})
	if err!=nil { t.Fatal(err) }
	if _,err=tree.Get(3);err!=btree.ErrKeyNotFound { t.Fatalf("discard left logical mapping: %v",err) }
	replayed,err:=pa.DiscardTxn(context.Background(),lease,AllocateRequest{
		IdempotencyKey:key3,Fingerprint:fp3,LogicalStart:3,BlockCount:1,Operation:3})
	if err!=nil||!replayed.Replayed { t.Fatalf("discard idempotency replay=%+v err=%v",replayed,err) }
	// Both old extents remain allocated until Reader-safe physical reclamation.
	bitmap:=make([]byte,g.BitmapPaddedBytes)
	if _,err=metadata.ReadAt(bitmap,int64(g.BitmapOffset+format.PageSize));err!=nil { t.Fatal(err) }
	if bitmap[0]&0x03!=0x03 { t.Fatalf("COW/discard prematurely freed data blocks: bitmap[0]=%08b",bitmap[0]) }
	_ = discard
}

func TestBMPOOL03AllocateZeroPublishUsesDataViewBlockIDs(t *testing.T){
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:8*format.BlockSize,MetadataBytes:64<<20,
		AGSizeBytes:4*format.BlockSize,WALSegmentBytes:1<<20,MappingBytes:8<<20,CheckpointBytes:64<<10})
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
	walLocks:=&crashDistributedLocker{}
	committer,err:=wal.NewDynamicSharedCommitter(metadata,sb,0,walLocks,"blockmesh:pool:bm03:ag:000:wal")
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
