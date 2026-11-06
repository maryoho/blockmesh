package allocator

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"blockmesh/internal/btree"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/wal"
)

type failArenaMetadataView struct{
	inner *poolspace.MetadataView
	arenaStart uint64
	fired bool
}
func(d *failArenaMetadataView)ReadAt(p []byte,off int64)(int,error){return d.inner.ReadAt(p,off)}
func(d *failArenaMetadataView)WriteAt(p []byte,off int64)(int,error){
	if !d.fired&&off>=int64(d.arenaStart*metastore.PageSize){d.fired=true;return 0,errors.New("injected crash during first metadata after-image")}
	return d.inner.WriteAt(p,off)
}
func(d *failArenaMetadataView)Sync()error{return d.inner.Sync()}
func(d *failArenaMetadataView)Size()int64{return d.inner.Size()}

func TestBMPOOL03EndToEndCrashRecoveryAndOnlineExpansion(t *testing.T){
	const oldMetadata=uint64(64<<20)
	const oldData=uint64(4*format.BlockSize)
	const extraMetadata=uint64(8<<20)
	const extraData=uint64(2*format.BlockSize)
	extraMetaBase:=poolspace.BootstrapSize+oldMetadata+oldData
	extraDataBase:=extraMetaBase+extraMetadata
	parentBytes:=extraDataBase+extraData
	parent:=newDynamicFormatMem(parentBytes)
	oldDataBase:=poolspace.BootstrapSize+oldMetadata
	for i:=oldDataBase;i<oldDataBase+oldData;i++{parent.b[i]=0xa5}
	for i:=extraDataBase;i<extraDataBase+extraData;i++{parent.b[i]=0xa5}

	meta,err:=poolspace.NewMetadataView(parent,[]poolspace.Extent{
		{Start:0,Length:poolspace.BootstrapSize,Kind:poolspace.Metadata},
		{Start:poolspace.BootstrapSize,Length:oldMetadata,Kind:poolspace.Metadata},
	})
	if err!=nil{t.Fatal(err)}
	data,err:=poolspace.NewDataView(parent,[]poolspace.Extent{{Start:oldDataBase,Length:oldData,Kind:poolspace.Data}})
	if err!=nil{t.Fatal(err)}
	g,err:=format.PlanDynamicGeometry(format.DynamicGeometryConfig{DataBytes:oldData,MetadataBytes:oldMetadata,
		AGSizeBytes:8*format.BlockSize,WALSegmentBytes:1<<20,MappingBytes:8<<20,CheckpointBytes:64<<10})
	if err!=nil{t.Fatal(err)}
	var uuid [16]byte;copy(uuid[:],"bm03-acceptance")
	sb,err:=format.NewDynamicSuperblock(g,uuid,1,1);if err!=nil{t.Fatal(err)}
	if err=FormatDynamicPool(meta,sb);err!=nil{t.Fatal(err)}
	arenaStart:=g.ReservedOffset/format.PageSize
	desc:=format.DynamicVolumeDescriptor{VolumeID:"bm03-acceptance-volume",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:9,ArenaStart:arenaStart,ArenaPages:512,
		RootPage:metastore.NoPage,LifecycleStart:arenaStart+512,LifecyclePages:8}
	if err=format.InitializeDynamicVolumeDescriptor(meta,sb,0,desc);err!=nil{t.Fatal(err)}
	if err=meta.Sync();err!=nil{t.Fatal(err)}
	store,err:=metastore.OpenDynamicPoolVolume(meta,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
	locks:=&crashDistributedLocker{}
	fault:=&failArenaMetadataView{inner:meta,arenaStart:arenaStart}
	committer,err:=wal.NewDynamicSharedCommitter(fault,sb,0,locks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil{t.Fatal(err)}
	tree,err:=btree.OpenPersistentWithExternalPages(store,committer,ResolveIdempotencyPages)
	if err!=nil{t.Fatal(err)}
	alloc,err:=NewDynamicPoolAllocator(store,tree,locks,data);if err!=nil{t.Fatal(err)}
	lease:=&crashOwnerLease{resource:"volume:"+desc.VolumeID,epoch:desc.OwnerEpoch}
	var key [16]byte;var fp [32]byte;key[0]=0x55;fp[0]=0xaa
	first:=AllocateRequest{IdempotencyKey:key,Fingerprint:fp,LogicalStart:10,BlockCount:1,Operation:1}
	if _,err=alloc.AllocateZeroPublishTxn(context.Background(),lease,first);!errors.Is(err,ErrAllocationOutcomeUnknown){
		t.Fatalf("expected uncertain WAL outcome, got %v",err)
	}
	if !fault.fired{t.Fatal("metadata after-image failure was not injected")}
	// Simulate process death: discard the old Store/Tree and recover only from
	// the durable DynamicJournal before opening a new B+Tree.
	recoveredJournal,err:=wal.OpenDynamicJournal(meta,sb,0);if err!=nil{t.Fatal(err)}
	if _,err=recoveredJournal.Recover();err!=nil{t.Fatal(err)}
	store2,err:=metastore.OpenDynamicPoolVolume(meta,sb,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	committer2,err:=wal.NewDynamicSharedCommitter(meta,sb,0,locks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil{t.Fatal(err)}
	tree2,err:=btree.OpenPersistentWithExternalPages(store2,committer2,ResolveIdempotencyPages);if err!=nil{t.Fatal(err)}
	alloc2,err:=NewDynamicPoolAllocator(store2,tree2,locks,data);if err!=nil{t.Fatal(err)}
	physical,err:=tree2.Get(first.LogicalStart);if err!=nil||physical!=0{t.Fatalf("recovered Mapping=%d err=%v",physical,err)}
	head,err:=tree2.Get(IdempotencyHash(first.IdempotencyKey));if err!=nil{t.Fatal(err)}
	record,found,err:=LookupIdempotencyRecord(store2,head,first.IdempotencyKey,first.Fingerprint)
	if err!=nil||!found||record.PhysicalStart!=0{t.Fatalf("recovered idempotency record=%+v found=%v err=%v",record,found,err)}
	if !bytes.Equal(parent.b[int(oldDataBase):int(oldDataBase+format.BlockSize)],make([]byte,format.BlockSize)){
		t.Fatal("allocated DataView block was not durably zeroed before metadata commit")
	}
	replayed,err:=alloc2.AllocateZeroPublishTxn(context.Background(),lease,first)
	if err!=nil||!replayed.Replayed||replayed.PhysicalStart!=0{t.Fatalf("recovered idempotent retry=%+v err=%v",replayed,err)}

	// New Bootstrap Extent Map generation exposes extra Metadata and Data
	// extents. The logical views concatenate them without moving old offsets.
	metaExpanded,err:=poolspace.NewMetadataView(parent,[]poolspace.Extent{
		{Start:0,Length:poolspace.BootstrapSize,Kind:poolspace.Metadata},
		{Start:poolspace.BootstrapSize,Length:oldMetadata,Kind:poolspace.Metadata},
		{Start:extraMetaBase,Length:extraMetadata,Kind:poolspace.Metadata},
	})
	if err!=nil{t.Fatal(err)}
	dataExpanded,err:=poolspace.NewDataView(parent,[]poolspace.Extent{
		{Start:oldDataBase,Length:oldData,Kind:poolspace.Data},
		{Start:extraDataBase,Length:extraData,Kind:poolspace.Data},
	})
	if err!=nil{t.Fatal(err)}
	next,err:=BuildDynamicGeometryUpdate(sb,uint64(metaExpanded.Size()),uint64(dataExpanded.Size()),2)
	if err!=nil{t.Fatal(err)}
	growCommitter,err:=wal.NewDynamicSharedCommitter(metaExpanded,sb,0,locks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil{t.Fatal(err)}
	if err=PublishDynamicGeometry(metaExpanded,sb,next,growCommitter);err!=nil{t.Fatal(err)}
	opened,err:=OpenDynamicSuperblockPair(metaExpanded);if err!=nil{t.Fatal(err)}
	if opened!=next{t.Fatalf("expanded geometry not published: %+v",opened)}
	store3,err:=metastore.OpenDynamicPoolVolume(metaExpanded,next,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	committer3,err:=wal.NewDynamicSharedCommitter(metaExpanded,next,0,locks,"blockmesh:pool:bm03:ag:000:wal")
	if err!=nil{t.Fatal(err)}
	tree3,err:=btree.OpenPersistentWithExternalPages(store3,committer3,ResolveIdempotencyPages);if err!=nil{t.Fatal(err)}
	alloc3,err:=NewDynamicPoolAllocator(store3,tree3,locks,dataExpanded);if err!=nil{t.Fatal(err)}
	second:=AllocateRequest{IdempotencyKey:[16]byte{0x56},Fingerprint:[32]byte{0xbb},LogicalStart:11,BlockCount:3,Operation:1}
	r2,err:=alloc3.AllocateZeroPublishTxn(context.Background(),lease,second);if err!=nil{t.Fatal(err)}
	if r2.PhysicalStart!=1{t.Fatalf("second allocation physical=%d want=1",r2.PhysicalStart)}
	third:=AllocateRequest{IdempotencyKey:[16]byte{0x57},Fingerprint:[32]byte{0xbc},LogicalStart:14,BlockCount:1,Operation:1}
	r3,err:=alloc3.AllocateZeroPublishTxn(context.Background(),lease,third);if err!=nil{t.Fatal(err)}
	if r3.PhysicalStart!=4{t.Fatalf("allocation from newly exposed Data capacity=%d want=4",r3.PhysicalStart)}
}
