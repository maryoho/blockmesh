package allocator

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"sync"

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


type failAtDynamicMetadataDevice struct {
	wal.DynamicMetadataDevice
	failAt int64
	failed bool
}

func (d *failAtDynamicMetadataDevice) WriteAt(p []byte, off int64) (int, error) {
	if !d.failed && off >= 0 && off <= d.failAt && int64(len(p)) > d.failAt-off {
		prefix := int(d.failAt-off)
		if prefix > 0 {
			n, err := d.DynamicMetadataDevice.WriteAt(p[:prefix], off)
			if err != nil { return n, err }
			if n != prefix { return n, errors.New("short write before injected metadata failure") }
		}
		d.failed = true
		return prefix, errors.New("injected WAL target apply failure")
	}
	return d.DynamicMetadataDevice.WriteAt(p, off)
}

type dynamicRetireReaderCoordinator struct { mu sync.Mutex }
func (c *dynamicRetireReaderCoordinator) LockReaderSet(string) (func(), error) {
	c.mu.Lock()
	return c.mu.Unlock, nil
}

func TestBMPOOL03DataRetireWaitsForReaderGenerationAndReclaimsBitmapAtomically(t *testing.T) {
	g, err := format.PlanDynamicGeometry(format.DynamicGeometryConfig{
		DataBytes:8*format.BlockSize, MetadataBytes:64<<20, AGSizeBytes:8*format.BlockSize,
		WALSegmentBytes:1<<20, MappingBytes:8<<20, CheckpointBytes:64<<10,
	})
	if err != nil { t.Fatal(err) }
	parentBytes := poolspace.BootstrapSize+g.MetadataBytes+g.DataBytes
	parent := newDynamicFormatMem(parentBytes)
	dataBase := poolspace.BootstrapSize+g.MetadataBytes
	metadata, err := poolspace.NewMetadataView(parent, []poolspace.Extent{
		{Start:0, Length:poolspace.BootstrapSize, Kind:poolspace.Metadata},
		{Start:poolspace.BootstrapSize, Length:g.MetadataBytes, Kind:poolspace.Metadata},
	})
	if err != nil { t.Fatal(err) }
	data, err := poolspace.NewDataView(parent, []poolspace.Extent{{Start:dataBase, Length:g.DataBytes, Kind:poolspace.Data}})
	if err != nil { t.Fatal(err) }
	var uuid [16]byte
	copy(uuid[:], "bm03-data-retire")
	sb, err := format.NewDynamicSuperblock(g, uuid, 1, 1)
	if err != nil { t.Fatal(err) }
	if err = FormatDynamicPool(metadata, sb); err != nil { t.Fatal(err) }
	arenaStart := g.ReservedOffset/format.PageSize
	desc := format.DynamicVolumeDescriptor{
		VolumeID:"bm03-data-retire-volume", AG:0, Generation:1, VolumeSequence:1,
		MappingRootOffset:g.MappingOffset, BitmapSequence:1, OwnerEpoch:7,
		ArenaStart:arenaStart, ArenaPages:512, RootPage:metastore.NoPage,
		LifecycleStart:arenaStart+512, LifecyclePages:8,
		OwnerRecordStart:arenaStart+512+8,
		ReaderTableStart:arenaStart+512+8+format.OwnerRecordPages,
		ReaderTablePages:uint32(format.ReaderTablePages),
	}
	if err = format.InitializeDynamicVolumeDescriptor(metadata, sb, 0, desc); err != nil { t.Fatal(err) }
	if err = metadata.Sync(); err != nil { t.Fatal(err) }
	store, err := metastore.OpenDynamicPoolVolume(metadata, sb, 0, desc.VolumeID)
	if err != nil { t.Fatal(err) }
	if err = metastore.FormatLifecycle(store); err != nil { t.Fatal(err) }
	locks := &crashDistributedLocker{}
	walLocks := &crashDistributedLocker{}
	committer, err := wal.NewDynamicSharedCommitter(metadata, sb, 0, walLocks, "blockmesh:pool:bm03:ag:000:wal")
	if err != nil { t.Fatal(err) }
	coordinator := &dynamicRetireReaderCoordinator{}
	tree, err := btree.OpenPersistentWithExternalPages(store, committer, ResolveIdempotencyPages, coordinator)
	if err != nil { t.Fatal(err) }
	readerStart, readerPages, err := store.ReaderTableRange()
	if err != nil { t.Fatal(err) }
	readers, err := metastore.NewReaderTable(store, readerStart, readerPages, committer, coordinator)
	if err != nil { t.Fatal(err) }
	alloc, err := NewDynamicPoolAllocator(store, tree, locks, data)
	if err != nil { t.Fatal(err) }
	lease := &crashOwnerLease{resource:"volume:"+desc.VolumeID, epoch:desc.OwnerEpoch}

	first, err := alloc.AllocateZeroPublishTxn(context.Background(), lease, AllocateRequest{
		IdempotencyKey:[16]byte{0x61}, Fingerprint:[32]byte{0xa1},
		LogicalStart:3, BlockCount:1, Operation:1,
	})
	if err != nil { t.Fatal(err) }
	oldRoot := tree.RootGeneration()
	if err = readers.Register(0, "reader-node", "reader-boot", 1, oldRoot); err != nil { t.Fatal(err) }
	oldData := bytes.Repeat([]byte{0x6d}, int(format.BlockSize))
	if n, e := data.WriteAt(oldData, int64(first.PhysicalStart*format.BlockSize)); e != nil || n != len(oldData) {
		t.Fatalf("seed source block n=%d err=%v", n, e)
	}
	if err = data.Sync(); err != nil { t.Fatal(err) }

	second, err := alloc.AllocateZeroPublishTxn(context.Background(), lease, AllocateRequest{
		IdempotencyKey:[16]byte{0x62}, Fingerprint:[32]byte{0xa2},
		LogicalStart:3, BlockCount:1, Operation:1,
	})
	if err != nil { t.Fatal(err) }
	if second.PhysicalStart == first.PhysicalStart { t.Fatal("COW reused source block before Reader-safe reclamation") }
	copied := make([]byte, format.BlockSize)
	if n, e := data.ReadAt(copied, int64(second.PhysicalStart*format.BlockSize)); e != nil || n != len(copied) || !bytes.Equal(copied, oldData) {
		t.Fatalf("COW copy did not preserve source data: n=%d err=%v", n, e)
	}
	// Reopen before collection: the pending Data Retire record must be found
	// through the reserved B+Tree index namespace after process recovery.
	pendingStore, e := metastore.OpenDynamicPoolVolume(metadata, sb, 0, desc.VolumeID)
	if e != nil { t.Fatal(e) }
	pendingTree, e := btree.OpenPersistentWithExternalPages(pendingStore, committer, ResolveIdempotencyPages, coordinator)
	if e != nil { t.Fatalf("reopen with pending Data Retire record: %v", e) }
	pendingRecords := 0
	for _, item := range pendingTree.Snapshot() {
		if item.Key < g.TotalBlocks || item.Key>>63 != 0 { continue }
		page, readErr := pendingStore.ReadPage(item.Value)
		if readErr != nil { t.Fatal(readErr) }
		if page.Type == DataRetireRecordPageType { pendingRecords++ }
	}
	if pendingRecords != 1 { t.Fatalf("recovered pending Data Retire records=%d, want 1", pendingRecords) }
	if _, err := alloc.CollectDataRetired(context.Background(), lease, readers, false); !errors.Is(err, metastore.ErrUnsafeReclaim) {
		t.Fatalf("collector must reject missing I/O drain proof: %v", err)
	}
	if got, err := alloc.CollectDataRetired(context.Background(), lease, readers, true); err != nil || got != 0 {
		t.Fatalf("old Reader must pin COW source: reclaimed=%d err=%v", got, err)
	}
	bitmap := make([]byte, g.BitmapPaddedBytes)
	if _, err = metadata.ReadAt(bitmap, int64(g.BitmapOffset+format.PageSize)); err != nil { t.Fatal(err) }
	if bitmap[0]&0x03 != 0x03 { t.Fatalf("source block was freed while Reader pinned it: %08b", bitmap[0]) }

	if err = readers.AdvanceGeneration(0, "reader-node", "reader-boot", 1, tree.RootGeneration(), true); err != nil { t.Fatal(err) }
	// Inject a failure while the WAL applies the Bitmap after-image. The
	// collector must report an ambiguous outcome; WAL recovery must replay both
	// the queue deletion and Bitmap clear, never just one of them.
	fault := &failAtDynamicMetadataDevice{DynamicMetadataDevice:metadata, failAt:int64(g.BitmapOffset+format.PageSize)}
	committer.Device = fault
	if _, err = alloc.CollectDataRetired(context.Background(), lease, readers, true); !errors.Is(err, ErrAllocationOutcomeUnknown) {
		t.Fatalf("Bitmap apply failure must be reported as ambiguous: %v", err)
	}
	if !fault.failed { t.Fatal("Bitmap after-image failure injection did not fire") }
	if err = committer.Recover(); err != nil { t.Fatalf("recover Data Retire/Bitmap WAL transaction: %v", err) }

	// Reopen every metadata object after recovery. The committed WAL transaction
	// must have removed the queue entry and freed the source bit atomically.
	store, err = metastore.OpenDynamicPoolVolume(metadata, sb, 0, desc.VolumeID)
	if err != nil { t.Fatal(err) }
	tree, err = btree.OpenPersistentWithExternalPages(store, committer, ResolveIdempotencyPages, coordinator)
	if err != nil { t.Fatalf("reopen recovered Data Retire root: %v", err) }
	readers, err = metastore.NewReaderTable(store, readerStart, readerPages, committer, coordinator)
	if err != nil { t.Fatal(err) }
	alloc, err = NewDynamicPoolAllocator(store, tree, locks, data)
	if err != nil { t.Fatal(err) }
	if _, err = metadata.ReadAt(bitmap, int64(g.BitmapOffset+format.PageSize)); err != nil { t.Fatal(err) }
	if bitmap[0]&0x01 != 0 || bitmap[0]&0x02 == 0 {
		t.Fatalf("WAL recovery did not atomically apply COW reclaim bitmap: %08b", bitmap[0])
	}
	for _, item := range tree.Snapshot() {
		if item.Key < format.DynamicRetireIndexKeyStart || item.Key >= format.DynamicRetireIndexKeyLimit { continue }
		page, readErr := store.ReadPage(item.Value)
		if readErr != nil { t.Fatal(readErr) }
		if page.Type == DataRetireRecordPageType { t.Fatal("Data Retire queue entry survived committed Bitmap release") }
	}
	// The Data Retire record page itself was retired by the same root
	// transaction. Advance past that deletion root and reclaim it through the
	// PersistentArena lifecycle queue.
	if err = readers.AdvanceGeneration(0, "reader-node", "reader-boot", 1, tree.RootGeneration(), true); err != nil { t.Fatal(err) }
	if got, err := alloc.CollectDataRetired(context.Background(), lease, readers, true); err != nil || got != 0 {
		t.Fatalf("metadata-only retire collection reclaimed=%d err=%v", got, err)
	}

	_, err = alloc.DiscardTxn(context.Background(), lease, AllocateRequest{
		IdempotencyKey:[16]byte{0x63}, Fingerprint:[32]byte{0xa3},
		LogicalStart:3, BlockCount:1, Operation:3,
	})
	if err != nil { t.Fatal(err) }
	if got, err := alloc.CollectDataRetired(context.Background(), lease, readers, true); err != nil || got != 0 {
		t.Fatalf("Reader pinned at pre-discard root must delay discard reclaim: reclaimed=%d err=%v", got, err)
	}
	if err = readers.AdvanceGeneration(0, "reader-node", "reader-boot", 1, tree.RootGeneration(), true); err != nil { t.Fatal(err) }
	if got, err := alloc.CollectDataRetired(context.Background(), lease, readers, true); err != nil || got != 1 {
		t.Fatalf("advanced Reader should release discarded block: reclaimed=%d err=%v", got, err)
	}
	if _, err = metadata.ReadAt(bitmap, int64(g.BitmapOffset+format.PageSize)); err != nil { t.Fatal(err) }
	if bitmap[0]&0x03 != 0 { t.Fatalf("discarded block remains allocated after safe reclaim: %08b", bitmap[0]) }
	if err = readers.AdvanceGeneration(0, "reader-node", "reader-boot", 1, tree.RootGeneration(), true); err != nil { t.Fatal(err) }
	if got, err := alloc.CollectDataRetired(context.Background(), lease, readers, true); err != nil || got != 0 {
		t.Fatalf("final metadata retire collection reclaimed=%d err=%v", got, err)
	}

	// A fresh open must resolve any remaining external idempotency pages and
	// accept the empty Data Retire index after the WAL-published collection.
	store2, err := metastore.OpenDynamicPoolVolume(metadata, sb, 0, desc.VolumeID)
	if err != nil { t.Fatal(err) }
	tree2, err := btree.OpenPersistentWithExternalPages(store2, committer, ResolveIdempotencyPages, coordinator)
	if err != nil { t.Fatalf("reopen after Data Retire collection: %v", err) }
	if len(tree2.Snapshot()) == 0 { t.Fatal("idempotency indexes disappeared during collection") }
}
