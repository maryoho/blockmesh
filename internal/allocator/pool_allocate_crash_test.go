package allocator

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"testing"

	"blockmesh/internal/btree"
	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
	"blockmesh/internal/wal"
)

type crashPoolMetadataDevice struct {
	inner device.Device
	mu sync.Mutex
	op string
	at int
	after bool
	writes int
	syncs int
	fired bool
}

func (d *crashPoolMetadataDevice) arm(op string, at int, after bool) {
	d.mu.Lock(); defer d.mu.Unlock()
	d.op, d.at, d.after = op, at, after
	d.writes, d.syncs, d.fired = 0, 0, false
}
func (d *crashPoolMetadataDevice) disarm() {
	d.mu.Lock(); defer d.mu.Unlock()
	d.op, d.at, d.fired = "", 0, false
}
func (d *crashPoolMetadataDevice) hit(op string, n int) (bool, bool) {
	d.mu.Lock(); defer d.mu.Unlock()
	if d.fired || d.op != op || d.at != n { return false, false }
	d.fired = true
	return true, d.after
}
func (d *crashPoolMetadataDevice) ReadAt(p []byte, off int64) (int,error) { return d.inner.ReadAt(p,off) }
func (d *crashPoolMetadataDevice) WriteAt(p []byte, off int64) (int,error) {
	d.mu.Lock(); d.writes++; n:=d.writes; d.mu.Unlock()
	fail,after:=d.hit("write",n)
	if fail&&!after { return 0,errors.New("injected metadata write crash") }
	written,err:=d.inner.WriteAt(p,off)
	if fail { return written,errors.New("injected metadata write completion uncertain") }
	return written,err
}
func (d *crashPoolMetadataDevice) Sync() error {
	d.mu.Lock();d.syncs++;n:=d.syncs;d.mu.Unlock()
	fail,after:=d.hit("sync",n)
	if fail&&!after{return errors.New("injected metadata sync crash")}
	err:=d.inner.Sync()
	if fail{return errors.New("injected metadata sync completion uncertain")}
	return err
}
func (d *crashPoolMetadataDevice) Size() int64 { return d.inner.Size() }
func (d *crashPoolMetadataDevice) Close() error { return d.inner.Close() }

type crashDataDevice struct {
	mu sync.Mutex
	data []byte
	failWrite bool
	failSync bool
	writes int
	syncs int
}
func newCrashDataDevice() *crashDataDevice {
	return &crashDataDevice{data:bytes.Repeat([]byte{0xa5},int(format.BlockSize))}
}
func (d *crashDataDevice) WriteAt(p []byte,off int64)(int,error) {
	d.mu.Lock();defer d.mu.Unlock()
	d.writes++
	if off<0||off>=int64(len(d.data)){return 0,errors.New("data write outside test extent")}
	n:=len(p)
	if int64(n)>int64(len(d.data))-off {n=len(d.data)-int(off)}
	if d.failWrite {
		// Simulate a torn write: a prefix reaches media but the operation fails.
		n/=4
		copy(d.data[int(off):int(off)+n],p[:n])
		return n,errors.New("injected partial data write")
	}
	copy(d.data[int(off):int(off)+n],p[:n])
	return n,nil
}
func (d *crashDataDevice) Sync() error {
	d.mu.Lock();defer d.mu.Unlock();d.syncs++
	if d.failSync{return errors.New("injected data flush failure")}
	return nil
}
func (d *crashDataDevice) Size() int64 { return int64(format.PoolSize) }
func (d *crashDataDevice) allZero() bool {
	d.mu.Lock();defer d.mu.Unlock()
	return bytes.Equal(d.data,make([]byte,len(d.data)))
}

type crashDistributedLocker struct{ mu sync.Mutex }
func (*crashDistributedLocker) Distributed() bool { return true }
func (l *crashDistributedLocker) Acquire(ctx context.Context,resource string)(lock.Lease,error) {
	return nil,errors.New("Owner lease is supplied by the test fixture")
}
func (l *crashDistributedLocker) Lock(ctx context.Context,resource string)(func()error,error) {
	if err:=ctx.Err();err!=nil{return nil,err}
	l.mu.Lock()
	return func()error{l.mu.Unlock();return nil},nil
}
type crashOwnerLease struct{ resource string; epoch uint64; closed bool }
func (l *crashOwnerLease) Resource()string{return l.resource}
func (l *crashOwnerLease) Valid()bool{return !l.closed}
func (l *crashOwnerLease) Epoch()uint64{return l.epoch}
func (l *crashOwnerLease) Close()error{l.closed=true;return nil}

// clusterSafeJournal is deliberately test-only: the fixture models the
// production contract in which a SAN lock/owner coordinator serializes access
// while the Journal supplies durable per-AG redo.
type clusterSafeJournal struct{ *wal.Journal }
func (*clusterSafeJournal) ClusterSafe()bool{return true}

type allocationCrashFixture struct {
	dev *crashPoolMetadataDevice
	pool format.Superblock
	data *crashDataDevice
	store *metastore.Store
	tree *btree.Tree
	journal *clusterSafeJournal
	allocator *PoolAllocator
	locks *crashDistributedLocker
	lease *crashOwnerLease
}

func newAllocationCrashFixture(t *testing.T) *allocationCrashFixture {
	t.Helper()
	path:=t.TempDir()+"/pool-metadata.img"
	f,err:=os.Create(path);if err!=nil{t.Fatal(err)}
	if err=f.Truncate(int64(format.MetadataSize));err!=nil{t.Fatal(err)}
	if err=f.Close();err!=nil{t.Fatal(err)}
	raw,err:=device.Open(path,true);if err!=nil{t.Fatal(err)}
	t.Cleanup(func(){_ = raw.Close()})
	dev:=&crashPoolMetadataDevice{inner:raw}
	pool,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	if _,err=dev.WriteAt(pool.Encode(),int64(format.SuperAOffset));err!=nil{t.Fatal(err)}
	pool.Generation=2
	if _,err=dev.WriteAt(pool.Encode(),int64(format.SuperBOffset));err!=nil{t.Fatal(err)}

	// Initialize AG 0's descriptor and bitmap as an empty, checksummed bitmap.
	bitmap:=make([]byte,format.BitmapBytes)
	descPage:=make([]byte,format.PageSize)
	binary.LittleEndian.PutUint64(descPage[:8],bitmapDescriptorMagic)
	binary.LittleEndian.PutUint32(descPage[8:12],0)
	binary.LittleEndian.PutUint64(descPage[16:24],1)
	binary.LittleEndian.PutUint32(descPage[24:28],crc32.Checksum(bitmap,castagnoli))
	if _,err=dev.WriteAt(descPage,int64(pool.BitmapOffset));err!=nil{t.Fatal(err)}
	for p:=uint64(0);p<format.BitmapPages;p++ {
		image:=bitmap[p*format.PageSize:(p+1)*format.PageSize]
		if _,err=dev.WriteAt(image,int64(pool.BitmapOffset+format.PageSize+p*format.PageSize));err!=nil{t.Fatal(err)}
	}

	pair,err:=format.VolumeDescriptorPairOffset(pool,0);if err!=nil{t.Fatal(err)}
	reservedPages:=pool.ReservedOffset/format.PageSize
	slotBytes:=(pool.MetadataSize-pool.ReservedOffset)/uint64(format.MaxVolumeDescriptors)/format.PageSize*format.PageSize
	slotPages:=slotBytes/format.PageSize
	arenaStart:=reservedPages+format.OwnerRecordPages
	arenaPages:=uint64(512)
	desc:=format.VolumeDescriptor{
		VolumeID:"allocation-crash-volume",AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:pool.MappingOffset,BitmapSequence:1,OwnerEpoch:7,
		ArenaStart:arenaStart,ArenaPages:arenaPages,RootPage:metastore.NoPage,
		LifecycleStart:arenaStart+arenaPages,LifecyclePages:8,
	}
	if desc.LifecycleStart+desc.LifecyclePages>reservedPages+slotPages-format.ReaderTablePages {
		t.Fatal("test Volume metadata geometry does not fit bootstrap slot")
	}
	descImage,err:=desc.Encode();if err!=nil{t.Fatal(err)}
	if _,err=dev.WriteAt(descImage,int64(pair));err!=nil{t.Fatal(err)}
	if _,err=dev.WriteAt(descImage,int64(pair+format.PageSize));err!=nil{t.Fatal(err)}
	if err=dev.Sync();err!=nil{t.Fatal(err)}

	store,err:=metastore.OpenPoolVolume(dev,pool,0,desc.VolumeID);if err!=nil{t.Fatal(err)}
	if err=metastore.FormatLifecycle(store);err!=nil{t.Fatal(err)}
	j,err:=wal.New(dev,pool,0);if err!=nil{t.Fatal(err)}
	clusterJournal:=&clusterSafeJournal{Journal:j}
	tree,err:=btree.OpenPersistentWithExternalPages(store,clusterJournal,ResolveIdempotencyPages)
	if err!=nil{t.Fatal(err)}
	data:=newCrashDataDevice()
	locks:=&crashDistributedLocker{}
	lease:=&crashOwnerLease{resource:"volume:"+desc.VolumeID,epoch:desc.OwnerEpoch}
	a,err:=NewPoolAllocator(store,tree,locks,data);if err!=nil{t.Fatal(err)}
	return &allocationCrashFixture{dev:dev,pool:pool,data:data,store:store,tree:tree,journal:clusterJournal,allocator:a,locks:locks,lease:lease}
}

func (f *allocationCrashFixture) reopen(t *testing.T) {
	t.Helper()
	// The old in-memory Store/Tree/Arena are intentionally abandoned, as they
	// would be after process death. wal.New must replay before the tree opens.
	j,err:=wal.New(f.dev,f.pool,0);if err!=nil{t.Fatalf("WAL recovery: %v",err)}
	store,err:=metastore.OpenPoolVolume(f.dev,f.pool,0,"allocation-crash-volume");if err!=nil{t.Fatal(err)}
	clusterJournal:=&clusterSafeJournal{Journal:j}
	tree,err:=btree.OpenPersistentWithExternalPages(store,clusterJournal,ResolveIdempotencyPages)
	if err!=nil{t.Fatalf("reopen persistent tree: %v",err)}
	a,err:=NewPoolAllocator(store,tree,f.locks,f.data);if err!=nil{t.Fatal(err)}
	f.store,f.tree,f.journal,f.allocator=store,tree,clusterJournal,a
}

func crashRequest() AllocateRequest {
	var key [16]byte;var fingerprint [32]byte
	key[0]=0x31;fingerprint[0]=0x72
	return AllocateRequest{IdempotencyKey:key,Fingerprint:fingerprint,LogicalStart:41,BlockCount:1,Operation:1}
}

func assertNoAllocationPublished(t *testing.T,f *allocationCrashFixture,req AllocateRequest) {
	t.Helper()
	if _,err:=f.tree.Get(req.LogicalStart);!errors.Is(err,btree.ErrKeyNotFound){t.Fatalf("Mapping visible after failed allocation: err=%v",err)}
	if _,err:=f.tree.Get(IdempotencyHash(req.IdempotencyKey));!errors.Is(err,btree.ErrKeyNotFound){t.Fatalf("idempotency index visible after failed allocation: err=%v",err)}
	bitmap:=make([]byte,format.PageSize)
	if n,err:=f.dev.ReadAt(bitmap,int64(f.pool.BitmapOffset+format.PageSize));err!=nil||n!=len(bitmap){t.Fatalf("read bitmap n=%d err=%v",n,err)}
	if bitmap[0]&1!=0{t.Fatal("Bitmap bit 0 published despite failed data zero")}
}

func assertAllocationPublished(t *testing.T,f *allocationCrashFixture,req AllocateRequest,wantPhysical uint64) {
	t.Helper()
	physical,err:=f.tree.Get(req.LogicalStart);if err!=nil||physical!=wantPhysical{t.Fatalf("Mapping physical=%d err=%v want=%d",physical,err,wantPhysical)}
	head,err:=f.tree.Get(IdempotencyHash(req.IdempotencyKey));if err!=nil{t.Fatalf("idempotency index: %v",err)}
	record,found,err:=LookupIdempotencyRecord(f.store,head,req.IdempotencyKey,req.Fingerprint)
	if err!=nil||!found||record.PhysicalStart!=wantPhysical{t.Fatalf("idempotency record=%+v found=%v err=%v",record,found,err)}
	bitmap:=make([]byte,format.PageSize)
	if n,e:=f.dev.ReadAt(bitmap,int64(f.pool.BitmapOffset+format.PageSize));e!=nil||n!=len(bitmap){t.Fatalf("read bitmap n=%d err=%v",n,e)}
	if bitmap[0]&1==0{t.Fatal("Mapping is visible but Bitmap bit 0 is free")}
	if !f.data.allZero(){t.Fatal("Mapping is visible but allocated data block is not zero")}
}

func TestAllocateZeroPublishCrashRecoveryDataWriteFailure(t *testing.T) {
	f:=newAllocationCrashFixture(t);req:=crashRequest()
	f.data.failWrite=true
	if _,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req);err==nil{t.Fatal("expected partial data write failure")}
	assertNoAllocationPublished(t,f,req)
	f.data.failWrite=false
	f.data.writes=0
	got,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req);if err!=nil{t.Fatalf("retry after failed zero: %v",err)}
	if got.PhysicalStart!=0{t.Fatalf("physical=%d want 0",got.PhysicalStart)}
	assertAllocationPublished(t,f,req,0)
	replayed,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req)
	if err!=nil||!replayed.Replayed||replayed.PhysicalStart!=0{t.Fatalf("idempotent retry=%+v err=%v",replayed,err)}
}

func TestAllocateZeroPublishCrashRecoveryDataSyncFailure(t *testing.T) {
	f:=newAllocationCrashFixture(t);req:=crashRequest()
	f.data.failSync=true
	if _,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req);err==nil{t.Fatal("expected data Sync failure")}
	assertNoAllocationPublished(t,f,req)
	f.data.failSync=false
	if _,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req);err!=nil{t.Fatalf("retry after Sync failure: %v",err)}
	assertAllocationPublished(t,f,req,0)
}

func TestAllocateZeroPublishCrashRecoveryCommittedWAL(t *testing.T) {
	for _,point:=range []struct{name,op string;at int;after bool}{
		{name:"commit-sync-durable-error",op:"sync",at:2,after:true},
		{name:"first-after-image-write-failure",op:"write",at:3,after:false},
	} {
		t.Run(point.name,func(t *testing.T){
			f:=newAllocationCrashFixture(t);req:=crashRequest()
			f.dev.arm(point.op,point.at,point.after)
			_,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req)
			if !errors.Is(err,ErrAllocationOutcomeUnknown){t.Fatalf("allocation error=%v, want ErrAllocationOutcomeUnknown",err)}
			f.dev.disarm()
			// Abandon all old process state and recover from the durable AG WAL.
			f.reopen(t)
			assertAllocationPublished(t,f,req,0)
			replayed,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req)
			if err!=nil||!replayed.Replayed||replayed.PhysicalStart!=0{t.Fatalf("post-recovery retry=%+v err=%v",replayed,err)}
		})
	}
}

func TestAllocateZeroPublishCrashRecoveryIntentWithoutCommit(t *testing.T) {
	f:=newAllocationCrashFixture(t);req:=crashRequest()
	// Intent is the first WAL write; fail before the second (Commit) record.
	f.dev.arm("write",2,false)
	_,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req)
	if !errors.Is(err,ErrAllocationOutcomeUnknown){t.Fatalf("allocation error=%v, want uncertain outcome",err)}
	f.dev.disarm()
	f.reopen(t)
	assertNoAllocationPublished(t,f,req)
	// Reusing the same idempotency key is safe: no committed record exists.
	got,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),f.lease,req)
	if err!=nil||got.Replayed||got.PhysicalStart!=0{t.Fatalf("retry after uncommitted intent=%+v err=%v",got,err)}
	assertAllocationPublished(t,f,req,0)
}

func TestAllocateZeroPublishCrashRecoveryRejectsStaleOwnerEpoch(t *testing.T) {
	f:=newAllocationCrashFixture(t);req:=crashRequest()
	stale:=&crashOwnerLease{resource:f.lease.resource,epoch:f.lease.epoch-1}
	if _,err:=f.allocator.AllocateZeroPublishTxn(context.Background(),stale,req);err==nil{t.Fatal("stale OwnerEpoch accepted")}
	assertNoAllocationPublished(t,f,req)
}

var _ = fmt.Sprintf
