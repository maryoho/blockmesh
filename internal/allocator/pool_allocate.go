package allocator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"blockmesh/internal/btree"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
	"blockmesh/internal/wal"
)

var (
	ErrAllocationOutcomeUnknown = errors.New("allocation WAL outcome is unknown; recover WAL and reopen the Volume before retrying")
	ErrLogicalRangeMapped       = errors.New("logical allocation range already contains mapped blocks")
	ErrPoolAllocatorUnavailable = errors.New("Pool allocator requires a persistent B+Tree and cluster-safe WAL")
)

// AllocateRequest describes one idempotent contiguous logical allocation.
// Fingerprint must be computed by the caller from the complete canonical
// request (Volume, operation, logical range, size and relevant options).
type AllocateRequest struct {
	IdempotencyKey [16]byte
	Fingerprint    [32]byte
	LogicalStart   uint64
	BlockCount     uint32
	Operation      uint16
}

// AllocateResult is the durable result of an allocation. Replayed is true
// when the idempotency key resolved to an earlier committed allocation.
type AllocateResult struct {
	LogicalStart uint64
	PhysicalStart uint64
	BlockCount uint32
	OwnerEpoch uint64
	Replayed bool
}

// PoolAllocator coordinates Pool bitmap allocation, data zeroing, the
// idempotency collision chain and B+Tree mapping publication. Its lock and WAL
// dependencies must be distributed implementations for shared-SAN use.
type PoolAllocator struct {
	store *metastore.Store
	tree *btree.Tree
	locks lock.DistributedLocker
	data DataDevice
	dynamic bool
}

func NewPoolAllocator(store *metastore.Store, tree *btree.Tree, locks lock.DistributedLocker, data DataDevice) (*PoolAllocator, error) {
	if store == nil || tree == nil || locks == nil || data == nil || !locks.Distributed() {
		return nil, ErrPoolAllocatorUnavailable
	}
	if _, err := store.PoolSuperblock(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPoolAllocatorUnavailable, err)
	}
	if !tree.UsesPersistentArena() || !tree.UsesClusterSafeCommitter() {
		return nil, ErrPoolAllocatorUnavailable
	}
	return &PoolAllocator{store: store, tree: tree, locks: locks, data: data}, nil
}

// NewDynamicPoolAllocator binds the allocator to BMPOOL03 logical views. The
// data argument must be the DataView; its byte zero is physical block ID zero.
func NewDynamicPoolAllocator(store *metastore.Store, tree *btree.Tree, locks lock.DistributedLocker, data DataDevice) (*PoolAllocator,error) {
	if store==nil||tree==nil||locks==nil||data==nil||!locks.Distributed(){return nil,ErrPoolAllocatorUnavailable}
	sb,err:=store.DynamicPoolSuperblock();if err!=nil{return nil,fmt.Errorf("%w: %v",ErrPoolAllocatorUnavailable,err)}
	if _,err=store.DynamicPoolDescriptor();err!=nil{return nil,fmt.Errorf("%w: %v",ErrPoolAllocatorUnavailable,err)}
	if !tree.UsesPersistentArena()||!tree.UsesClusterSafeCommitter(){return nil,ErrPoolAllocatorUnavailable}
	if data.Size()<0||uint64(data.Size())<sb.Geometry.DataBytes{return nil,errors.New("DataView is smaller than BMPOOL03 Data geometry")}
	if view,ok:=data.(interface{Generation()uint64});ok&&view.Generation()!=0&&view.Generation()!=sb.ViewGeneration{return nil,errors.New("DataView Extent Map generation does not match BMPOOL03 superblock")}
	return &PoolAllocator{store:store,tree:tree,locks:locks,data:data,dynamic:true},nil
}

// AllocateZeroPublishTxn performs the complete allocation protocol:
// reserve bitmap range -> reserve immutable idempotency page -> zero data and
// flush the data LUN -> atomically publish bitmap, Mapping, idempotency index,
// record page, lifecycle state and Volume root in one AG WAL transaction.
//
// On an ambiguous WAL error the metadata reservation is intentionally kept
// pinned. The caller must fence/stop this Volume, recover its AG WAL, and reopen
// the persistent tree before accepting another allocation.
func (a *PoolAllocator) AllocateZeroPublishTxn(ctx context.Context, ownerLease lock.Lease, req AllocateRequest) (AllocateResult, error) {
	var result AllocateResult
	if a == nil || a.store == nil || a.tree == nil || a.locks == nil || a.data == nil {
		return result, ErrPoolAllocatorUnavailable
	}
	if ctx == nil || ownerLease == nil || !ownerLease.Valid() {
		return result, errors.New("live Volume Owner lease and context are required")
	}
	if req.IdempotencyKey == ([16]byte{}) || req.Fingerprint == ([32]byte{}) ||
		req.BlockCount == 0 || req.Operation == 0 {
		return result, ErrIdempotencyRecord
	}
	if req.LogicalStart >= uint64(1)<<63 ||
		uint64(req.BlockCount) > (uint64(1)<<63)-req.LogicalStart {
		return result, errors.New("logical range overlaps the reserved idempotency key namespace")
	}

	var totalBlocks uint64
	var volumeID string
	var ownerEpoch,generation,mappingSequence uint64
	var ag uint32
	var dynamicSB format.DynamicSuperblock
	var err error
	if a.dynamic {
		dynamicSB,err=a.store.DynamicPoolSuperblock();if err!=nil{return result,err}
		desc,e:=a.store.DynamicPoolDescriptor();if e!=nil{return result,e}
		totalBlocks=dynamicSB.Geometry.TotalBlocks;volumeID=desc.VolumeID;ownerEpoch=desc.OwnerEpoch
		generation=desc.Generation;mappingSequence=desc.MappingSequence;ag=desc.AG
	} else {
		pool,e:=a.store.PoolSuperblock();if e!=nil{return result,e}
		desc,e:=a.store.PoolDescriptor();if e!=nil{return result,e}
		totalBlocks=pool.TotalBlocks;volumeID=desc.VolumeID;ownerEpoch=desc.OwnerEpoch
		generation=desc.Generation;mappingSequence=desc.MappingSequence;ag=desc.AG
	}
	if ownerLease.Resource()!="volume:"+volumeID||!ownerLease.Valid(){return result,errors.New("Owner lease does not match Volume Descriptor")}
	if ownerEpoch==0{return result,errors.New("Volume Descriptor has no active OwnerEpoch")}
	epochLease,ok:=ownerLease.(interface{Epoch()uint64})
	if !ok||epochLease.Epoch()!=ownerEpoch{return result,errors.New("Owner lease epoch does not match the durable Volume Descriptor")}
	if req.LogicalStart+uint64(req.BlockCount)>totalBlocks{return result,errors.New("logical allocation range exceeds Pool capacity")}
	if generation==math.MaxUint64||mappingSequence==math.MaxUint64{return result,errors.New("Volume metadata generation exhausted")}
	unlock, err := a.locks.Lock(ctx, fmt.Sprintf("ag:%03d", ag))
	if err != nil { return result, fmt.Errorf("acquire AG %d lock: %w", ag, err) }
	defer unlock()
	if err = ctx.Err(); err != nil { return result, err }
	if !ownerLease.Valid() { return result, errors.New("Volume Owner lease lost before allocation") }
	// Re-read the durable descriptor after obtaining the AG lock. This closes
	// the race with a concurrent OwnerEpoch takeover or root publication while
	// this request was waiting for the lock.
	if a.dynamic {
		current,e:=a.store.DynamicPoolDescriptor();if e!=nil{return result,e}
		if current.VolumeID!=volumeID||current.AG!=ag||current.OwnerEpoch!=ownerEpoch{return result,errors.New("BMPOOL03 Volume Descriptor changed while waiting for AG lock")}
		generation,mappingSequence=current.Generation,current.MappingSequence
	} else {
		current,e:=a.store.PoolDescriptor();if e!=nil{return result,e}
		if current.VolumeID!=volumeID||current.AG!=ag||current.OwnerEpoch!=ownerEpoch{return result,errors.New("BMPOOL02 Volume Descriptor changed while waiting for AG lock")}
		generation,mappingSequence=current.Generation,current.MappingSequence
	}
	if !ownerLease.Valid(){return result,errors.New("Volume Owner lease lost after descriptor refresh")}


	// Idempotency lookup is serialized by the same AG lock as the eventual
	// index insertion, including hash-collision chain extension.
	hash := IdempotencyHash(req.IdempotencyKey)
	head, lookupErr := a.tree.Get(hash)
	if lookupErr == nil {
		record, found, err := LookupIdempotencyRecord(a.store, head, req.IdempotencyKey, req.Fingerprint)
		if err != nil { return result, err }
		if found {
			return AllocateResult{LogicalStart:record.LogicalStart,PhysicalStart:record.PhysicalStart,
				BlockCount:record.BlockCount,OwnerEpoch:record.OwnerEpoch,Replayed:true}, nil
		}
	} else if !errors.Is(lookupErr, btree.ErrKeyNotFound) {
		return result, fmt.Errorf("lookup idempotency index: %w", lookupErr)
	} else {
		head = 0
	}

	// Capture the currently published physical mapping. Mapped blocks are
	// copied to the newly allocated extent before the root switch; unmapped
	// blocks remain zero-filled. The original physical blocks stay allocated
	// until the Reader-generation-aware data-retire collector reclaims them.
	oldPhysical := make([]uint64, req.BlockCount)
	oldMapped := make([]bool, req.BlockCount)
	for i:=uint64(0); i<uint64(req.BlockCount); i++ {
		value, lookupErr := a.tree.Get(req.LogicalStart+i)
		if lookupErr == nil {
			oldPhysical[i], oldMapped[i] = value, true
		} else if !errors.Is(lookupErr, btree.ErrKeyNotFound) {
			return result, fmt.Errorf("lookup logical block %d: %w", req.LogicalStart+i, lookupErr)
		}
	}

	var physical uint64
	var bitmapImages []metastore.PageImage
	if a.dynamic { physical,bitmapImages,err=PlanDynamicBitmapAllocation(a.store,dynamicSB,ag,req.BlockCount) } else {
		pool,_:=a.store.PoolSuperblock();physical,bitmapImages,err=PlanBitmapAllocation(a.store,pool,ag,req.BlockCount)
	}
	if err != nil { return result, fmt.Errorf("plan AG bitmap allocation: %w", err) }
	reservation, ids, err := a.tree.ReserveExternalPages(1)
	if err != nil { return result, fmt.Errorf("reserve idempotency record page: %w", err) }
	if len(ids) != 1 {
		_ = a.tree.AbortReservedArenaPages(reservation)
		return result, metastore.ErrInvalidPageSet
	}
	abortReservation := func(cause error) (AllocateResult,error) {
		if abortErr := a.tree.AbortReservedArenaPages(reservation); abortErr != nil {
			return result, errors.Join(cause, fmt.Errorf("abort unused idempotency reservation: %w", abortErr))
		}
		return result, cause
	}

	// BMPOOL02 adds DataStartSector through poolDataView. BMPOOL03 receives a
	// DataView whose offset zero is already the first logical Data block.
	var view DataDevice
	if a.dynamic { view=a.data } else {
		pool,_:=a.store.PoolSuperblock();view,err=newPoolDataView(a.data,pool);if err!=nil{return abortReservation(err)}
	}
	if err = ZeroBlocks(ctx, view, physical, req.BlockCount); err != nil {
		return abortReservation(fmt.Errorf("zero newly allocated data blocks: %w", err))
	}
	for i, mapped := range oldMapped {
		if !mapped { continue }
		if err = copyPhysicalBlock(ctx, view, oldPhysical[i], physical+uint64(i)); err != nil {
			return abortReservation(fmt.Errorf("COW copy logical block %d from physical block %d: %w", req.LogicalStart+uint64(i), oldPhysical[i], err))
		}
	}
	// ZeroBlocks syncs the initial zero image; copied old data must also reach
	// stable storage before the mapping transaction can become visible.
	if err = view.Sync(); err != nil {
		return abortReservation(fmt.Errorf("sync COW destination data: %w", err))
	}
	if !ownerLease.Valid() {
		return abortReservation(errors.New("Volume Owner lease lost after data zeroing and before metadata publication"))
	}
	if err = ctx.Err(); err != nil { return abortReservation(err) }

	record := IdempotencyRecord{
		NextPage:head, Key:req.IdempotencyKey, Fingerprint:req.Fingerprint,
		LogicalStart:req.LogicalStart, PhysicalStart:physical, BlockCount:req.BlockCount,
		Operation:req.Operation, OwnerEpoch:ownerEpoch,
	}
	page, err := BuildIdempotencyRecordPage(ids[0], generation+1, record)
	if err != nil { return abortReservation(err) }
	updates := make([]btree.Item, 0, int(req.BlockCount)+1)
	for i:=uint64(0); i<uint64(req.BlockCount); i++ {
		updates = append(updates, btree.Item{Key:req.LogicalStart+i,Value:physical+i})
	}
	updates = append(updates, btree.Item{Key:hash,Value:ids[0]})
	if err = a.tree.PutBatchWithReservedArenaPages(updates,reservation,[]metastore.Page{page},bitmapImages); err != nil {
		if isKnownPreCommitAllocationError(err) {
			return result, fmt.Errorf("publish allocation transaction: %w", err)
		}
		return result, fmt.Errorf("%w: publish allocation transaction: %v", ErrAllocationOutcomeUnknown, err)
	}
	return AllocateResult{LogicalStart:req.LogicalStart,PhysicalStart:physical,
		BlockCount:req.BlockCount,OwnerEpoch:ownerEpoch}, nil
}

func isKnownPreCommitAllocationError(err error) bool {
	return errors.Is(err,wal.ErrJournalFull) ||
		errors.Is(err,metastore.ErrRetireQueueFull) ||
		errors.Is(err,metastore.ErrMetadataTransactionTooLarge) ||
		errors.Is(err,metastore.ErrInvalidPageSet) ||
		errors.Is(err,metastore.ErrArenaFull) ||
		errors.Is(err,btree.ErrCorruptTree)
}


const cowCopyChunkSize = 1 << 20

// copyPhysicalBlock preserves the full old 4MiB block before the original BIO
// is redirected to the new extent. The DataView is fragmented but presents a
// contiguous logical ReaderAt/WriterAt address space.
func copyPhysicalBlock(ctx context.Context, dev DataDevice, source, destination uint64) error {
	reader, ok := dev.(io.ReaderAt)
	if !ok { return errors.New("COW requires a DataView implementing io.ReaderAt") }
	if source > math.MaxUint64/format.BlockSize || destination > math.MaxUint64/format.BlockSize {
		return errors.New("COW physical block offset overflow")
	}
	srcOff, dstOff := source*format.BlockSize, destination*format.BlockSize
	if dev.Size() < 0 || srcOff > uint64(dev.Size()) || format.BlockSize > uint64(dev.Size())-srcOff ||
		dstOff > uint64(dev.Size()) || format.BlockSize > uint64(dev.Size())-dstOff {
		return errors.New("COW source or destination block exceeds DataView")
	}
	buf := make([]byte, cowCopyChunkSize)
	for done := uint64(0); done < format.BlockSize; {
		if err := ctx.Err(); err != nil { return err }
		n := uint64(len(buf)); if format.BlockSize-done < n { n = format.BlockSize-done }
		read, err := reader.ReadAt(buf[:int(n)], int64(srcOff+done))
		if err != nil && !(errors.Is(err, io.EOF) && read == int(n)) { return err }
		if read != int(n) { return io.ErrUnexpectedEOF }
		written := 0
		for written < int(n) {
			m, writeErr := dev.WriteAt(buf[written:int(n)], int64(dstOff+done)+int64(written))
			written += m
			if writeErr != nil { return writeErr }
			if m == 0 { return io.ErrShortWrite }
		}
		done += n
	}
	return nil
}

// poolDataView translates Pool-relative byte offsets into the backing data
// device's DataStartSector-relative range.
type poolDataView struct {
	dev DataDevice
	base uint64
	size int64
}

func newPoolDataView(dev DataDevice, pool format.Superblock) (*poolDataView,error) {
	if dev==nil || pool.DataStartSector > math.MaxUint64/512 { return nil,errors.New("invalid Pool data device geometry") }
	base:=pool.DataStartSector*512
	if dev.Size()<0 || base>uint64(dev.Size()) || pool.PoolSize>uint64(dev.Size())-base {
		return nil,fmt.Errorf("Pool data range [%d,%d) exceeds data device size %d",base,base+pool.PoolSize,dev.Size())
	}
	return &poolDataView{dev:dev,base:base,size:int64(pool.PoolSize)},nil
}
func (v *poolDataView) WriteAt(p []byte, off int64)(int,error) {
	if off<0 || uint64(off)>uint64(v.size) || uint64(len(p))>uint64(v.size)-uint64(off) {
		return 0,errors.New("Pool-relative data write outside Pool")
	}
	if v.base>math.MaxUint64-uint64(off) || v.base+uint64(off)>math.MaxInt64 {
		return 0,errors.New("Pool data offset overflows int64")
	}
	return v.dev.WriteAt(p,int64(v.base+uint64(off)))
}
func (v *poolDataView) Sync() error { return v.dev.Sync() }
func (v *poolDataView) Size() int64 { return v.size }

