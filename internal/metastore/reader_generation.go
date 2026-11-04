package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"time"

	"blockmesh/internal/fence"
)

const (
	readerPageMagic = "BMREAD01"
	readerPageVersion = uint32(2)
	readerHeaderSize = 48
	readerIdentitySize = 128
)

type ReaderState uint32

const (
	ReaderEmpty ReaderState = iota
	ReaderActive
	ReaderLeaving
	ReaderFenced
	ReaderClosed
)

var (
	ErrReaderSlot = errors.New("reader slot is invalid or unavailable")
	ErrReaderIdentity = errors.New("reader identity or epoch mismatch")
	ErrReaderGeneration = errors.New("reader generation cannot move backwards")
	ErrReaderNotFenced = errors.New("reader has not been fenced")
	ErrReaderCoordinatorRequired = errors.New("distributed reader-set coordinator is required")
)

// ReaderRecord is the durable state of one reader instance. BootID must be
// unique per process/host boot; NodeID alone is not sufficient to distinguish
// a restarted node from its stale predecessor. A new reader must register at
// the current committed root before it begins reading; later heartbeats may
// advance its generation monotonically.
type ReaderRecord struct {
	Slot uint32
	NodeID string
	BootID string
	Epoch uint64
	RootGeneration uint64
	HeartbeatSequence uint64
	HeartbeatUnixNano int64
	State ReaderState
}

// ReaderTable stores one reader slot per dedicated 4 KiB metadata page.
// The slot range must be reserved by the caller and must not overlap the
// Arena, lifecycle pages, WAL, checkpoint area, or any other metadata object.
// Slot assignment/ownership must be serialized by the cluster lock service.
// ReaderCoordinator must provide a cluster-wide exclusive barrier shared by
// reader registration, root publication, and retire collection. It must be
// backed by the volume lease/lock service, not a process-local mutex.
type ReaderCoordinator interface {
	LockReaderSet(volumeID string) (unlock func(), err error)
}

func acquireReaderSet(c ReaderCoordinator, volumeID string) (func(),error) {
	if c==nil { return nil,ErrReaderCoordinatorRequired }
	unlock,err:=c.LockReaderSet(volumeID)
	if err!=nil { return nil,err }
	if unlock==nil { return nil,errors.New("reader coordinator returned a nil unlock function") }
	return unlock,nil
}

type ReaderTable struct {
	mu sync.Mutex
	store *Store
	startPage uint64
	slots uint32
	committer Committer
	coordinator ReaderCoordinator
}

func NewReaderTable(store *Store, startPage uint64, slots uint32, committer Committer, coordinators ...ReaderCoordinator) (*ReaderTable,error) {
	if len(coordinators)>1 { return nil,errors.New("at most one reader coordinator may be supplied") }
	if store==nil || committer==nil || slots==0 { return nil,errors.New("store, nonzero reader slots and WAL committer are required") }
	sb:=store.Superblock()
	count:=uint64(slots)
	if startPage<2 || startPage>sb.TotalPages || count>sb.TotalPages-startPage { return nil,ErrReaderSlot }
	end:=startPage+count
	arenaEnd:=sb.ArenaStart+sb.ArenaPages
	if startPage<arenaEnd && end>sb.ArenaStart { return nil,ErrReaderSlot }
	if sb.LifecyclePages>0 {
		lifeEnd:=sb.LifecycleStart+sb.LifecyclePages
		if startPage<lifeEnd && end>sb.LifecycleStart { return nil,ErrReaderSlot }
	}
	var coordinator ReaderCoordinator
	if len(coordinators)==1 { coordinator=coordinators[0] }
	if coordinator!=nil {
		clusterCommitter,ok:=committer.(ClusterSafeCommitter)
		if !ok || !clusterCommitter.ClusterSafe() { return nil,ErrClusterSafeCommitterRequired }
	}
	return &ReaderTable{store:store,startPage:startPage,slots:slots,committer:committer,coordinator:coordinator},nil
}

func (t *ReaderTable) lockReaderSet() (func(), error) {
	if t.coordinator == nil {
		return func() {}, nil // single-node mode only
	}
	return acquireReaderSet(t.coordinator, t.store.Superblock().VolumeID)
}

func (t *ReaderTable) readLocked(slot uint32) (ReaderRecord,error) {
	if slot>=t.slots { return ReaderRecord{},ErrReaderSlot }
	b:=make([]byte,PageSize)
	off:=(t.startPage+uint64(slot))*PageSize
	if err:=readFullAt(t.store.dev,b,int64(off));err!=nil{return ReaderRecord{},err}
	if allZeroLifecycle(b) { return ReaderRecord{Slot:slot,State:ReaderEmpty},nil }
	return decodeReaderRecord(slot,b)
}

func (t *ReaderTable) writeLocked(r ReaderRecord) error {
	b,err:=encodeReaderRecord(r);if err!=nil{return err}
	off:=(t.startPage+uint64(r.Slot))*PageSize
	return t.committer.CommitImages([]PageImage{{Offset:off,Data:b}})
}

func (t *ReaderTable) Register(slot uint32,nodeID,bootID string,epoch,rootGeneration uint64) error {
	if t.coordinator!=nil {
		unlock,err:=acquireReaderSet(t.coordinator,t.store.Superblock().VolumeID);if err!=nil{return err};defer unlock()
	}
	t.mu.Lock();defer t.mu.Unlock()
	if err:=t.store.Refresh();err!=nil{return err}
	if slot>=t.slots || nodeID=="" || bootID=="" || len(nodeID)>readerIdentitySize || len(bootID)>readerIdentitySize || epoch==0 { return ErrReaderSlot }
	if rootGeneration!=t.store.Superblock().MappingSequence { return ErrReaderGeneration }
	old,err:=t.readLocked(slot);if err!=nil{return err}
	if old.State!=ReaderEmpty && old.State!=ReaderFenced && old.State!=ReaderClosed { return ErrReaderSlot }
	if old.Epoch>=epoch { return ErrReaderIdentity }
	return t.writeLocked(ReaderRecord{Slot:slot,NodeID:nodeID,BootID:bootID,Epoch:epoch,RootGeneration:rootGeneration,HeartbeatSequence:1,HeartbeatUnixNano:time.Now().UnixNano(),State:ReaderActive})
}

func (t *ReaderTable) Heartbeat(slot uint32,nodeID,bootID string,epoch,rootGeneration uint64) error {
	unlock, lockErr := t.lockReaderSet(); if lockErr != nil { return lockErr }; defer unlock()
	t.mu.Lock();defer t.mu.Unlock()
	if err:=t.store.Refresh();err!=nil{return err}
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if r.State!=ReaderActive || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	if rootGeneration!=r.RootGeneration { return ErrReaderGeneration }
	if r.HeartbeatSequence==math.MaxUint64 { return errors.New("reader heartbeat sequence exhausted") }
	r.HeartbeatSequence++
	r.HeartbeatUnixNano=time.Now().UnixNano()
	return t.writeLocked(r)
}

// AdvanceGeneration is called only after the reader has stopped issuing I/O
// against its old root, drained every in-flight request that may reference
// that generation, and refreshed its B+Tree to the new committed root.
func (t *ReaderTable) AdvanceGeneration(slot uint32,nodeID,bootID string,epoch,newRootGeneration uint64,oldGenerationIOsDrained bool) error {
	unlock, lockErr := t.lockReaderSet(); if lockErr != nil { return lockErr }; defer unlock()
	t.mu.Lock();defer t.mu.Unlock()
	if err:=t.store.Refresh();err!=nil{return err}
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if r.State!=ReaderActive || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	if !oldGenerationIOsDrained || newRootGeneration<=r.RootGeneration ||
		newRootGeneration>t.store.Superblock().MappingSequence { return ErrReaderGeneration }
	if r.HeartbeatSequence==math.MaxUint64 { return errors.New("reader heartbeat sequence exhausted") }
	r.RootGeneration=newRootGeneration;r.HeartbeatSequence++;r.HeartbeatUnixNano=time.Now().UnixNano()
	return t.writeLocked(r)
}

func (t *ReaderTable) BeginLeave(slot uint32,nodeID,bootID string,epoch uint64) error {
	unlock, lockErr := t.lockReaderSet(); if lockErr != nil { return lockErr }; defer unlock()
	t.mu.Lock();defer t.mu.Unlock()
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if r.State!=ReaderActive || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	r.State=ReaderLeaving
	return t.writeLocked(r)
}

// CloseDrained is the graceful path: the caller must stop new reads and drain
// every in-flight I/O before marking the slot reusable.
func (t *ReaderTable) CloseDrained(slot uint32,nodeID,bootID string,epoch uint64,ioDrained bool) error {
	if !ioDrained { return ErrUnsafeReclaim }
	unlock, lockErr := t.lockReaderSet(); if lockErr != nil { return lockErr }; defer unlock()
	t.mu.Lock();defer t.mu.Unlock()
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if (r.State!=ReaderActive && r.State!=ReaderLeaving) || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	r.State=ReaderClosed
	return t.writeLocked(r)
}

// FenceAndClose first asks the configured SAN fencer to revoke every path for
// this exact boot instance. The durable Fenced state is written only after
// fencing succeeds; a timeout or heartbeat loss never changes the slot.
func (t *ReaderTable) FenceAndClose(ctx context.Context,slot uint32,nodeID,bootID string,epoch uint64,fencer fence.Fencer) error {
	if fencer==nil { return ErrReaderNotFenced }
	if t.coordinator == nil { return ErrReaderCoordinatorRequired }
	unlock, lockErr := t.lockReaderSet(); if lockErr != nil { return lockErr }; defer unlock()
	t.mu.Lock()
	r,err:=t.readLocked(slot)
	if err!=nil { t.mu.Unlock();return err }
	if (r.State!=ReaderActive && r.State!=ReaderLeaving) || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch {
		t.mu.Unlock();return ErrReaderIdentity
	}
	t.mu.Unlock()
	if err=fencer.FenceNode(ctx,nodeID,bootID);err!=nil{return fmt.Errorf("fence reader %s/%s: %w",nodeID,bootID,err)}
	t.mu.Lock();defer t.mu.Unlock()
	current,err:=t.readLocked(slot);if err!=nil{return err}
	if current.NodeID!=nodeID || current.BootID!=bootID || current.Epoch!=epoch || (current.State!=ReaderActive && current.State!=ReaderLeaving) { return ErrReaderIdentity }
	current.State=ReaderFenced
	return t.writeLocked(current)
}

func (t *ReaderTable) Snapshot() ([]ReaderRecord,error) {
	t.mu.Lock();defer t.mu.Unlock()
	out:=make([]ReaderRecord,0,t.slots)
	for i:=uint32(0);i<t.slots;i++ {
		r,err:=t.readLocked(i);if err!=nil{return nil,err}
		if r.State!=ReaderEmpty && r.State!=ReaderFenced && r.State!=ReaderClosed { out=append(out,r) }
	}
	return out,nil
}

// SafeGeneration is the minimum root generation held by any Active or
// Leaving reader. Gracefully Closed and SAN Fenced slots no longer pin pages.
func (t *ReaderTable) SafeGeneration() (uint64,error) {
	readers,err:=t.Snapshot();if err!=nil{return 0,err}
	safe:=uint64(math.MaxUint64)
	for _,r:=range readers { if r.RootGeneration<safe {safe=r.RootGeneration} }
	return safe,nil
}

func encodeReaderRecord(r ReaderRecord) ([]byte,error) {
	if r.NodeID=="" || r.BootID=="" || len(r.NodeID)>readerIdentitySize || len(r.BootID)>readerIdentitySize ||
		r.Epoch==0 || r.HeartbeatUnixNano<=0 || (r.State!=ReaderActive && r.State!=ReaderLeaving && r.State!=ReaderFenced && r.State!=ReaderClosed) { return nil,ErrReaderSlot }
	b:=make([]byte,PageSize);copy(b[:8],readerPageMagic)
	binary.LittleEndian.PutUint32(b[8:12],readerPageVersion)
	binary.LittleEndian.PutUint32(b[12:16],uint32(r.State))
	binary.LittleEndian.PutUint64(b[16:24],r.Epoch)
	binary.LittleEndian.PutUint64(b[24:32],r.RootGeneration)
	binary.LittleEndian.PutUint64(b[32:40],r.HeartbeatSequence)
	binary.LittleEndian.PutUint64(b[304:312],uint64(r.HeartbeatUnixNano))
	binary.LittleEndian.PutUint16(b[40:42],uint16(len(r.NodeID)))
	binary.LittleEndian.PutUint16(b[42:44],uint16(len(r.BootID)))
	copy(b[readerHeaderSize:readerHeaderSize+readerIdentitySize],r.NodeID)
	copy(b[readerHeaderSize+readerIdentitySize:readerHeaderSize+2*readerIdentitySize],r.BootID)
	binary.LittleEndian.PutUint32(b[PageSize-4:],crc32.Checksum(b[:PageSize-4],crcTable))
	return b,nil
}

func decodeReaderRecord(slot uint32,b []byte) (ReaderRecord,error) {
	version:=binary.LittleEndian.Uint32(b[8:12])
	if len(b)!=PageSize || string(b[:8])!=readerPageMagic || (version!=1 && version!=readerPageVersion) ||
		crc32.Checksum(b[:PageSize-4],crcTable)!=binary.LittleEndian.Uint32(b[PageSize-4:]) { return ReaderRecord{},ErrLifecycleCorrupt }
	state:=ReaderState(binary.LittleEndian.Uint32(b[12:16]))
	nl,bl:=int(binary.LittleEndian.Uint16(b[40:42])),int(binary.LittleEndian.Uint16(b[42:44]))
	if nl==0 || bl==0 || nl>readerIdentitySize || bl>readerIdentitySize ||
		(state!=ReaderActive && state!=ReaderLeaving && state!=ReaderFenced && state!=ReaderClosed) { return ReaderRecord{},ErrLifecycleCorrupt }
	for _,v:=range b[44:readerHeaderSize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	for _,v:=range b[readerHeaderSize+nl:readerHeaderSize+readerIdentitySize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	for _,v:=range b[readerHeaderSize+readerIdentitySize+bl:readerHeaderSize+2*readerIdentitySize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	if version==1 {
		for _,v:=range b[readerHeaderSize+2*readerIdentitySize:PageSize-4] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	} else {
		for _,v:=range b[312:PageSize-4] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	}
	r:=ReaderRecord{Slot:slot,State:state,Epoch:binary.LittleEndian.Uint64(b[16:24]),RootGeneration:binary.LittleEndian.Uint64(b[24:32]),
		HeartbeatSequence:binary.LittleEndian.Uint64(b[32:40]),HeartbeatUnixNano:int64(binary.LittleEndian.Uint64(b[304:312])),NodeID:string(b[readerHeaderSize:readerHeaderSize+nl]),
		BootID:string(b[readerHeaderSize+readerIdentitySize:readerHeaderSize+readerIdentitySize+bl])}
	if r.Epoch==0 || r.HeartbeatSequence==0 {return ReaderRecord{},ErrLifecycleCorrupt}
	if version==1 { r.HeartbeatUnixNano=0 } else if r.HeartbeatUnixNano<=0 { return ReaderRecord{},ErrLifecycleCorrupt }
	return r,nil
}
