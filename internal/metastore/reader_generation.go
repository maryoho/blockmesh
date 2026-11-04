package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"blockmesh/internal/fence"
)

const (
	readerPageMagic = "BMREAD01"
	readerPageVersion = uint32(1)
	readerHeaderSize = 48
	readerIdentitySize = 128
)

type ReaderState uint32

const (
	ReaderEmpty ReaderState = iota
	ReaderActive
	ReaderLeaving
	ReaderFenced
)

var (
	ErrReaderSlot = errors.New("reader slot is invalid or unavailable")
	ErrReaderIdentity = errors.New("reader identity or epoch mismatch")
	ErrReaderGeneration = errors.New("reader generation cannot move backwards")
	ErrReaderNotFenced = errors.New("reader has not been fenced")
)

// ReaderRecord is the durable state of one reader instance. BootID must be
// unique per process/host boot; NodeID alone is not sufficient to distinguish
// a restarted node from its stale predecessor.
type ReaderRecord struct {
	Slot uint32
	NodeID string
	BootID string
	Epoch uint64
	RootGeneration uint64
	HeartbeatSequence uint64
	State ReaderState
}

// ReaderTable stores one reader slot per dedicated 4 KiB metadata page.
// The slot range must be reserved by the caller and must not overlap the
// Arena, lifecycle pages, WAL, checkpoint area, or any other metadata object.
// Slot assignment/ownership must be serialized by the cluster lock service.
type ReaderTable struct {
	mu sync.Mutex
	store *Store
	startPage uint64
	slots uint32
	committer Committer
}

func NewReaderTable(store *Store, startPage uint64, slots uint32, committer Committer) (*ReaderTable,error) {
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
	return &ReaderTable{store:store,startPage:startPage,slots:slots,committer:committer},nil
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
	t.mu.Lock();defer t.mu.Unlock()
	if slot>=t.slots || nodeID=="" || bootID=="" || len(nodeID)>readerIdentitySize || len(bootID)>readerIdentitySize || epoch==0 { return ErrReaderSlot }
	if rootGeneration>t.store.Superblock().MappingSequence { return ErrReaderGeneration }
	old,err:=t.readLocked(slot);if err!=nil{return err}
	if old.State!=ReaderEmpty && old.State!=ReaderFenced { return ErrReaderSlot }
	if old.Epoch>=epoch { return ErrReaderIdentity }
	return t.writeLocked(ReaderRecord{Slot:slot,NodeID:nodeID,BootID:bootID,Epoch:epoch,RootGeneration:rootGeneration,HeartbeatSequence:1,State:ReaderActive})
}

func (t *ReaderTable) Heartbeat(slot uint32,nodeID,bootID string,epoch,rootGeneration uint64) error {
	t.mu.Lock();defer t.mu.Unlock()
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if r.State!=ReaderActive || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	if rootGeneration<r.RootGeneration || rootGeneration>t.store.Superblock().MappingSequence { return ErrReaderGeneration }
	if r.HeartbeatSequence==math.MaxUint64 { return errors.New("reader heartbeat sequence exhausted") }
	r.RootGeneration=rootGeneration;r.HeartbeatSequence++
	return t.writeLocked(r)
}

func (t *ReaderTable) BeginLeave(slot uint32,nodeID,bootID string,epoch uint64) error {
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
	t.mu.Lock();defer t.mu.Unlock()
	r,err:=t.readLocked(slot);if err!=nil{return err}
	if (r.State!=ReaderActive && r.State!=ReaderLeaving) || r.NodeID!=nodeID || r.BootID!=bootID || r.Epoch!=epoch { return ErrReaderIdentity }
	r.State=ReaderFenced
	return t.writeLocked(r)
}

// FenceAndClose first asks the configured SAN fencer to revoke every path for
// this exact boot instance. The durable Fenced state is written only after
// fencing succeeds; a timeout or heartbeat loss never changes the slot.
func (t *ReaderTable) FenceAndClose(ctx context.Context,slot uint32,nodeID,bootID string,epoch uint64,fencer fence.Fencer) error {
	if fencer==nil { return ErrReaderNotFenced }
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
		if r.State!=ReaderEmpty && r.State!=ReaderFenced { out=append(out,r) }
	}
	return out,nil
}

// SafeGeneration is the minimum root generation held by any non-fenced
// reader. No readers yields MaxUint64. Leaving readers remain pinned until
// their I/O is drained or the fencer confirms revocation.
func (t *ReaderTable) SafeGeneration() (uint64,error) {
	readers,err:=t.Snapshot();if err!=nil{return 0,err}
	safe:=uint64(math.MaxUint64)
	for _,r:=range readers { if r.RootGeneration<safe {safe=r.RootGeneration} }
	return safe,nil
}

func encodeReaderRecord(r ReaderRecord) ([]byte,error) {
	if r.Slot==math.MaxUint32 || r.NodeID=="" || r.BootID=="" || len(r.NodeID)>readerIdentitySize || len(r.BootID)>readerIdentitySize ||
		r.Epoch==0 || (r.State!=ReaderActive && r.State!=ReaderLeaving && r.State!=ReaderFenced) { return nil,ErrReaderSlot }
	b:=make([]byte,PageSize);copy(b[:8],readerPageMagic)
	binary.LittleEndian.PutUint32(b[8:12],readerPageVersion)
	binary.LittleEndian.PutUint32(b[12:16],uint32(r.State))
	binary.LittleEndian.PutUint64(b[16:24],r.Epoch)
	binary.LittleEndian.PutUint64(b[24:32],r.RootGeneration)
	binary.LittleEndian.PutUint64(b[32:40],r.HeartbeatSequence)
	binary.LittleEndian.PutUint16(b[40:42],uint16(len(r.NodeID)))
	binary.LittleEndian.PutUint16(b[42:44],uint16(len(r.BootID)))
	copy(b[readerHeaderSize:readerHeaderSize+readerIdentitySize],r.NodeID)
	copy(b[readerHeaderSize+readerIdentitySize:readerHeaderSize+2*readerIdentitySize],r.BootID)
	binary.LittleEndian.PutUint32(b[PageSize-4:],crc32.Checksum(b[:PageSize-4],crcTable))
	return b,nil
}

func decodeReaderRecord(slot uint32,b []byte) (ReaderRecord,error) {
	if len(b)!=PageSize || string(b[:8])!=readerPageMagic || binary.LittleEndian.Uint32(b[8:12])!=readerPageVersion ||
		crc32.Checksum(b[:PageSize-4],crcTable)!=binary.LittleEndian.Uint32(b[PageSize-4:]) { return ReaderRecord{},ErrLifecycleCorrupt }
	state:=ReaderState(binary.LittleEndian.Uint32(b[12:16]))
	nl,bl:=int(binary.LittleEndian.Uint16(b[40:42])),int(binary.LittleEndian.Uint16(b[42:44]))
	if nl==0 || bl==0 || nl>readerIdentitySize || bl>readerIdentitySize ||
		(state!=ReaderActive && state!=ReaderLeaving && state!=ReaderFenced) { return ReaderRecord{},ErrLifecycleCorrupt }
	for _,v:=range b[44:readerHeaderSize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	for _,v:=range b[readerHeaderSize+nl:readerHeaderSize+readerIdentitySize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	for _,v:=range b[readerHeaderSize+readerIdentitySize+bl:readerHeaderSize+2*readerIdentitySize] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	for _,v:=range b[readerHeaderSize+2*readerIdentitySize:PageSize-4] {if v!=0{return ReaderRecord{},ErrLifecycleCorrupt}}
	r:=ReaderRecord{Slot:slot,State:state,Epoch:binary.LittleEndian.Uint64(b[16:24]),RootGeneration:binary.LittleEndian.Uint64(b[24:32]),
		HeartbeatSequence:binary.LittleEndian.Uint64(b[32:40]),NodeID:string(b[readerHeaderSize:readerHeaderSize+nl]),
		BootID:string(b[readerHeaderSize+readerIdentitySize:readerHeaderSize+readerIdentitySize+bl])}
	if r.Epoch==0 || r.HeartbeatSequence==0 {return ReaderRecord{},ErrLifecycleCorrupt}
	return r,nil
}
