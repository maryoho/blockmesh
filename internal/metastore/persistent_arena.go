package metastore

import (
 "encoding/binary"
 "errors"
 "fmt"
 "hash/crc32"
 "math"
 "sync"
)

const (
 lifecycleStateMagic = "BMARNA01"
 lifecycleQueueMagic = "BMRETR01"
 lifecycleVersion = uint32(1)
 lifecycleHeaderSize = 32
 lifecycleRecordSize = 16
 MaxPersistentRetiredPages = (PageSize-4-lifecycleHeaderSize)/lifecycleRecordSize
)

var (
 ErrLifecycleUnformatted = errors.New("persistent arena lifecycle region is not initialized")
 ErrLifecycleAlreadyInitialized = errors.New("persistent arena lifecycle region is already initialized")
 ErrRetireQueueFull = errors.New("persistent retire queue is full")
 ErrLifecycleCorrupt = errors.New("persistent arena lifecycle metadata is corrupt")
)

// PersistentArena combines a volatile reservation allocator with durable
// lifecycle checkpoints. Live pages are reconstructed from the published
// root graph plus every page in the durable retire queue. In-flight
// reservations are intentionally volatile: after a crash, uncommitted COW
// pages are unreachable and therefore safe to reuse.
type PersistentArena struct {
 mu sync.Mutex
 store *Store
 arena *Arena
 committer Committer
 stateGeneration uint64
 rootSequence uint64
 batches []RetireBatch
}

func lifecycleOffsets(sb Superblock)(uint64,uint64,error){
 if sb.LifecyclePages<2 { return 0,0,ErrLifecycleUnformatted }
 return sb.LifecycleStart*PageSize,(sb.LifecycleStart+1)*PageSize,nil
}

// FormatLifecycle initializes the two reserved lifecycle pages. It must be
// called only while the metadata device is exclusively owned and before
// opening the persistent Arena.
func FormatLifecycle(store *Store) error {
 if store==nil { return errors.New("nil metastore") }
 stateOff,queueOff,err:=lifecycleOffsets(store.Superblock()); if err!=nil{return err}
 existingState:=make([]byte,PageSize);if err=readFullAt(store.dev,existingState,int64(stateOff));err!=nil{return err}
 existingQueue:=make([]byte,PageSize);if err=readFullAt(store.dev,existingQueue,int64(queueOff));err!=nil{return err}
 if !allZeroLifecycle(existingState)||!allZeroLifecycle(existingQueue){return ErrLifecycleAlreadyInitialized}
 state:=encodeLifecycleState(1,store.Superblock().MappingSequence)
 queue,err:=encodeRetireQueue(nil,1);if err!=nil{return err}
 if err=writeFullAt(store.dev,state,int64(stateOff));err!=nil{return err}
 if err=writeFullAt(store.dev,queue,int64(queueOff));err!=nil{return err}
 return store.Sync()
}

func allZeroLifecycle(b []byte)bool{for _,v:=range b{if v!=0{return false}};return true}

func encodeLifecycleState(generation,rootSequence uint64)[]byte{
 b:=make([]byte,PageSize);copy(b[:8],lifecycleStateMagic)
 binary.LittleEndian.PutUint32(b[8:12],lifecycleVersion)
 binary.LittleEndian.PutUint64(b[16:24],generation)
 binary.LittleEndian.PutUint64(b[24:32],rootSequence)
 binary.LittleEndian.PutUint32(b[PageSize-4:],crc32.Checksum(b[:PageSize-4],crcTable))
 return b
}

func decodeLifecycleState(b []byte)(uint64,uint64,error){
 if len(b)!=PageSize||string(b[:8])!=lifecycleStateMagic||binary.LittleEndian.Uint32(b[8:12])!=lifecycleVersion||
  crc32.Checksum(b[:PageSize-4],crcTable)!=binary.LittleEndian.Uint32(b[PageSize-4:]) {return 0,0,ErrLifecycleCorrupt}
 for _,v:=range b[12:16]{if v!=0{return 0,0,ErrLifecycleCorrupt}}
 for _,v:=range b[32:PageSize-4]{if v!=0{return 0,0,ErrLifecycleCorrupt}}
 generation:=binary.LittleEndian.Uint64(b[16:24])
 if generation==0{return 0,0,ErrLifecycleCorrupt}
 return generation,binary.LittleEndian.Uint64(b[24:32]),nil
}

func encodeRetireQueue(batches []RetireBatch,generation uint64)([]byte,error){
 count:=0
 for _,batch:=range batches { if len(batch.PageIDs)==0 {continue}; if len(batch.PageIDs)>MaxPersistentRetiredPages-count{return nil,ErrRetireQueueFull};count+=len(batch.PageIDs) }
 b:=make([]byte,PageSize);copy(b[:8],lifecycleQueueMagic);binary.LittleEndian.PutUint32(b[8:12],lifecycleVersion);binary.LittleEndian.PutUint32(b[12:16],uint32(count))
 binary.LittleEndian.PutUint64(b[16:24],generation)
 off:=lifecycleHeaderSize
 for _,batch:=range batches { for _,id:=range batch.PageIDs {
  binary.LittleEndian.PutUint64(b[off:off+8],batch.RetireGeneration)
  binary.LittleEndian.PutUint64(b[off+8:off+16],id);off+=lifecycleRecordSize
 }}
 binary.LittleEndian.PutUint32(b[PageSize-4:],crc32.Checksum(b[:PageSize-4],crcTable))
 return b,nil
}

func decodeRetireQueue(b []byte,store *Store)([]RetireBatch,uint64,error){
 if len(b)!=PageSize||string(b[:8])!=lifecycleQueueMagic||binary.LittleEndian.Uint32(b[8:12])!=lifecycleVersion||
  crc32.Checksum(b[:PageSize-4],crcTable)!=binary.LittleEndian.Uint32(b[PageSize-4:]) {return nil,0,ErrLifecycleCorrupt}
 count:=int(binary.LittleEndian.Uint32(b[12:16]));if count>MaxPersistentRetiredPages{return nil,0,ErrLifecycleCorrupt}
 for _,v:=range b[24:lifecycleHeaderSize]{if v!=0{return nil,0,ErrLifecycleCorrupt}}
 queueGeneration:=binary.LittleEndian.Uint64(b[16:24])
 if queueGeneration==0{return nil,0,ErrLifecycleCorrupt}
 batches:=make([]RetireBatch,0);index:=make(map[uint64]int);seen:=make(map[uint64]struct{},count)
 off:=lifecycleHeaderSize;sb:=store.Superblock()
 for i:=0;i<count;i++ {
  generation:=binary.LittleEndian.Uint64(b[off:off+8]);id:=binary.LittleEndian.Uint64(b[off+8:off+16]);off+=lifecycleRecordSize
  if id<sb.ArenaStart||id-sb.ArenaStart>=sb.ArenaPages||generation>sb.MappingSequence {return nil,0,ErrLifecycleCorrupt}
  if _,ok:=seen[id];ok{return nil,0,ErrLifecycleCorrupt};seen[id]=struct{}{}
  batchIndex,ok:=index[generation];if !ok {batchIndex=len(batches);index[generation]=batchIndex;batches=append(batches,RetireBatch{RetireGeneration:generation})}
  batches[batchIndex].PageIDs=append(batches[batchIndex].PageIDs,id)
 }
 for _,v:=range b[off:PageSize-4]{if v!=0{return nil,0,ErrLifecycleCorrupt}}
 return batches,queueGeneration,nil
}

// NewPersistentArena restores live pages from the root graph and durable
// retire records. Every retired page remains pinned until Collect proves the
// reader generation, I/O drain and fencing conditions.
func NewPersistentArena(store *Store,livePages []uint64,committer Committer)(*PersistentArena,error){
 if store==nil||committer==nil{return nil,errors.New("store and WAL committer are required")}
 stateOff,queueOff,err:=lifecycleOffsets(store.Superblock());if err!=nil{return nil,err}
 stateImage:=make([]byte,PageSize);if err=readFullAt(store.dev,stateImage,int64(stateOff));err!=nil{return nil,err}
 queueImage:=make([]byte,PageSize);if err=readFullAt(store.dev,queueImage,int64(queueOff));err!=nil{return nil,err}
 generation,rootSequence,err:=decodeLifecycleState(stateImage);if err!=nil{return nil,err}
 batches,queueGeneration,err:=decodeRetireQueue(queueImage,store);if err!=nil{return nil,err}
 if rootSequence!=store.Superblock().MappingSequence||queueGeneration!=generation{return nil,ErrLifecycleCorrupt}
 live:=append([]uint64(nil),livePages...)
 for _,batch:=range batches {live=append(live,batch.PageIDs...)}
 arena,err:=NewArena(store,live);if err!=nil{return nil,err}
 return &PersistentArena{store:store,arena:arena,committer:committer,stateGeneration:generation,rootSequence:rootSequence,batches:batches},nil
}

func (p *PersistentArena) Arena()*Arena {if p==nil{return nil};return p.arena}
func (p *PersistentArena) Reserve(count uint64)(*Reservation,error){if p==nil{return nil,errors.New("nil persistent arena")};return p.arena.Reserve(count)}
func (p *PersistentArena) Stats()ArenaStats{return p.arena.Stats()}
func (p *PersistentArena) Pending()[]RetireBatch{
 p.mu.Lock();defer p.mu.Unlock();out:=make([]RetireBatch,len(p.batches))
 for i,b:=range p.batches{out[i]=RetireBatch{RetireGeneration:b.RetireGeneration,PageIDs:append([]uint64(nil),b.PageIDs...)}}
 return out
}

// CommitPagesAndPublish persists new pages, lifecycle state, retire records,
// and the new root descriptor in one WAL transaction. A non-ErrJournalFull
// error may have an uncertain commit outcome; callers must stop using this
// instance and reopen/recover rather than aborting the reservation.
func (p *PersistentArena) CommitPagesAndPublish(reservation *Reservation,pages []Page,rootPage,mappingSequence,retireGeneration uint64,retired []uint64)error{
 if p==nil{return errors.New("nil persistent arena")}
 p.mu.Lock();defer p.mu.Unlock()
 if mappingSequence<=p.rootSequence||retireGeneration>=mappingSequence{return errors.New("invalid lifecycle generation")}
 if p.store.Superblock().MappingSequence!=p.rootSequence{return ErrLifecycleCorrupt}
 if p.stateGeneration==math.MaxUint64{return errors.New("lifecycle generation exhausted")}
 // Validate the volatile reservation against the exact COW page set before
 // recording any durable after-images.
 p.arena.mu.Lock()
 if reservation==nil {
  if len(pages)!=0 {p.arena.mu.Unlock();return errors.New("COW pages require an Arena reservation")}
 } else {
  if reservation.arena!=p.arena||reservation.done||len(reservation.pages)!=len(pages) {p.arena.mu.Unlock();return ErrInvalidPageSet}
  reservedIDs:=make(map[uint64]struct{},len(reservation.pages))
  for _,id:=range reservation.pages {owner,ok:=p.arena.reserved[id];if !ok||owner!=reservation.id {p.arena.mu.Unlock();return ErrInvalidPageSet};reservedIDs[id]=struct{}{}}
  for _,page:=range pages {if _,ok:=reservedIDs[page.ID];!ok {p.arena.mu.Unlock();return ErrInvalidPageSet};delete(reservedIDs,page.ID)}
  if len(reservedIDs)!=0 {p.arena.mu.Unlock();return ErrInvalidPageSet}
 }
 retiredSeen:=make(map[uint64]struct{},len(retired))
 currentRoot:=p.store.Superblock().RootPage
 for _,id:=range retired {
  if id< p.arena.start||id-p.arena.start>=p.arena.pages||id==currentRoot {p.arena.mu.Unlock();return ErrInvalidPageSet}
  if _,ok:=retiredSeen[id];ok {p.arena.mu.Unlock();return ErrInvalidPageSet};retiredSeen[id]=struct{}{}
  if _,ok:=p.arena.live[id];!ok {p.arena.mu.Unlock();return ErrInvalidPageSet}
  if _,ok:=p.arena.reserved[id];ok {p.arena.mu.Unlock();return ErrInvalidPageSet}
 }
 p.arena.mu.Unlock()
 nextBatches:=make([]RetireBatch,len(p.batches));for i,b:=range p.batches{nextBatches[i]=RetireBatch{RetireGeneration:b.RetireGeneration,PageIDs:append([]uint64(nil),b.PageIDs...)}}
 if len(retired)>0 {nextBatches=append(nextBatches,RetireBatch{RetireGeneration:retireGeneration,PageIDs:append([]uint64(nil),retired...)})}
 queueImage,err:=encodeRetireQueue(nextBatches,p.stateGeneration+1);if err!=nil{return err}
 stateImage:=encodeLifecycleState(p.stateGeneration+1,mappingSequence)
 sb:=p.store.Superblock();stateOff,queueOff,err:=lifecycleOffsets(sb);if err!=nil{return err}
 extras:=[]PageImage{{Offset:stateOff,Data:stateImage},{Offset:queueOff,Data:queueImage}}
 if err=p.store.CommitPagesAndPublishWithImages(pages,extras,rootPage,mappingSequence,p.committer);err!=nil{return err}
 p.stateGeneration++;p.rootSequence=mappingSequence;p.batches=nextBatches
 return nil
}

// Collect durably removes safe retire records before making their pages free.
// A crash after the WAL commit but before the in-memory release is safe:
// reopening reconstructs free space from the root and the now-shorter queue.
func (p *PersistentArena) Collect(proof ReclaimProof)(int,error){
 if p==nil{return 0,errors.New("nil persistent arena")}
 if !proof.IOsDrained||!proof.FencingConfirmed{return 0,ErrUnsafeReclaim}
 p.mu.Lock();defer p.mu.Unlock()
 if p.store.Superblock().MappingSequence!=p.rootSequence{return 0,ErrLifecycleCorrupt}
 kept:=make([]RetireBatch,0,len(p.batches));var release []uint64
 for _,b:=range p.batches {if proof.SafeGeneration>b.RetireGeneration {release=append(release,b.PageIDs...)}else{kept=append(kept,b)}}
 if len(release)==0{return 0,nil}
 if p.stateGeneration==math.MaxUint64{return 0,errors.New("lifecycle generation exhausted")}
 queueImage,err:=encodeRetireQueue(kept,p.stateGeneration+1);if err!=nil{return 0,err}
 stateImage:=encodeLifecycleState(p.stateGeneration+1,p.rootSequence)
 stateOff,queueOff,err:=lifecycleOffsets(p.store.Superblock());if err!=nil{return 0,err}
 if err=p.committer.CommitImages([]PageImage{{Offset:stateOff,Data:stateImage},{Offset:queueOff,Data:queueImage}});err!=nil{return 0,err}
 if err=p.arena.ReleasePages(release);err!=nil{return 0,fmt.Errorf("retire queue committed but in-memory page release failed: %w",err)}
 p.stateGeneration++;p.batches=kept
 return len(release),nil
}

var _ = fmt.Sprintf
