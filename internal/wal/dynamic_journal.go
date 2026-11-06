package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

// DynamicJournal is a BMPOOL03 append-only redo journal. Its segment base and
// capacity are derived from the persisted DynamicGeometry, never BMPOOL02
// package constants. P4-1.2.5.17 adds checkpointed bank rotation/reclamation.
type DynamicJournal struct {
	mu sync.Mutex
	dev device.Device
	sb format.DynamicSuperblock
	ag uint32
	base uint64
	size uint64
	end uint64
	nextTx uint64
	lastTx uint64
	poisoned bool
}

func OpenDynamicJournal(dev device.Device,sb format.DynamicSuperblock,ag uint32)(*DynamicJournal,error){
	if dev==nil{return nil,errors.New("nil BMPOOL03 metadata device")}
	if err:=sb.Validate();err!=nil{return nil,err}
	g:=sb.Geometry
	if ag>=g.AGCount{return nil,fmt.Errorf("BMPOOL03 AG %d out of range",ag)}
	if g.WALSegmentBytes<4*format.PageSize{return nil,errors.New("BMPOOL03 WAL segment must be at least 16KiB")}
	if dev.Size()<0||uint64(dev.Size())<g.MetadataBytes{return nil,errors.New("BMPOOL03 MetadataView is smaller than geometry")}
	base:=g.WALOffset+uint64(ag)*g.WALSegmentBytes
	if base<g.WALOffset||base>g.MetadataBytes||g.WALSegmentBytes>g.MetadataBytes-base{return nil,errors.New("BMPOOL03 AG WAL segment outside MetadataView")}
	j:=&DynamicJournal{dev:dev,sb:sb,ag:ag,base:base,size:g.WALSegmentBytes,nextTx:1}
	if _,err:=j.Recover();err!=nil{return nil,fmt.Errorf("recover BMPOOL03 AG %d WAL: %w",ag,err)}
	return j,nil
}

// Commit persists Intent, then Commit, then applies page after-images. An
// error after any WAL write poisons this instance; reopen/recovery is required.
func(j *DynamicJournal)Commit(updates []PageUpdate)error{
	if len(updates)==0{return errors.New("empty BMPOOL03 WAL transaction")}
	j.mu.Lock();defer j.mu.Unlock()
	if j.poisoned{return errors.New("BMPOOL03 WAL requires recovery after previous I/O error")}
	if j.nextTx==0||j.nextTx==math.MaxUint64{return errors.New("BMPOOL03 WAL transaction ID exhausted")}
	if err:=j.validateUpdates(updates);err!=nil{return err}
	payload,err:=encodeUpdates(updates);if err!=nil{return err}
	tx:=j.nextTx
	intent,err:=encodeJournalRecord(journalIntent,tx,payload);if err!=nil{return err}
	if err=j.append(intent);err!=nil{if !errors.Is(err,ErrJournalFull){j.poisoned=true};return err}
	j.nextTx++
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	cp:=make([]byte,4);binary.LittleEndian.PutUint32(cp,crc32.Checksum(payload,journalCRC))
	commit,err:=encodeJournalRecord(journalCommit,tx,cp);if err!=nil{j.poisoned=true;return err}
	if err=j.append(commit);err!=nil{j.poisoned=true;return err}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	for _,u:=range updates{if err=j.apply(u);err!=nil{j.poisoned=true;return fmt.Errorf("apply BMPOOL03 WAL tx %d: %w",tx,err)}}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	j.lastTx=tx
	return nil
}

func(j *DynamicJournal)append(record []byte)error{
	if uint64(len(record))>j.size-j.end{return ErrJournalFull}
	if err:=writeAtFull(j.dev,record,int64(j.base+j.end));err!=nil{return err}
	j.end+=uint64(len(record));return nil
}

func(j *DynamicJournal)validateUpdates(updates []PageUpdate)error{
	seen:=make(map[uint64]struct{},len(updates))
	walEnd:=j.sb.Geometry.WALOffset+j.sb.Geometry.WALSize
	for _,u:=range updates{
		if u.Offset%format.PageSize!=0||len(u.Image)!=int(format.PageSize){return errors.New("BMPOOL03 WAL update must be a 4KiB page")}
		if u.Offset<2*format.PageSize||u.Offset>j.sb.Geometry.MetadataBytes||format.PageSize>j.sb.Geometry.MetadataBytes-u.Offset{return errors.New("BMPOOL03 WAL update outside metadata data regions")}
		if u.Offset<dynamicFormatMarkerEnd&&u.Offset+format.PageSize>dynamicFormatMarkerStart{return errors.New("BMPOOL03 WAL update overlaps format marker")}
		if u.Offset<walEnd&&u.Offset+format.PageSize>j.sb.Geometry.WALOffset{return errors.New("BMPOOL03 WAL update overlaps WAL region")}
		if _,ok:=seen[u.Offset];ok{return errors.New("duplicate BMPOOL03 WAL page update")};seen[u.Offset]=struct{}{}
	}
	return nil
}

const dynamicFormatMarkerStart=uint64(2*format.PageSize)
const dynamicFormatMarkerEnd=dynamicFormatMarkerStart+format.PageSize

func(j *DynamicJournal)apply(u PageUpdate)error{return writeAtFull(j.dev,u.Image,int64(u.Offset))}

// Recover scans the complete dynamic AG segment and idempotently reapplies
// every committed transaction. Incomplete trailing records are ignored.
func(j *DynamicJournal)Recover()(bool,error){
	j.mu.Lock();defer j.mu.Unlock()
	intents:=map[uint64][]PageUpdate{}
	intentCRC:=map[uint64]uint32{}
	var off uint64
	next:=uint64(1)
	var committed []journalTxn
	for off<j.size{
		if j.size-off<journalHeaderSize{break}
		h:=make([]byte,journalHeaderSize)
		n,err:=j.dev.ReadAt(h,int64(j.base+off))
		if err!=nil&&err!=io.EOF{return false,err}
		if n==0&&err==io.EOF{break}
		if n<len(h){break}
		if allZero(h){break}
		if binary.LittleEndian.Uint32(h[:4])!=journalMagic||binary.LittleEndian.Uint16(h[4:6])!=journalVersion||crc32.Checksum(h[:24],journalCRC)!=binary.LittleEndian.Uint32(h[24:28]){return false,ErrJournalCorrupt}
		kind:=binary.LittleEndian.Uint16(h[6:8]);plen:=uint64(binary.LittleEndian.Uint32(h[8:12]));tx:=binary.LittleEndian.Uint64(h[12:20])
		if tx==0||plen>journalMaxRecord||plen>j.size-off-journalHeaderSize{return false,ErrJournalCorrupt}
		p:=make([]byte,int(plen));if plen>0{n,err=j.dev.ReadAt(p,int64(j.base+off+journalHeaderSize));if err!=nil&&err!=io.EOF{return false,err};if n<len(p){break}}
		if crc32.Checksum(p,journalCRC)!=binary.LittleEndian.Uint32(h[20:24]){return false,ErrJournalCorrupt}
		switch kind{
		case journalIntent:
			if _,ok:=intents[tx];ok{return false,ErrJournalCorrupt}
			updates,e:=decodeUpdates(p);if e!=nil{return false,e};if e=j.validateUpdates(updates);e!=nil{return false,ErrJournalCorrupt}
			intents[tx]=updates;intentCRC[tx]=crc32.Checksum(p,journalCRC)
		case journalCommit:
			if len(p)!=4||intents[tx]==nil||binary.LittleEndian.Uint32(p)!=intentCRC[tx]{return false,ErrJournalCorrupt}
			committed=append(committed,journalTxn{txID:tx,updates:intents[tx]});delete(intents,tx);delete(intentCRC,tx)
		default:return false,ErrJournalCorrupt
		}
		if tx>=next{if tx==math.MaxUint64{next=0}else{next=tx+1}}
		off+=journalHeaderSize+plen
	}
	for _,tx:=range committed{for _,u:=range tx.updates{if err:=j.apply(u);err!=nil{return false,err}}}
	if len(committed)>0{if err:=j.dev.Sync();err!=nil{return false,err}}
	j.end,j.nextTx,j.poisoned=off,next,false
	for _,tx:=range committed{if tx.txID>j.lastTx{j.lastTx=tx.txID}}
	return len(committed)>0,nil
}

func(j *DynamicJournal)UsedBytes()uint64{j.mu.Lock();defer j.mu.Unlock();return j.end}
func(j *DynamicJournal)CapacityBytes()uint64{return j.size}
func(j *DynamicJournal)AG()uint32{return j.ag}
