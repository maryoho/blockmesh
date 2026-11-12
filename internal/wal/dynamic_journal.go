package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"

		"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

// DynamicJournal is a BMPOOL03 per-AG redo journal. Each geometry-sized AG
// segment is split into two banks; dual checkpoint descriptors select the
// durable active bank and make bank rotation crash-safe.
type DynamicJournal struct {
	mu sync.Mutex
	dev DynamicMetadataDevice
	sb format.DynamicSuperblock
	ag uint32
	segmentBase uint64
	bankSize uint64
	base uint64
	end uint64
	activeBank uint32
	checkpointSlot uint32
	checkpointGeneration uint64
	checkpointTxID uint64
	nextTx uint64
	lastTx uint64
	poisoned bool
}

func OpenDynamicJournal(dev DynamicMetadataDevice,sb format.DynamicSuperblock,ag uint32)(*DynamicJournal,error){
	if dev==nil{return nil,errors.New("nil BMPOOL03 metadata device")}
	if err:=sb.Validate();err!=nil{return nil,err}
	g:=sb.Geometry
	if ag>=g.AGCount{return nil,fmt.Errorf("BMPOOL03 AG %d out of range",ag)}
	if g.WALSegmentBytes<4*format.PageSize||g.WALSegmentBytes%(2*format.PageSize)!=0{return nil,errors.New("BMPOOL03 WAL segment must contain two page-aligned banks")}
	if dev.Size()<0||uint64(dev.Size())<g.MetadataBytes{return nil,errors.New("BMPOOL03 MetadataView is smaller than geometry")}
	seg:=g.WALOffset+uint64(ag)*g.WALSegmentBytes
	if seg<g.WALOffset||seg>g.MetadataBytes||g.WALSegmentBytes>g.MetadataBytes-seg{return nil,errors.New("BMPOOL03 AG WAL segment outside MetadataView")}
	j:=&DynamicJournal{dev:dev,sb:sb,ag:ag,segmentBase:seg,bankSize:g.WALSegmentBytes/2}
	cp,slot,err:=j.loadCheckpoint()
	if err!=nil{return nil,err}
	j.activeBank,j.checkpointSlot=cp.ActiveBank,slot
	j.checkpointGeneration,j.checkpointTxID=cp.Generation,cp.CheckpointTxID
	j.lastTx=cp.CheckpointTxID
	if j.lastTx==math.MaxUint64{return nil,errors.New("BMPOOL03 WAL transaction ID exhausted")}
	j.nextTx=j.lastTx+1
	j.base=j.segmentBase+uint64(j.activeBank)*j.bankSize
	if _,err=j.Recover();err!=nil{return nil,fmt.Errorf("recover BMPOOL03 AG %d WAL: %w",ag,err)}
	return j,nil
}

func(j *DynamicJournal)loadCheckpoint()(format.DynamicCheckpoint,uint32,error){
	var zero format.DynamicCheckpoint
	var valid []format.DynamicCheckpoint
	var slots []uint32
	for slot:=uint32(0);slot<2;slot++{
		off,err:=format.DynamicCheckpointOffset(j.sb.Geometry,j.ag,slot);if err!=nil{return zero,0,err}
		page:=make([]byte,format.PageSize)
		n,e:=j.dev.ReadAt(page,int64(off));if e!=nil&&!(e==io.EOF&&n==len(page)){return zero,0,e};if n!=len(page){return zero,0,io.ErrUnexpectedEOF}
		cp,e:=format.DecodeDynamicCheckpoint(page,j.ag);if e==nil{valid=append(valid,cp);slots=append(slots,slot)}
	}
	if len(valid)==0{return zero,0,errors.New("no valid BMPOOL03 checkpoint descriptor")}
	best:=0
	if len(valid)==2{
		if valid[0].Generation==valid[1].Generation&&valid[0]!=valid[1]{return zero,0,errors.New("conflicting BMPOOL03 checkpoint copies at equal generation")}
		if valid[1].Generation>valid[0].Generation{best=1}
	}
	chosen,slot:=valid[best],slots[best]
	other:=uint32(1)-slot
	off,err:=format.DynamicCheckpointOffset(j.sb.Geometry,j.ag,other);if err!=nil{return zero,0,err}
	page,err:=chosen.Encode();if err!=nil{return zero,0,err}
	if err=writeDynamicAtFull(j.dev,page,int64(off));err!=nil{return zero,0,err}
	if err=j.dev.Sync();err!=nil{return zero,0,err}
	return chosen,slot,nil
}

// Commit follows Intent Sync -> Commit Sync -> apply after-images -> Sync.
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
	if err=j.append(commit);err!=nil{if !errors.Is(err,ErrJournalFull){j.poisoned=true};return err}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	for _,u:=range updates{if err=j.apply(u);err!=nil{j.poisoned=true;return fmt.Errorf("apply BMPOOL03 WAL tx %d: %w",tx,err)}}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	j.lastTx=tx
	return nil
}

func(j *DynamicJournal)append(record []byte)error{
	if uint64(len(record))>j.bankSize-j.end{return ErrJournalFull}
	if err:=writeDynamicAtFull(j.dev,record,int64(j.base+j.end));err!=nil{return err}
	j.end+=uint64(len(record));return nil
}

func(j *DynamicJournal)validateUpdates(updates []PageUpdate)error{
	seen:=make(map[uint64]struct{},len(updates))
	g:=j.sb.Geometry
	walEnd:=g.WALOffset+g.WALSize
	for _,u:=range updates{
		if u.Offset%format.PageSize!=0||len(u.Image)!=int(format.PageSize){return errors.New("BMPOOL03 WAL update must be a 4KiB page")}
		if u.Offset<2*format.PageSize||u.Offset>g.MetadataBytes||format.PageSize>g.MetadataBytes-u.Offset{return errors.New("BMPOOL03 WAL update outside metadata data regions")}
		if u.Offset<3*format.PageSize&&u.Offset+format.PageSize>2*format.PageSize{return errors.New("BMPOOL03 WAL update overlaps format marker")}
		if u.Offset<walEnd&&u.Offset+format.PageSize>g.WALOffset{return errors.New("BMPOOL03 WAL update overlaps WAL region")}
		if _,ok:=seen[u.Offset];ok{return errors.New("duplicate BMPOOL03 WAL page update")};seen[u.Offset]=struct{}{}
	}
	return nil
}
func(j *DynamicJournal)apply(u PageUpdate)error{return writeDynamicAtFull(j.dev,u.Image,int64(u.Offset))}


// CommitImages adapts DynamicJournal to the metastore transaction interface.
func(j *DynamicJournal)CommitImages(images []metastore.PageImage)error{
	updates:=make([]PageUpdate,len(images))
	for i,image:=range images{updates[i]=PageUpdate{Offset:image.Offset,Image:image.Data}}
	return j.Commit(updates)
}

// Checkpoint clears the inactive bank before publishing a new dual-copy
// descriptor. A crash before descriptor durability keeps the old bank active;
// after descriptor durability the selected bank is already empty.
func(j *DynamicJournal)Checkpoint()error{
	j.mu.Lock();defer j.mu.Unlock()
	if j.poisoned{return errors.New("BMPOOL03 WAL requires recovery before checkpoint")}
	if j.checkpointGeneration==math.MaxUint64{return errors.New("BMPOOL03 checkpoint generation exhausted")}
	nextBank:=uint32(1)-j.activeBank
	nextBase:=j.segmentBase+uint64(nextBank)*j.bankSize
	if err:=zeroDynamicRange(j.dev,nextBase,j.bankSize);err!=nil{return fmt.Errorf("clear inactive BMPOOL03 WAL bank: %w",err)}
	if err:=j.dev.Sync();err!=nil{return fmt.Errorf("sync cleared BMPOOL03 WAL bank: %w",err)}
	cp:=format.DynamicCheckpoint{AG:j.ag,Generation:j.checkpointGeneration+1,CheckpointTxID:j.lastTx,ActiveBank:nextBank}
	page,err:=cp.Encode();if err!=nil{return err}
	nextSlot:=uint32(1)-j.checkpointSlot
	off,err:=format.DynamicCheckpointOffset(j.sb.Geometry,j.ag,nextSlot);if err!=nil{return err}
	if err=writeDynamicAtFull(j.dev,page,int64(off));err!=nil{j.poisoned=true;return err}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	mirrorOff,err:=format.DynamicCheckpointOffset(j.sb.Geometry,j.ag,j.checkpointSlot);if err!=nil{j.poisoned=true;return err}
	if err=writeDynamicAtFull(j.dev,page,int64(mirrorOff));err!=nil{j.poisoned=true;return err}
	if err=j.dev.Sync();err!=nil{j.poisoned=true;return err}
	j.activeBank,j.checkpointSlot=nextBank,nextSlot
	j.checkpointGeneration,j.checkpointTxID=cp.Generation,cp.CheckpointTxID
	j.base,j.end=nextBase,0
	if cp.CheckpointTxID==math.MaxUint64{j.poisoned=true;return errors.New("BMPOOL03 WAL transaction ID exhausted")}
	j.nextTx=cp.CheckpointTxID+1
	return nil
}

// Recover replays committed transactions in the selected bank. A durable
// Intent without Commit is retained but never applied.
func(j *DynamicJournal)Recover()(bool,error){
	j.mu.Lock();defer j.mu.Unlock()
	intents:=map[uint64][]PageUpdate{}
	intentCRC:=map[uint64]uint32{}
	var off uint64
	next:=j.checkpointTxID+1
	var committed []journalTxn
	for off<j.bankSize{
		if j.bankSize-off<journalHeaderSize{break}
		h:=make([]byte,journalHeaderSize)
		n,err:=j.dev.ReadAt(h,int64(j.base+off));if err!=nil&&err!=io.EOF{return false,err};if n==0&&err==io.EOF{break};if n<len(h){break}
		if allZero(h){break}
		if binary.LittleEndian.Uint32(h[:4])!=journalMagic||binary.LittleEndian.Uint16(h[4:6])!=journalVersion||crc32.Checksum(h[:24],journalCRC)!=binary.LittleEndian.Uint32(h[24:28]){return false,ErrJournalCorrupt}
		kind:=binary.LittleEndian.Uint16(h[6:8]);plen:=uint64(binary.LittleEndian.Uint32(h[8:12]));tx:=binary.LittleEndian.Uint64(h[12:20])
		if tx<=j.checkpointTxID||tx==0||plen>journalMaxRecord||plen>j.bankSize-off-journalHeaderSize{return false,ErrJournalCorrupt}
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
func(j *DynamicJournal)CapacityBytes()uint64{return j.bankSize}
func(j *DynamicJournal)AG()uint32{return j.ag}

func writeDynamicAtFull(dev DynamicMetadataDevice,b []byte,off int64)error{
	n,err:=dev.WriteAt(b,off);if err!=nil{return err};if n!=len(b){return io.ErrShortWrite};return nil
}

func zeroDynamicRange(dev DynamicMetadataDevice,off,length uint64)error{
	zero:=make([]byte,1<<20)
	for done:=uint64(0);done<length;{
		n:=uint64(len(zero));if length-done<n{n=length-done}
		if err:=writeDynamicAtFull(dev,zero[:int(n)],int64(off+done));err!=nil{return err}
		done+=n
	}
	return nil
}
