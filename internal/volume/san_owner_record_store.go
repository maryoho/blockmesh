package volume

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strings"
	"sync"
)

const (
	ownerRecordPageSize = 4096
	ownerRecordCRCOffset = ownerRecordPageSize - 4
	ownerRecordVersion = uint32(1)
)

var (
	ownerRecordMagic = [8]byte{'B','M','O','W','N','R','0','1'}
	ownerRecordCRCTable = crc32.MakeTable(crc32.Castagnoli)
	ErrSANOwnerRecordNotFound = errors.New("SAN OwnerRecord not found")
	ErrSANOwnerRecordCorrupt = errors.New("SAN OwnerRecord copies are corrupt")
)

// OwnerRecordDevice is the minimal durable random-access interface required
// by SANOwnerRecordStore. The device is normally the shared Pool metadata LUN.
type OwnerRecordDevice interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

type SANOwnerRecordStoreConfig struct {
	Device OwnerRecordDevice
	VolumeID string
	// SlotAOffset and SlotBOffset are two dedicated, 4 KiB-aligned metadata
	// pages reserved for this Volume. They must not overlap Pool superblocks,
	// Volume descriptors, WAL, bitmap, mapping, Arena, lifecycle or Reader Table.
	SlotAOffset uint64
	SlotBOffset uint64
}

// SANOwnerRecordStore persists the last Owner identity directly on shared SAN
// metadata. It has no Kubernetes, API-server, etcd, file-system or network
// control-plane dependency.
//
// CompareAndSwap MUST be called while the caller holds the exclusive sanlock
// Owner resource for this Volume. sanlock supplies cross-node serialization;
// the two checksummed slots supply crash-safe durable publication. This type
// deliberately does not pretend that a process-local mutex is a distributed
// lock and does not acquire a second lock of its own.
type SANOwnerRecordStore struct {
	dev OwnerRecordDevice
	volumeID string
	offsets [2]uint64
	mu sync.Mutex
}

type ownerRecordSlot struct {
	record OwnerRecord
	generation uint64
	index int
}

func NewSANOwnerRecordStore(cfg SANOwnerRecordStoreConfig) (*SANOwnerRecordStore, error) {
	if cfg.Device == nil || strings.TrimSpace(cfg.VolumeID) == "" || len(cfg.VolumeID) > 64 {
		return nil, errors.New("SAN OwnerRecord store requires a device and VolumeID of 1..64 bytes")
	}
	if cfg.SlotAOffset%ownerRecordPageSize != 0 || cfg.SlotBOffset%ownerRecordPageSize != 0 ||
		cfg.SlotAOffset == cfg.SlotBOffset || cfg.SlotAOffset > math.MaxInt64-ownerRecordPageSize ||
		cfg.SlotBOffset > math.MaxInt64-ownerRecordPageSize {
		return nil, errors.New("SAN OwnerRecord slots must be distinct aligned 4KiB offsets")
	}
	return &SANOwnerRecordStore{dev:cfg.Device,volumeID:cfg.VolumeID,offsets:[2]uint64{cfg.SlotAOffset,cfg.SlotBOffset}},nil
}

func encodeSANOwnerRecord(record OwnerRecord, generation uint64) ([]byte,error) {
	if strings.TrimSpace(record.VolumeID)=="" || len(record.VolumeID)>64 ||
		strings.TrimSpace(record.NodeID)=="" || len(record.NodeID)>128 ||
		strings.TrimSpace(record.BootID)=="" || len(record.BootID)>128 ||
		record.Epoch==0 || generation==0 {
		return nil, errors.New("invalid SAN OwnerRecord")
	}
	b:=make([]byte,ownerRecordPageSize)
	copy(b[:8],ownerRecordMagic[:])
	binary.LittleEndian.PutUint32(b[8:12],ownerRecordVersion)
	binary.LittleEndian.PutUint64(b[16:24],generation)
	binary.LittleEndian.PutUint64(b[24:32],record.Epoch)
	binary.LittleEndian.PutUint16(b[32:34],uint16(len(record.VolumeID)))
	binary.LittleEndian.PutUint16(b[34:36],uint16(len(record.NodeID)))
	binary.LittleEndian.PutUint16(b[36:38],uint16(len(record.BootID)))
	copy(b[40:104],record.VolumeID)
	copy(b[104:232],record.NodeID)
	copy(b[232:360],record.BootID)
	binary.LittleEndian.PutUint32(b[ownerRecordCRCOffset:],crc32.Checksum(b[:ownerRecordCRCOffset],ownerRecordCRCTable))
	return b,nil
}

func decodeSANOwnerRecord(b []byte, expectedVolume string, index int) (ownerRecordSlot,error) {
	var out ownerRecordSlot
	if len(b)!=ownerRecordPageSize || string(b[:8])!=string(ownerRecordMagic[:]) ||
		binary.LittleEndian.Uint32(b[8:12])!=ownerRecordVersion ||
		crc32.Checksum(b[:ownerRecordCRCOffset],ownerRecordCRCTable)!=binary.LittleEndian.Uint32(b[ownerRecordCRCOffset:]) {
		return out,ErrSANOwnerRecordCorrupt
	}
	generation:=binary.LittleEndian.Uint64(b[16:24])
	vl,nl,bl:=int(binary.LittleEndian.Uint16(b[32:34])),int(binary.LittleEndian.Uint16(b[34:36])),int(binary.LittleEndian.Uint16(b[36:38]))
	if generation==0 || vl<1 || vl>64 || nl<1 || nl>128 || bl<1 || bl>128 {
		return out,ErrSANOwnerRecordCorrupt
	}
	record:=OwnerRecord{
		VolumeID:string(b[40:40+vl]),
		NodeID:string(b[104:104+nl]),
		BootID:string(b[232:232+bl]),
		Epoch:binary.LittleEndian.Uint64(b[24:32]),
	}
	if record.VolumeID!=expectedVolume || record.Epoch==0 {
		return out,ErrSANOwnerRecordCorrupt
	}
	for _,v:=range b[12:16] { if v!=0 { return out,ErrSANOwnerRecordCorrupt } }
	for _,v:=range b[38:40] { if v!=0 { return out,ErrSANOwnerRecordCorrupt } }
	for _,v:=range b[360:ownerRecordCRCOffset] { if v!=0 { return out,ErrSANOwnerRecordCorrupt } }
	return ownerRecordSlot{record:record,generation:generation,index:index},nil
}

func (s *SANOwnerRecordStore) readCurrent() (ownerRecordSlot,bool,error) {
	var valid []ownerRecordSlot
	var nonzero bool
	for i,off:=range s.offsets {
		b:=make([]byte,ownerRecordPageSize)
		n,err:=s.dev.ReadAt(b,int64(off))
		if err!=nil && !(err==io.EOF && n==len(b)) {
			if err==io.EOF && n==0 { continue }
			return ownerRecordSlot{},false,fmt.Errorf("read SAN OwnerRecord slot %d: %w",i,err)
		}
		if n!=len(b) { continue }
		if allZeroBytes(b) { continue }
		nonzero=true
		slot,decodeErr:=decodeSANOwnerRecord(b,s.volumeID,i)
		if decodeErr==nil { valid=append(valid,slot) }
	}
	if len(valid)==0 {
		if !nonzero { return ownerRecordSlot{},false,nil }
		return ownerRecordSlot{},false,ErrSANOwnerRecordCorrupt
	}
	best:=valid[0]
	for _,slot:=range valid[1:] {
		if slot.generation==best.generation && slot.record!=best.record {
			return ownerRecordSlot{},false,ErrSANOwnerRecordCorrupt
		}
		if slot.generation>best.generation { best=slot }
	}
	return best,true,nil
}

func allZeroBytes(b []byte) bool {
	for _,v:=range b { if v!=0 { return false } }
	return true
}

func (s *SANOwnerRecordStore) Load(ctx context.Context, volumeID string) (OwnerRecord,bool,error) {
	if s==nil || ctx==nil { return OwnerRecord{},false,errors.New("SAN OwnerRecord store and context are required") }
	if volumeID!=s.volumeID { return OwnerRecord{},false,errors.New("SAN OwnerRecord VolumeID mismatch") }
	if err:=ctx.Err();err!=nil { return OwnerRecord{},false,err }
	s.mu.Lock()
	defer s.mu.Unlock()
	slot,ok,err:=s.readCurrent()
	if err!=nil || !ok { return OwnerRecord{},ok,err }
	return slot.record,true,nil
}

// CompareAndSwap writes the inactive checksummed slot and flushes it before
// returning. A torn write leaves the previous slot intact. Cross-node CAS
// safety depends on the mandatory sanlock Owner resource held by the caller.
func (s *SANOwnerRecordStore) CompareAndSwap(ctx context.Context, volumeID string, expected *OwnerRecord, next OwnerRecord) error {
	if s==nil || ctx==nil { return errors.New("SAN OwnerRecord store and context are required") }
	if volumeID!=s.volumeID || next.VolumeID!=s.volumeID { return errors.New("SAN OwnerRecord VolumeID mismatch") }
	if err:=ctx.Err();err!=nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	current,exists,err:=s.readCurrent()
	if err!=nil { return err }
	if exists!=(expected!=nil) || (expected!=nil && current.record!=*expected) {
		return ErrOwnerRecordConflict
	}
	generation:=uint64(1)
	target:=0
	if exists {
		if next.Epoch<=current.record.Epoch {
			return fmt.Errorf("OwnerRecord epoch %d must exceed durable epoch %d",next.Epoch,current.record.Epoch)
		}
		if current.generation==math.MaxUint64 { return errors.New("SAN OwnerRecord generation exhausted") }
		generation=current.generation+1
		target=1-current.index
	}
	image,err:=encodeSANOwnerRecord(next,generation)
	if err!=nil { return err }
	n,err:=s.dev.WriteAt(image,int64(s.offsets[target]))
	if err!=nil { return fmt.Errorf("write SAN OwnerRecord slot %d: %w",target,err) }
	if n!=len(image) { return io.ErrShortWrite }
	if err=s.dev.Sync();err!=nil { return fmt.Errorf("flush SAN OwnerRecord slot %d: %w",target,err) }
	return nil
}

var _ OwnerRecordStore = (*SANOwnerRecordStore)(nil)
