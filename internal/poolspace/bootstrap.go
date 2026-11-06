package poolspace

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"blockmesh/internal/device"
)

const (
	BootstrapMagic uint64 = 0x424D504F4F4C3033 // BMPOOL03
	BootstrapVersion uint32 = 3
	BootstrapSlotA uint64 = 0
	BootstrapSlotB uint64 = PageSize
	extentMapSlotA uint64 = 2 * PageSize
	extentMapSlotSize uint64 = (BootstrapSize - 2*PageSize) / 2
	bootstrapCRCOffset = 128
)

var (
	ErrLegacyPoolFormat = errors.New("legacy BMPOOL02 Pool format is read-only to the dynamic layout implementation")
	ErrBootstrapUnformatted = errors.New("dynamic Pool bootstrap metadata is not formatted")
	ErrExtentMapTooLarge = errors.New("extent map exceeds reserved bootstrap slot")
)

type BootstrapRecord struct {
	Generation uint64
	PoolUUID [16]byte
	DeviceBytes uint64
	BootstrapBytes uint64
	MetadataBytes uint64
	ExtentMapOffset uint64
	ExtentMapBytes uint64
	ExtentMapGeneration uint64
	LogicalSectorBytes uint32
	Slot uint64
}

type BootstrapStore struct {
	dev device.Device
	record BootstrapRecord
	allocator *Allocator
}

func (r BootstrapRecord) encode() ([]byte, error) {
	if r.Generation == 0 || r.PoolUUID == ([16]byte{}) || r.DeviceBytes == 0 ||
		r.BootstrapBytes != BootstrapSize || r.MetadataBytes < r.BootstrapBytes ||
		r.MetadataBytes > r.DeviceBytes || r.ExtentMapGeneration == 0 ||
		(r.ExtentMapOffset != extentMapSlotA && r.ExtentMapOffset != extentMapSlotA+extentMapSlotSize) ||
		r.ExtentMapBytes == 0 || r.ExtentMapBytes > extentMapSlotSize ||
		r.LogicalSectorBytes < 512 || r.LogicalSectorBytes > 65536 ||
		r.LogicalSectorBytes&(r.LogicalSectorBytes-1) != 0 {
		return nil, ErrInvalidGeometry
	}
	b := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(b[0:8], BootstrapMagic)
	binary.LittleEndian.PutUint32(b[8:12], BootstrapVersion)
	binary.LittleEndian.PutUint32(b[12:16], uint32(PageSize))
	binary.LittleEndian.PutUint64(b[16:24], r.Generation)
	copy(b[24:40], r.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[40:48], r.DeviceBytes)
	binary.LittleEndian.PutUint64(b[48:56], r.BootstrapBytes)
	binary.LittleEndian.PutUint64(b[56:64], r.MetadataBytes)
	binary.LittleEndian.PutUint64(b[64:72], r.ExtentMapOffset)
	binary.LittleEndian.PutUint64(b[72:80], r.ExtentMapBytes)
	binary.LittleEndian.PutUint64(b[80:88], r.ExtentMapGeneration)
	binary.LittleEndian.PutUint32(b[88:92], r.LogicalSectorBytes)
	binary.LittleEndian.PutUint32(b[bootstrapCRCOffset:bootstrapCRCOffset+4], 0)
	binary.LittleEndian.PutUint32(b[bootstrapCRCOffset:bootstrapCRCOffset+4], crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	return b, nil
}

func decodeBootstrap(b []byte, slot uint64) (BootstrapRecord, error) {
	var r BootstrapRecord
	if len(b) != int(PageSize) || binary.LittleEndian.Uint64(b[:8]) != BootstrapMagic {
		return r, ErrBootstrapUnformatted
	}
	if binary.LittleEndian.Uint32(b[8:12]) != BootstrapVersion || binary.LittleEndian.Uint32(b[12:16]) != uint32(PageSize) {
		return r, errors.New("unsupported dynamic Pool bootstrap version")
	}
	c := append([]byte(nil), b...)
	want := binary.LittleEndian.Uint32(c[bootstrapCRCOffset:bootstrapCRCOffset+4])
	binary.LittleEndian.PutUint32(c[bootstrapCRCOffset:bootstrapCRCOffset+4], 0)
	if crc32.Checksum(c, crc32.MakeTable(crc32.Castagnoli)) != want {
		return r, errors.New("Bootstrap Metadata CRC32C mismatch")
	}
	r.Generation = binary.LittleEndian.Uint64(b[16:24])
	copy(r.PoolUUID[:], b[24:40])
	r.DeviceBytes = binary.LittleEndian.Uint64(b[40:48])
	r.BootstrapBytes = binary.LittleEndian.Uint64(b[48:56])
	r.MetadataBytes = binary.LittleEndian.Uint64(b[56:64])
	r.ExtentMapOffset = binary.LittleEndian.Uint64(b[64:72])
	r.ExtentMapBytes = binary.LittleEndian.Uint64(b[72:80])
	r.ExtentMapGeneration = binary.LittleEndian.Uint64(b[80:88])
	r.LogicalSectorBytes = binary.LittleEndian.Uint32(b[88:92])
	r.Slot = slot
	if _, err := r.encode(); err != nil { return BootstrapRecord{}, err }
	return r, nil
}

func readFull(dev device.Device, b []byte, off int64) error {
	n, err := dev.ReadAt(b, off)
	if err != nil && !(errors.Is(err, io.EOF) && n == len(b)) { return err }
	if n != len(b) { return io.ErrUnexpectedEOF }
	return nil
}
func writeFull(dev device.Device, b []byte, off int64) error {
	n, err := dev.WriteAt(b, off)
	if err != nil { return err }
	if n != len(b) { return io.ErrShortWrite }
	return nil
}

func loadExtentMap(dev device.Device, r BootstrapRecord) (ExtentMap, error) {
	if r.ExtentMapBytes > extentMapSlotSize || r.ExtentMapOffset > uint64(dev.Size()) ||
		r.ExtentMapBytes > uint64(dev.Size())-r.ExtentMapOffset || r.ExtentMapBytes > math.MaxInt {
		return ExtentMap{}, ErrInvalidGeometry
	}
	b := make([]byte, int(r.ExtentMapBytes))
	if err := readFull(dev, b, int64(r.ExtentMapOffset)); err != nil { return ExtentMap{}, err }
	m, err := DecodeExtentMap(b)
	if err != nil { return ExtentMap{}, err }
	if m.Generation != r.ExtentMapGeneration || m.DeviceBytes != r.DeviceBytes { return ExtentMap{}, ErrInvalidGeometry }
	return m, nil
}

// FormatBootstrap creates a new BMPOOL03 Pool. It refuses non-zero bootstrap
// pages, so it cannot overwrite BMPOOL02 or an unrecognized existing format.
// The caller must hold exclusive ownership/fencing.
func FormatBootstrap(dev device.Device, volumeSlots uint32, logicalSectorBytes uint32) (*BootstrapStore, error) {
	if dev == nil || dev.Size() <= 0 { return nil, ErrInvalidGeometry }
	deviceBytes := uint64(dev.Size())
	if logicalSectorBytes == 0 { logicalSectorBytes = 512 }
	if logicalSectorBytes < 512 || logicalSectorBytes > 65536 ||
		logicalSectorBytes&(logicalSectorBytes-1) != 0 || deviceBytes%uint64(logicalSectorBytes) != 0 {
		return nil, ErrInvalidGeometry
	}
	for _, off := range []uint64{BootstrapSlotA, BootstrapSlotB} {
		p := make([]byte, PageSize)
		if err := readFull(dev, p, int64(off)); err != nil { return nil, fmt.Errorf("inspect bootstrap page at %d: %w", off, err) }
		magic := binary.LittleEndian.Uint64(p[:8])
		if magic == 0x424D504F4F4C3032 { return nil, ErrLegacyPoolFormat }
		for _, v := range p { if v != 0 { return nil, errors.New("refusing to format a non-empty or unknown device") } }
	}
	plan, err := PlanDevice(deviceBytes, volumeSlots)
	if err != nil { return nil, err }
	a, err := NewAllocator(deviceBytes, BootstrapSize, plan.InitialMetadata)
	if err != nil { return nil, err }
	m := ExtentMap{Generation:1, DeviceBytes:deviceBytes, Extents:a.Extents()}
	encoded, err := m.Encode()
	if err != nil { return nil, err }
	if uint64(len(encoded)) > extentMapSlotSize { return nil, ErrExtentMapTooLarge }
	var uuid [16]byte
	if _, err = io.ReadFull(rand.Reader, uuid[:]); err != nil { return nil, err }
	r := BootstrapRecord{Generation:1, PoolUUID:uuid, DeviceBytes:deviceBytes,
		BootstrapBytes:BootstrapSize, MetadataBytes:BootstrapSize+plan.InitialMetadata,
		ExtentMapOffset:extentMapSlotA, ExtentMapBytes:uint64(len(encoded)),
		ExtentMapGeneration:1, LogicalSectorBytes:logicalSectorBytes}
	if err = writeFull(dev, encoded, int64(r.ExtentMapOffset)); err != nil { return nil, err }
	if err = dev.Sync(); err != nil { return nil, err }
	sb, err := r.encode()
	if err != nil { return nil, err }
	r.Slot = BootstrapSlotB
	if err = writeFull(dev, sb, int64(BootstrapSlotB)); err != nil { return nil, err }
	if err = dev.Sync(); err != nil { return nil, err }
	return &BootstrapStore{dev:dev, record:r, allocator:a}, nil
}

// OpenBootstrap chooses the highest valid record whose referenced extent map
// also validates. A torn newer copy is ignored in favor of the older copy.
func OpenBootstrap(dev device.Device) (*BootstrapStore, error) {
	if dev == nil { return nil, ErrInvalidGeometry }
	type candidate struct { r BootstrapRecord; m ExtentMap }
	var candidates []candidate
	legacy := false
	for _, slot := range []uint64{BootstrapSlotA, BootstrapSlotB} {
		b := make([]byte, PageSize)
		if readFull(dev,b,int64(slot)) == nil && binary.LittleEndian.Uint64(b[:8]) == 0x424D504F4F4C3032 { legacy = true }
		r, err := decodeBootstrap(b,slot)
		if err != nil { continue }
		m, err := loadExtentMap(dev,r)
		if err != nil { continue }
		candidates = append(candidates,candidate{r,m})
	}
	if len(candidates)==0 {
		if legacy { return nil,ErrLegacyPoolFormat }
		return nil,ErrBootstrapUnformatted
	}
	best:=candidates[0]
	for _, c:=range candidates[1:] {
		if c.r.PoolUUID!=best.r.PoolUUID { return nil,errors.New("Bootstrap copies refer to different Pool UUIDs") }
		if c.r.Generation==best.r.Generation && c.r!=best.r { return nil,errors.New("conflicting Bootstrap copies at equal generation") }
		if c.r.Generation>best.r.Generation { best=c }
	}
	if uint64(dev.Size())<best.r.DeviceBytes { return nil,errors.New("backing device is smaller than persisted Pool geometry; refusing shrink") }
	a,err:=allocatorFromMap(best.m)
	if err!=nil{return nil,err}
	return &BootstrapStore{dev:dev,record:best.r,allocator:a},nil
}

func allocatorFromMap(m ExtentMap)(*Allocator,error){
	if len(m.Extents)==0||m.Extents[0].Start!=0||m.Extents[0].Length<BootstrapSize||m.Extents[0].Kind!=Metadata{return nil,ErrInvalidGeometry}
	return &Allocator{deviceBytes:m.DeviceBytes,extents:append([]Extent(nil),m.Extents...)},nil
}
func(s *BootstrapStore)Record()BootstrapRecord{if s==nil{return BootstrapRecord{}};return s.record}
func(s *BootstrapStore)Extents()[]Extent{if s==nil||s.allocator==nil{return nil};return s.allocator.Extents()}

func metadataExtentBytes(extents []Extent)uint64{
	var n uint64
	for _,e:=range extents{if e.Kind==Metadata{n+=e.Length}}
	return n
}
func(s *BootstrapStore)persist(next *Allocator,deviceBytes uint64)error{
	if s==nil||s.dev==nil||next==nil{return ErrInvalidGeometry}
	if s.record.Generation==math.MaxUint64||s.record.ExtentMapGeneration==math.MaxUint64{return errors.New("Bootstrap generation exhausted")}
	mapSlot:=extentMapSlotA
	if s.record.ExtentMapOffset==extentMapSlotA{mapSlot=extentMapSlotA+extentMapSlotSize}
	m:=ExtentMap{Generation:s.record.ExtentMapGeneration+1,DeviceBytes:deviceBytes,Extents:next.Extents()}
	encoded,err:=m.Encode();if err!=nil{return err}
	if uint64(len(encoded))>extentMapSlotSize{return ErrExtentMapTooLarge}
	if err=writeFull(s.dev,encoded,int64(mapSlot));err!=nil{return err}
	if err=s.dev.Sync();err!=nil{return err}
	r:=s.record;r.Generation++;r.DeviceBytes=deviceBytes;r.MetadataBytes=metadataExtentBytes(next.Extents())
	r.ExtentMapOffset=mapSlot;r.ExtentMapBytes=uint64(len(encoded));r.ExtentMapGeneration=m.Generation
	inactive:=BootstrapSlotA;if s.record.Slot==BootstrapSlotA{inactive=BootstrapSlotB}
	sb,err:=r.encode();if err!=nil{return err}
	if err=writeFull(s.dev,sb,int64(inactive));err!=nil{return err}
	if err=s.dev.Sync();err!=nil{return err}
	r.Slot=inactive;s.record,s.allocator=r,next
	return nil
}

// Rescan adds newly visible capacity as Free extents only. It never grows
// Metadata automatically and rejects device shrink.
func(s *BootstrapStore)Rescan()(uint64,error){
	if s==nil||s.dev==nil{return 0,ErrInvalidGeometry}
	actual:=uint64(s.dev.Size())
	if r,ok:=s.dev.(interface{Rescan()(uint64,error)});ok{
		var err error;actual,err=r.Rescan();if err!=nil{return s.record.DeviceBytes,err}
	}
	if actual<s.record.DeviceBytes{return s.record.DeviceBytes,fmt.Errorf("device shrink from %d to %d is not supported",s.record.DeviceBytes,actual)}
	if actual==s.record.DeviceBytes{return actual,nil}
	next:=&Allocator{deviceBytes:s.allocator.deviceBytes,extents:s.allocator.Extents()}
	if err:=next.Rescan(actual);err!=nil{return s.record.DeviceBytes,err}
	if err:=s.persist(next,actual);err!=nil{return s.record.DeviceBytes,err}
	return actual,nil
}

// ExpandMetadata takes observed usage and forecast growth, not a requested
// capacity. The allocator chooses the required 1GiB extents automatically.
func(s *BootstrapStore)ExpandMetadata(usedBytes,growthBytes uint64)(uint64,error){
	if s==nil||s.allocator==nil{return 0,ErrInvalidGeometry}
	currentTotal:=metadataExtentBytes(s.allocator.Extents())
	if currentTotal<s.record.BootstrapBytes{return 0,ErrInvalidGeometry}
	current:=currentTotal-s.record.BootstrapBytes
	target,err:=MetadataExpansionTarget(current,usedBytes,growthBytes,ExtentSize)
	if err!=nil{return currentTotal,err}
	next:=&Allocator{deviceBytes:s.allocator.deviceBytes,extents:s.allocator.Extents()}
	if _,err=next.AllocateMetadata(target-current);err!=nil{return currentTotal,err}
	if err=s.persist(next,s.record.DeviceBytes);err!=nil{return currentTotal,err}
	return target+s.record.BootstrapBytes,nil
}
