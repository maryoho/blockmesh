package poolspace

import (
	"context"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"
	"time"

	"blockmesh/internal/device"
)

const (
	BootstrapMagic uint64 = 0x424D504F4F4C3033 // BMPOOL03
	BootstrapVersion uint32 = 3
	bootstrapFormatMarkerOffset uint64 = BootstrapSize-PageSize
	BootstrapSlotA uint64 = 0
	BootstrapSlotB uint64 = PageSize
	extentMapSlotA uint64 = 2 * PageSize
	extentMapSlotSize uint64 = 2 << 20
	bootstrapCRCOffset = 128
)

var (
	ErrLegacyPoolFormat = errors.New("legacy BMPOOL02 Pool format is read-only to the dynamic layout implementation")
	ErrBootstrapUnformatted = errors.New("dynamic Pool bootstrap metadata is not formatted")
	ErrBootstrapFormatIncomplete = errors.New("Bootstrap Pool format is incomplete; retry Pool initialization under the fenced Owner Lease")
	ErrExtentMapTooLarge = errors.New("extent map exceeds reserved bootstrap slot")
	ErrStaleBootstrap = errors.New("stale Bootstrap generation")
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
	OwnerEpoch uint64
	PoolName string
	Slot uint64
}

type BootstrapLocker interface {
 Distributed() bool
 Lock(context.Context,string)(func()error,error)
}
type BootstrapOwnerLease interface {
 Epoch() uint64
 Valid() bool
}

type BootstrapStore struct {
	mu sync.Mutex
	dev device.Device
	record BootstrapRecord
	allocator *Allocator
	locker BootstrapLocker
	owner BootstrapOwnerLease
}

func (r BootstrapRecord) encode() ([]byte,error) {
	if r.Generation==0 || (r.Generation>1 && r.OwnerEpoch==0) || r.PoolUUID==([16]byte{}) || r.DeviceBytes==0 ||
		r.BootstrapBytes!=BootstrapSize || r.MetadataBytes<r.BootstrapBytes ||
		r.MetadataBytes>r.DeviceBytes || r.ExtentMapGeneration==0 ||
		(r.ExtentMapOffset!=extentMapSlotA && r.ExtentMapOffset!=extentMapSlotA+extentMapSlotSize) ||
		r.ExtentMapBytes==0 || r.ExtentMapBytes>extentMapSlotSize ||
		r.LogicalSectorBytes<512 || r.LogicalSectorBytes>65536 ||
		r.LogicalSectorBytes&(r.LogicalSectorBytes-1)!=0 || r.DeviceBytes%uint64(r.LogicalSectorBytes)!=0 || !validPoolName(r.PoolName) {
		return nil,ErrInvalidGeometry
	}
	b:=make([]byte,PageSize)
	binary.LittleEndian.PutUint64(b[0:8],BootstrapMagic)
	binary.LittleEndian.PutUint32(b[8:12],BootstrapVersion)
	binary.LittleEndian.PutUint32(b[12:16],uint32(PageSize))
	binary.LittleEndian.PutUint64(b[16:24],r.Generation)
	copy(b[24:40],r.PoolUUID[:])
	binary.LittleEndian.PutUint64(b[40:48],r.DeviceBytes)
	binary.LittleEndian.PutUint64(b[48:56],r.BootstrapBytes)
	binary.LittleEndian.PutUint64(b[56:64],r.MetadataBytes)
	binary.LittleEndian.PutUint64(b[64:72],r.ExtentMapOffset)
	binary.LittleEndian.PutUint64(b[72:80],r.ExtentMapBytes)
	binary.LittleEndian.PutUint64(b[80:88],r.ExtentMapGeneration)
	binary.LittleEndian.PutUint32(b[88:92],r.LogicalSectorBytes)
	binary.LittleEndian.PutUint64(b[96:104],r.OwnerEpoch)
	copy(b[104:128],r.PoolName)
	binary.LittleEndian.PutUint32(b[bootstrapCRCOffset:bootstrapCRCOffset+4],0)
	binary.LittleEndian.PutUint32(b[bootstrapCRCOffset:bootstrapCRCOffset+4],crc32.Checksum(b,crc32.MakeTable(crc32.Castagnoli)))
	return b,nil
}

func decodeBootstrap(b []byte,slot uint64)(BootstrapRecord,error){
	var r BootstrapRecord
	if len(b)!=int(PageSize)||binary.LittleEndian.Uint64(b[:8])!=BootstrapMagic{return r,ErrBootstrapUnformatted}
	if binary.LittleEndian.Uint32(b[8:12])!=BootstrapVersion||binary.LittleEndian.Uint32(b[12:16])!=uint32(PageSize){return r,errors.New("unsupported dynamic Pool bootstrap version")}
	c:=append([]byte(nil),b...)
	want:=binary.LittleEndian.Uint32(c[bootstrapCRCOffset:bootstrapCRCOffset+4])
	binary.LittleEndian.PutUint32(c[bootstrapCRCOffset:bootstrapCRCOffset+4],0)
	if crc32.Checksum(c,crc32.MakeTable(crc32.Castagnoli))!=want{return r,errors.New("Bootstrap Metadata CRC32C mismatch")}
	r.Generation=binary.LittleEndian.Uint64(b[16:24])
	copy(r.PoolUUID[:],b[24:40])
	r.DeviceBytes=binary.LittleEndian.Uint64(b[40:48])
	r.BootstrapBytes=binary.LittleEndian.Uint64(b[48:56])
	r.MetadataBytes=binary.LittleEndian.Uint64(b[56:64])
	r.ExtentMapOffset=binary.LittleEndian.Uint64(b[64:72])
	r.ExtentMapBytes=binary.LittleEndian.Uint64(b[72:80])
	r.ExtentMapGeneration=binary.LittleEndian.Uint64(b[80:88])
	r.LogicalSectorBytes=binary.LittleEndian.Uint32(b[88:92])
	r.OwnerEpoch=binary.LittleEndian.Uint64(b[96:104])
	nameField:=b[104:128]
	nameEnd:=bytes.IndexByte(nameField,0)
	if nameEnd<0 { return BootstrapRecord{},ErrInvalidGeometry }
	for _,v:=range nameField[nameEnd:] { if v!=0 { return BootstrapRecord{},ErrInvalidGeometry } }
	r.PoolName=string(nameField[:nameEnd])
	r.Slot=slot
 for _,v:=range b[92:96]{if v!=0{return BootstrapRecord{},ErrInvalidGeometry}}
 for _,v:=range b[bootstrapCRCOffset+4:]{if v!=0{return BootstrapRecord{},ErrInvalidGeometry}}
 if _,err:=r.encode();err!=nil{return BootstrapRecord{},err}
	return r,nil
}

func readFull(dev device.Device,b []byte,off int64)error{
	n,err:=dev.ReadAt(b,off)
	if err!=nil&&!(errors.Is(err,io.EOF)&&n==len(b)){return err}
	if n!=len(b){return io.ErrUnexpectedEOF}
	return nil
}
func writeFull(dev device.Device,b []byte,off int64)error{
	n,err:=dev.WriteAt(b,off)
	if err!=nil{return err}
	if n!=len(b){return io.ErrShortWrite}
	return nil
}
func loadExtentMap(dev device.Device,r BootstrapRecord)(ExtentMap,error){
	if r.ExtentMapBytes>extentMapSlotSize||r.ExtentMapOffset>uint64(dev.Size())||
		r.ExtentMapBytes>uint64(dev.Size())-r.ExtentMapOffset||r.ExtentMapBytes>math.MaxInt{return ExtentMap{},ErrInvalidGeometry}
	b:=make([]byte,int(r.ExtentMapBytes))
	if err:=readFull(dev,b,int64(r.ExtentMapOffset));err!=nil{return ExtentMap{},err}
	m,err:=DecodeExtentMap(b);if err!=nil{return ExtentMap{},err}
	if m.Generation!=r.ExtentMapGeneration||m.DeviceBytes!=r.DeviceBytes{return ExtentMap{},ErrInvalidGeometry}
	return m,nil
}


// validPoolName accepts an empty name only for backward compatibility with
// Bootstrap records created before Pool Catalog names were persisted.
func validPoolName(name string) bool {
	if len(name) > 23 { return false }
	for i:=0;i<len(name);i++ {
		c:=name[i]
		if !((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='.'||c=='-'||c=='_') { return false }
	}
	return true
}

// FormatBootstrap creates a new BMPOOL03 Pool. It refuses non-zero bootstrap
// pages, so it cannot overwrite BMPOOL02 or an unrecognized existing format.
// The caller must hold exclusive ownership and fence every other initiator.
func FormatBootstrap(dev device.Device,volumeSlots uint32,logicalSectorBytes uint32)(*BootstrapStore,error){
	return FormatBootstrapNamed(dev,"",volumeSlots,logicalSectorBytes)
}

// FormatBootstrapNamed persists the Pool Catalog name in both redundant
// Bootstrap records. An empty name is reserved for legacy/development callers.
func FormatBootstrapNamed(dev device.Device,poolName string,volumeSlots uint32,logicalSectorBytes uint32)(*BootstrapStore,error){
	if !validPoolName(poolName) { return nil,fmt.Errorf("invalid Pool name %q: use 1-23 ASCII letters, digits, dot, dash or underscore",poolName) }
	if dev==nil||dev.Size()<=0{return nil,ErrInvalidGeometry}
	deviceBytes:=uint64(dev.Size())
	if block,ok:=dev.(interface{LogicalSectorSize()int});ok{
		actual:=block.LogicalSectorSize()
		if actual<=0{return nil,ErrInvalidGeometry}
		if logicalSectorBytes!=0&&logicalSectorBytes!=uint32(actual){return nil,fmt.Errorf("configured logical sector size %d differs from device %d",logicalSectorBytes,actual)}
		logicalSectorBytes=uint32(actual)
	}
	if logicalSectorBytes==0{logicalSectorBytes=512}
	if logicalSectorBytes<512||logicalSectorBytes>65536||logicalSectorBytes&(logicalSectorBytes-1)!=0||deviceBytes%uint64(logicalSectorBytes)!=0{return nil,ErrInvalidGeometry}

	marker:=make([]byte,PageSize)
	if err:=readFull(dev,marker,int64(bootstrapFormatMarkerOffset));err!=nil{return nil,fmt.Errorf("inspect Bootstrap format marker: %w",err)}
	var r BootstrapRecord
	var allocator *Allocator
	resume:=false
	if !bytes.Equal(marker,make([]byte,PageSize)) {
		existing,err:=decodeBootstrap(marker,BootstrapSlotB)
		if err!=nil{return nil,fmt.Errorf("%w: format marker is occupied or corrupt",ErrBootstrapFormatIncomplete)}
		if existing.PoolName!=poolName||existing.DeviceBytes!=deviceBytes||existing.LogicalSectorBytes!=logicalSectorBytes {
			return nil,fmt.Errorf("%w: marker belongs to Pool %q with different device geometry",ErrBootstrapFormatIncomplete,existing.PoolName)
		}
		r=existing
		plan,err:=PlanDevice(deviceBytes,volumeSlots);if err!=nil{return nil,err}
		if r.MetadataBytes!=BootstrapSize+plan.InitialMetadata{return nil,fmt.Errorf("%w: requested volume slot geometry differs from format marker",ErrBootstrapFormatIncomplete)}
		allocator,err=NewAllocator(deviceBytes,BootstrapSize,plan.InitialMetadata);if err!=nil{return nil,err}
		resume=true
	} else {
		for _,off:=range []uint64{BootstrapSlotA,BootstrapSlotB}{
			p:=make([]byte,PageSize)
			if err:=readFull(dev,p,int64(off));err!=nil{return nil,fmt.Errorf("inspect bootstrap page at %d: %w",off,err)}
			if binary.LittleEndian.Uint64(p[:8])==0x424D504F4F4C3032{return nil,ErrLegacyPoolFormat}
			if !bytes.Equal(p,make([]byte,PageSize)){return nil,errors.New("refusing to format a non-empty or unknown device")}
		}
		for off:=uint64(2*PageSize);off<BootstrapSize;off+=PageSize{
			if off==bootstrapFormatMarkerOffset{continue}
			p:=make([]byte,PageSize)
			if err:=readFull(dev,p,int64(off));err!=nil{return nil,fmt.Errorf("inspect reserved Bootstrap page at %d: %w",off,err)}
			if !bytes.Equal(p,make([]byte,PageSize)){return nil,fmt.Errorf("refusing to overwrite non-zero reserved Bootstrap page at %d",off)}
		}
		plan,err:=PlanDevice(deviceBytes,volumeSlots);if err!=nil{return nil,err}
		allocator,err=NewAllocator(deviceBytes,BootstrapSize,plan.InitialMetadata);if err!=nil{return nil,err}
		var uuid [16]byte
		if _,err=io.ReadFull(rand.Reader,uuid[:]);err!=nil{return nil,err}
		r=BootstrapRecord{Generation:1,PoolUUID:uuid,DeviceBytes:deviceBytes,BootstrapBytes:BootstrapSize,
			MetadataBytes:BootstrapSize+plan.InitialMetadata,ExtentMapOffset:extentMapSlotA,
			ExtentMapGeneration:1,LogicalSectorBytes:logicalSectorBytes,PoolName:poolName}
	}
	m:=ExtentMap{Generation:r.ExtentMapGeneration,DeviceBytes:r.DeviceBytes,Extents:allocator.Extents()}
	encoded,err:=m.Encode();if err!=nil{return nil,err}
	if uint64(len(encoded))>extentMapSlotSize{return nil,ErrExtentMapTooLarge}
	r.ExtentMapBytes=uint64(len(encoded))
	markerPage,err:=r.encode();if err!=nil{return nil,err}
	if !resume {
		if err=writeFull(dev,markerPage,int64(bootstrapFormatMarkerOffset));err!=nil{return nil,fmt.Errorf("write Bootstrap format intent: %w",err)}
		if err=dev.Sync();err!=nil{return nil,fmt.Errorf("persist Bootstrap format intent: %w",err)}
	}
	if err=writeFull(dev,encoded,int64(r.ExtentMapOffset));err!=nil{return nil,fmt.Errorf("write initial Bootstrap Extent Map: %w",err)}
	if err=dev.Sync();err!=nil{return nil,fmt.Errorf("persist initial Bootstrap Extent Map: %w",err)}
	if err=writeFull(dev,markerPage,int64(BootstrapSlotB));err!=nil{return nil,fmt.Errorf("publish initial Bootstrap record: %w",err)}
	if err=dev.Sync();err!=nil{return nil,fmt.Errorf("persist initial Bootstrap record: %w",err)}
	if err=writeFull(dev,make([]byte,PageSize),int64(bootstrapFormatMarkerOffset));err!=nil{return nil,fmt.Errorf("clear Bootstrap format intent: %w",err)}
	if err=dev.Sync();err!=nil{return nil,fmt.Errorf("persist Bootstrap format completion: %w",err)}
	r.Slot=BootstrapSlotB
	return &BootstrapStore{dev:dev,record:r,allocator:allocator},nil
}

// OpenBootstrap replays the one-record Bootstrap WAL and chooses the highest
// valid record whose extent-map root validates.
func OpenBootstrap(dev device.Device)(*BootstrapStore,error){
	if dev==nil{return nil,ErrInvalidGeometry}
	marker:=make([]byte,PageSize)
	if err:=readFull(dev,marker,int64(bootstrapFormatMarkerOffset));err!=nil{return nil,err}
	if !bytes.Equal(marker,make([]byte,PageSize)){return nil,ErrBootstrapFormatIncomplete}
	blockedSlot,walErr:=recoverBootstrapWAL(dev)
	if walErr!=nil{return nil,fmt.Errorf("recover Bootstrap WAL: %w",walErr)}
	type candidate struct{r BootstrapRecord;m ExtentMap}
	var candidates []candidate
	legacy:=false
	for _,slot:=range []uint64{BootstrapSlotA,BootstrapSlotB}{
		if slot==blockedSlot{continue}
		b:=make([]byte,PageSize)
		if readFull(dev,b,int64(slot))==nil&&binary.LittleEndian.Uint64(b[:8])==0x424D504F4F4C3032{legacy=true}
		r,err:=decodeBootstrap(b,slot);if err!=nil{continue}
		m,err:=loadExtentMap(dev,r);if err!=nil{continue}
		candidates=append(candidates,candidate{r,m})
	}
	if len(candidates)==0{if legacy{return nil,ErrLegacyPoolFormat};return nil,ErrBootstrapUnformatted}
	best:=candidates[0]
	for _,c:=range candidates[1:]{
		if c.r.PoolUUID!=best.r.PoolUUID{return nil,errors.New("Bootstrap copies refer to different Pool UUIDs")}
		if c.r.Generation==best.r.Generation&&c.r!=best.r{return nil,errors.New("conflicting Bootstrap copies at equal generation")}
		if c.r.Generation>best.r.Generation{if c.r.OwnerEpoch<best.r.OwnerEpoch{return nil,errors.New("Bootstrap OwnerEpoch regressed across generations")};best=c}
	}
	if uint64(dev.Size())<best.r.DeviceBytes{return nil,errors.New("backing device is smaller than persisted Pool geometry; refusing shrink")}
 if block,ok:=dev.(interface{LogicalSectorSize()int});ok&&uint32(block.LogicalSectorSize())!=best.r.LogicalSectorBytes{return nil,errors.New("backing device logical sector size differs from persisted Pool geometry")}
	a,err:=allocatorFromMap(best.m);if err!=nil{return nil,err}
	return &BootstrapStore{dev:dev,record:best.r,allocator:a},nil
}
func allocatorFromMap(m ExtentMap)(*Allocator,error){
	if len(m.Extents)==0||m.Extents[0].Start!=0||m.Extents[0].Length<BootstrapSize||m.Extents[0].Kind!=Metadata{return nil,ErrInvalidGeometry}
	return &Allocator{deviceBytes:m.DeviceBytes,extents:append([]Extent(nil),m.Extents...)},nil
}
func(s *BootstrapStore)Record()BootstrapRecord{if s==nil{return BootstrapRecord{}};s.mu.Lock();defer s.mu.Unlock();return s.record}
func(s *BootstrapStore)Extents()[]Extent{if s==nil||s.allocator==nil{return nil};s.mu.Lock();defer s.mu.Unlock();return s.allocator.Extents()}

// OpenDataView returns a logical Data-only view over the Pool's fragmented
// Data extents. Metadata and Data still share the same parent LUN.
func(s *BootstrapStore)OpenDataView()(*DataView,error){
 if s==nil||s.dev==nil||s.allocator==nil{return nil,ErrInvalidGeometry}
 s.mu.Lock();defer s.mu.Unlock()
 var dataExtents []Extent
 for _,e:=range s.allocator.extents{if e.Kind==Data{dataExtents=append(dataExtents,e)}}
 v,err:=NewDataView(s.dev,dataExtents);if err!=nil{return nil,err};v.generation=s.record.Generation;return v,nil
}

// OpenMetadataView exposes Metadata extents as a logical device while keeping
// the Bootstrap reservation addressable only through the parent Pool device.
func(s *BootstrapStore)OpenMetadataView()(*MetadataView,error){
 if s==nil||s.dev==nil||s.allocator==nil{return nil,ErrInvalidGeometry}
 s.mu.Lock();defer s.mu.Unlock()
 var extents []Extent
 for _,e:=range s.allocator.extents{if e.Kind==Metadata{extents=append(extents,e)}}
 v,err:=NewMetadataView(s.dev,extents);if err!=nil{return nil,err};v.generation=s.record.Generation;return v,nil
}

// OpenViews snapshots both logical address spaces from one Extent Map
// generation. Reopen these views after any Pool geometry update.
func(s *BootstrapStore)OpenViews()(*MetadataView,*DataView,uint64,error){
 if s==nil||s.dev==nil||s.allocator==nil{return nil,nil,0,ErrInvalidGeometry}
 s.mu.Lock();defer s.mu.Unlock()
 var metadataExtents,dataExtents []Extent
 for _,e:=range s.allocator.extents{
  switch e.Kind{case Metadata:metadataExtents=append(metadataExtents,e);case Data:dataExtents=append(dataExtents,e)}
 }
 metadata,err:=NewMetadataView(s.dev,metadataExtents);if err!=nil{return nil,nil,0,err}
 var data *DataView
 if len(dataExtents)>0{data,err=NewDataView(s.dev,dataExtents);if err!=nil{return nil,nil,0,err}}
 metadata.generation=s.record.Generation
 if data!=nil{data.generation=s.record.Generation}
 return metadata,data,s.record.Generation,nil
}

// AllocateDataExtents reserves whole 4MiB-aligned physical ranges for the
// Pool's logical Data address space. It publishes the Extent Map before
// returning. Per-block allocation remains governed by the AG Bitmap WAL.
func(s *BootstrapStore)AllocateDataExtents(bytes uint64)([]Extent,error){
 if s==nil||s.allocator==nil||bytes==0||bytes%AllocationSize!=0{return nil,ErrInvalidGeometry}
 s.mu.Lock();defer s.mu.Unlock()
 next:=&Allocator{deviceBytes:s.allocator.deviceBytes,extents:s.allocator.Extents()}
 allocated,err:=next.AllocateData(bytes);if err!=nil{return nil,err}
 if err=s.persist(next,s.record.DeviceBytes);err!=nil{return nil,err}
 return allocated,nil
}


// OpenExtentView returns a bounded non-owning view over one physical extent.
// Metadata and Data views always share the same parent Device; closing a view
// never closes the underlying LUN.
func(s *BootstrapStore)OpenExtentView(index int)(*device.Range,error){
 if s==nil||s.dev==nil{return nil,ErrInvalidGeometry}
 s.mu.Lock();defer s.mu.Unlock()
 if index<0||index>=len(s.allocator.extents){return nil,ErrInvalidGeometry}
 e:=s.allocator.extents[index]
 return device.NewRange(s.dev,e.Start,e.Length)
}


// SetCommitLocker installs the distributed SAN lock required for Bootstrap
// updates. A host-local flock does not satisfy this interface.
func(s *BootstrapStore)SetCommitLocker(l BootstrapLocker)error{
	if s==nil||l==nil||!l.Distributed(){return errors.New("a distributed SAN Bootstrap locker is required")}
	s.mu.Lock();defer s.mu.Unlock();s.locker=l;return nil
}

// SetOwnerLease binds metadata publication to the current fenced SAN Owner.
func(s *BootstrapStore)SetOwnerLease(owner BootstrapOwnerLease)error{
 if s==nil||owner==nil||!owner.Valid()||owner.Epoch()==0{return errors.New("a live fenced Pool Owner lease with non-zero epoch is required")}
 s.mu.Lock();defer s.mu.Unlock()
 if owner.Epoch()<s.record.OwnerEpoch{return errors.New("Pool Owner lease epoch is older than Bootstrap OwnerEpoch")}
 s.owner=owner
 return nil
}

func metadataExtentBytes(extents []Extent)uint64{
	var n uint64
	for _,e:=range extents{if e.Kind==Metadata{n+=e.Length}}
	return n
}

// persist writes the new ExtentMap, WAL Intent, inactive Bootstrap after-image,
// then the WAL Commit. Recovery ignores an inactive after-image without Commit.
func(s *BootstrapStore)persist(next *Allocator,deviceBytes uint64)error{
 if s==nil||s.dev==nil||next==nil{return ErrInvalidGeometry}
 if s.locker==nil||!s.locker.Distributed(){return errors.New("distributed SAN Bootstrap lock is required")}
 if s.owner==nil||!s.owner.Valid()||s.owner.Epoch()==0||s.owner.Epoch()<s.record.OwnerEpoch{return errors.New("live fenced Pool Owner lease/Epoch is required")}
 ownerEpoch:=s.owner.Epoch()
 ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second);defer cancel()
 resource:=fmt.Sprintf("blockmesh:pool:%x:bootstrap",s.record.PoolUUID)
 release,err:=s.locker.Lock(ctx,resource);if err!=nil{return fmt.Errorf("acquire Pool Bootstrap lock: %w",err)}
 current,openErr:=OpenBootstrap(s.dev)
 if openErr!=nil{_ = release();return openErr}
 if current.record.Generation!=s.record.Generation||current.record.PoolUUID!=s.record.PoolUUID{_ = release();return ErrStaleBootstrap}
 if !s.owner.Valid()||s.owner.Epoch()!=ownerEpoch||ownerEpoch<s.record.OwnerEpoch{_ = release();return errors.New("Pool Owner lease lost before Bootstrap update")}
 if s.record.Generation==math.MaxUint64||s.record.ExtentMapGeneration==math.MaxUint64{_ = release();return errors.New("Bootstrap generation exhausted")}
 mapSlot:=extentMapSlotA
 if s.record.ExtentMapOffset==extentMapSlotA{mapSlot=extentMapSlotA+extentMapSlotSize}
 m:=ExtentMap{Generation:s.record.ExtentMapGeneration+1,DeviceBytes:deviceBytes,Extents:next.Extents()}
 encoded,err:=m.Encode()
 if err==nil&&uint64(len(encoded))>extentMapSlotSize{err=ErrExtentMapTooLarge}
 if err==nil{err=writeFull(s.dev,encoded,int64(mapSlot))}
 if err==nil{err=s.dev.Sync()}
 if err==nil&&(!s.owner.Valid()||s.owner.Epoch()!=ownerEpoch){err=errors.New("Pool Owner lease lost before Bootstrap Intent")}
 r:=s.record
 inactive:=BootstrapSlotA
 if s.record.Slot==BootstrapSlotA{inactive=BootstrapSlotB}
 var sb []byte
 var intent bootstrapIntent
 if err==nil{
  r.Generation++;r.DeviceBytes=deviceBytes;r.MetadataBytes=metadataExtentBytes(next.Extents())
  r.ExtentMapOffset=mapSlot;r.ExtentMapBytes=uint64(len(encoded));r.ExtentMapGeneration=m.Generation
  r.OwnerEpoch=ownerEpoch
  sb,err=r.encode()
 }
 if err==nil{
  var intentPage []byte
  intentPage,intent,err=encodeBootstrapIntent(r.Generation,inactive,sb)
  if err==nil{err=writeFull(s.dev,intentPage,int64(bootstrapWALIntentOffset))}
  if err==nil{err=s.dev.Sync()}
 }
 if err==nil&&(!s.owner.Valid()||s.owner.Epoch()!=ownerEpoch){err=errors.New("Pool Owner lease lost before Bootstrap after-image")}
 if err==nil{err=writeFull(s.dev,sb,int64(inactive))}
 if err==nil{err=s.dev.Sync()}
 if err==nil&&(!s.owner.Valid()||s.owner.Epoch()!=ownerEpoch){err=errors.New("Pool Owner lease lost before Bootstrap Commit")}
 if err==nil{err=writeFull(s.dev,encodeBootstrapCommit(intent),int64(bootstrapWALCommitOffset))}
 if err==nil{err=s.dev.Sync()}
 if err==nil{r.Slot=inactive;s.record,s.allocator=r,next}
 releaseErr:=release()
 if err!=nil{return err}
 if releaseErr!=nil{return releaseErr}
 return nil
}

// Rescan adds newly visible capacity as Free extents only. It never grows
// Metadata automatically and rejects device shrink.
func(s *BootstrapStore)Rescan()(uint64,error){
	if s==nil||s.dev==nil{return 0,ErrInvalidGeometry}
	s.mu.Lock();defer s.mu.Unlock()
	actual:=uint64(s.dev.Size())
	if r,ok:=s.dev.(interface{Rescan()(uint64,error)});ok{var err error;actual,err=r.Rescan();if err!=nil{return s.record.DeviceBytes,err}}
	if actual<s.record.DeviceBytes{return s.record.DeviceBytes,fmt.Errorf("device shrink from %d to %d is not supported",s.record.DeviceBytes,actual)}
 if block,ok:=s.dev.(interface{LogicalSectorSize()int});ok&&uint32(block.LogicalSectorSize())!=s.record.LogicalSectorBytes{return s.record.DeviceBytes,errors.New("logical sector size changed; refusing Pool rescan")}
	if actual==s.record.DeviceBytes{return actual,nil}
	next:=&Allocator{deviceBytes:s.allocator.deviceBytes,extents:s.allocator.Extents()}
	if err:=next.Rescan(actual);err!=nil{return s.record.DeviceBytes,err}
	if err:=s.persist(next,actual);err!=nil{return s.record.DeviceBytes,err}
	return actual,nil
}

// ExpandMetadata accepts usage and forecast observations, never a target size.
func(s *BootstrapStore)ExpandMetadata(usedBytes,growthBytes uint64)(uint64,error){
	if s==nil||s.allocator==nil{return 0,ErrInvalidGeometry}
	s.mu.Lock();defer s.mu.Unlock()
	currentTotal:=metadataExtentBytes(s.allocator.Extents())
	if currentTotal<s.record.BootstrapBytes{return 0,ErrInvalidGeometry}
	current:=currentTotal-s.record.BootstrapBytes
	target,err:=MetadataExpansionTarget(current,usedBytes,growthBytes,ExtentSize);if err!=nil{return currentTotal,err}
	next:=&Allocator{deviceBytes:s.allocator.deviceBytes,extents:s.allocator.Extents()}
	if _,err=next.AllocateMetadata(target-current);err!=nil{return currentTotal,err}
	if err=s.persist(next,s.record.DeviceBytes);err!=nil{return currentTotal,err}
	return target+s.record.BootstrapBytes,nil
}
