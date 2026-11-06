package uapi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	RequestControlOffset uint64 = 0x00000000
	RequestRingOffset uint64 = 0x00010000
	CompletionControlOffset uint64 = 0x00040000
	CompletionRingOffset uint64 = 0x00050000
	MappingPayloadOffset uint64 = 0x00060000
	PageSize uint64 = 4096
	SetupSize = 80
	CancelSize = 40
	ABIInfoSize = 64
)

type QueueLayout struct {
	Order uint32
	Entries uint32
	RequestControl, RequestRing uint64
	CompletionControl, CompletionRing uint64
	Payload uint64
	PayloadBytes uint64
	TotalBytes uint64
}
func alignPage(n uint64) uint64 { return (n + PageSize - 1) & ^(PageSize - 1) }
func Layout(order uint32) (QueueLayout, error) {
	if order < MinRingOrder || order > MaxRingOrder { return QueueLayout{}, fmt.Errorf("ring order must be in [%d,%d]", MinRingOrder, MaxRingOrder) }
	entries := uint32(1) << order
	if RequestRingOffset+uint64(entries)*RequestSize > CompletionControlOffset || CompletionRingOffset+uint64(entries)*CompletionSize > MappingPayloadOffset {
		return QueueLayout{}, errors.New("ring regions overlap")
	}
	payloadBytes := uint64(entries)*MaxBatch*MappingSize
	return QueueLayout{Order:order, Entries:entries, RequestControl:RequestControlOffset, RequestRing:RequestRingOffset,
		CompletionControl:CompletionControlOffset, CompletionRing:CompletionRingOffset, Payload:MappingPayloadOffset,
		PayloadBytes:payloadBytes, TotalBytes:alignPage(MappingPayloadOffset+payloadBytes)}, nil
}

type SetupQueue struct {
	RingOrder uint32
	Flags uint32
	QueueGeneration uint64
	RequestControlOffset, RequestRingOffset uint64
	CompletionControlOffset, CompletionRingOffset, PayloadOffset uint64
	QueueID uint32
	MaxBatch uint32
}
func (s SetupQueue) Validate() error {
	l,err:=Layout(s.RingOrder); if err!=nil{return err}
	if s.QueueGeneration==0{return errors.New("queue generation must be non-zero")}; if s.QueueID==0{return errors.New("queue ID must be non-zero")}
	if s.RequestControlOffset!=EncodeMmapOffset(s.QueueID,l.RequestControl) || s.RequestRingOffset!=EncodeMmapOffset(s.QueueID,l.RequestRing) || s.CompletionControlOffset!=EncodeMmapOffset(s.QueueID,l.CompletionControl) || s.CompletionRingOffset!=EncodeMmapOffset(s.QueueID,l.CompletionRing) || s.PayloadOffset!=EncodeMmapOffset(s.QueueID,l.Payload){return errors.New("kernel returned unexpected mmap offsets")}
	if s.MaxBatch!=MaxBatch{return errors.New("kernel max batch mismatch")}
	return nil
}
func (s SetupQueue) MarshalBinary() ([]byte,error) {
	if err:=s.Validate();err!=nil{return nil,err}
	b:=make([]byte,SetupSize)
	binary.LittleEndian.PutUint32(b[0:4],SetupSize);binary.LittleEndian.PutUint32(b[4:8],uint32(Version))
	binary.LittleEndian.PutUint32(b[8:12],s.RingOrder);binary.LittleEndian.PutUint32(b[12:16],s.Flags)
	binary.LittleEndian.PutUint64(b[16:24],s.QueueGeneration)
	binary.LittleEndian.PutUint64(b[24:32],s.RequestControlOffset);binary.LittleEndian.PutUint64(b[32:40],s.RequestRingOffset)
	binary.LittleEndian.PutUint64(b[40:48],s.CompletionControlOffset);binary.LittleEndian.PutUint64(b[48:56],s.CompletionRingOffset)
	binary.LittleEndian.PutUint64(b[56:64],s.PayloadOffset);binary.LittleEndian.PutUint32(b[64:68],s.MaxBatch); binary.LittleEndian.PutUint32(b[68:72],s.QueueID)
	return b,nil
}
func UnmarshalSetupQueue(b []byte)(SetupQueue,error){
	var s SetupQueue
	if len(b)!=SetupSize{return s,fmt.Errorf("setup size %d, want %d",len(b),SetupSize)}
	if binary.LittleEndian.Uint32(b[:4])!=SetupSize||binary.LittleEndian.Uint32(b[4:8])!=uint32(Version){return s,errors.New("setup ABI size or version mismatch")}
	for _,v:=range b[72:80]{if v!=0{return s,errors.New("setup reserved bytes are non-zero")}}
	s.RingOrder=binary.LittleEndian.Uint32(b[8:12]);s.Flags=binary.LittleEndian.Uint32(b[12:16]);s.QueueGeneration=binary.LittleEndian.Uint64(b[16:24])
	s.RequestControlOffset=binary.LittleEndian.Uint64(b[24:32]);s.RequestRingOffset=binary.LittleEndian.Uint64(b[32:40])
	s.CompletionControlOffset=binary.LittleEndian.Uint64(b[40:48]);s.CompletionRingOffset=binary.LittleEndian.Uint64(b[48:56])
	s.PayloadOffset=binary.LittleEndian.Uint64(b[56:64]);s.MaxBatch=binary.LittleEndian.Uint32(b[64:68]);s.QueueID=binary.LittleEndian.Uint32(b[68:72])
	return s,s.Validate()
}

type CancelRequest struct{RequestID,QueueGeneration uint64;Flags uint32}
func(c CancelRequest)MarshalBinary()([]byte,error){
	if c.RequestID==0||c.QueueGeneration==0{return nil,errors.New("cancel requires request ID and queue generation")}
	b:=make([]byte,CancelSize);binary.LittleEndian.PutUint32(b[:4],CancelSize);binary.LittleEndian.PutUint32(b[4:8],c.Flags)
	binary.LittleEndian.PutUint64(b[8:16],c.RequestID);binary.LittleEndian.PutUint64(b[16:24],c.QueueGeneration);return b,nil
}
func UnmarshalCancelRequest(b []byte)(CancelRequest,error){
	var c CancelRequest
	if len(b)!=CancelSize||binary.LittleEndian.Uint32(b[:4])!=CancelSize{return c,errors.New("cancel size mismatch")}
	for _,v:=range b[24:40]{if v!=0{return c,errors.New("cancel reserved bytes are non-zero")}}
	c.Flags=binary.LittleEndian.Uint32(b[4:8]);c.RequestID=binary.LittleEndian.Uint64(b[8:16]);c.QueueGeneration=binary.LittleEndian.Uint64(b[16:24])
	if c.RequestID==0||c.QueueGeneration==0{return c,errors.New("cancel requires request ID and queue generation")};return c,nil
}
type ABIInfo struct{Features uint64}
func(a ABIInfo)MarshalBinary()[]byte{
	b:=make([]byte,ABIInfoSize)
	for i,v:=range []uint32{ABIInfoSize,uint32(Version),RequestSize,CompletionSize,MappingSize,RingControlSize,MaxBatch,MaxRingOrder}{binary.LittleEndian.PutUint32(b[i*4:i*4+4],v)}
	binary.LittleEndian.PutUint64(b[32:40],a.Features);return b
}
func UnmarshalABIInfo(b []byte)(ABIInfo,error){
	var a ABIInfo
	if len(b)!=ABIInfoSize||binary.LittleEndian.Uint32(b[:4])!=ABIInfoSize{return a,errors.New("ABI info size mismatch")}
	for i,v:=range []uint32{uint32(Version),RequestSize,CompletionSize,MappingSize,RingControlSize,MaxBatch,MaxRingOrder}{if binary.LittleEndian.Uint32(b[(i+1)*4:(i+2)*4])!=v{return a,fmt.Errorf("ABI field %d mismatch",i+1)}}
	a.Features=binary.LittleEndian.Uint64(b[32:40]);for _,v:=range b[40:64]{if v!=0{return a,errors.New("ABI info reserved bytes are non-zero")}};return a,nil
}

type pendingState uint8
const(pendingActive pendingState=iota;pendingCancelled)
type requestKey struct{generation,id uint64}
type Correlator struct{mu sync.Mutex;generation uint64;pending map[requestKey]pendingState}
func NewCorrelator(generation uint64)(*Correlator,error){if generation==0{return nil,errors.New("queue generation must be non-zero")};return &Correlator{generation:generation,pending:make(map[requestKey]pendingState)},nil}
func(c *Correlator)Register(id,generation uint64)error{
	c.mu.Lock();defer c.mu.Unlock();if generation!=c.generation||id==0{return errors.New("request belongs to invalid queue generation or has zero ID")}
	k:=requestKey{generation,id};if _,ok:=c.pending[k];ok{return errors.New("duplicate in-flight request ID")};c.pending[k]=pendingActive;return nil
}
func(c *Correlator)Cancel(id,generation uint64)bool{
	c.mu.Lock();defer c.mu.Unlock();k:=requestKey{generation,id};s,ok:=c.pending[k];if !ok||generation!=c.generation||s==pendingCancelled{return false};c.pending[k]=pendingCancelled;return true
}
func(c *Correlator)Complete(done Completion)bool{
	c.mu.Lock();defer c.mu.Unlock();k:=requestKey{done.QueueGeneration,done.RequestID};s,ok:=c.pending[k]
	if !ok||done.QueueGeneration!=c.generation||s!=pendingActive{return false};delete(c.pending,k);return true
}
func(c *Correlator)Reset(generation uint64)error{
	c.mu.Lock();defer c.mu.Unlock();if generation==0||generation==c.generation{return errors.New("reset requires a different non-zero generation")}
	c.generation=generation;c.pending=make(map[requestKey]pendingState);return nil
}
func(c *Correlator)Pending()int{c.mu.Lock();defer c.mu.Unlock();return len(c.pending)}
