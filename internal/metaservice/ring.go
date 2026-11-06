package metaservice

import (
 "context"
 "encoding/binary"
 "errors"
 "fmt"
 "sync/atomic"
 "unsafe"
 "blockmesh/internal/uapi"
)

var (
 ErrQueueGeneration=errors.New("queue generation mismatch")
 ErrRingCorrupt=errors.New("shared ring indices or layout are corrupt")
 ErrMalformedRequest=errors.New("malformed request without usable correlation identity")
)
type QueueMemory struct {
 Info uapi.SetupQueue
 RequestControl,RequestRing,CompletionControl,CompletionRing,Payload []byte
}
func idx(b []byte,o int)*uint32{return (*uint32)(unsafe.Pointer(&b[o]))}
func(q *QueueMemory) validate()error{
 if e:=q.Info.Validate();e!=nil{return e}
 var endian uint16=1
 if *(*byte)(unsafe.Pointer(&endian))!=1{return errors.New("shared ring requires little-endian host")}
 l,e:=uapi.Layout(q.Info.RingOrder);if e!=nil{return e}
 if len(q.RequestControl)<4096||len(q.CompletionControl)<4096||
  uint64(len(q.RequestRing))<uint64(l.Entries)*uapi.RequestSize||
  uint64(len(q.CompletionRing))<uint64(l.Entries)*uapi.CompletionSize||
  uint64(len(q.Payload))<l.PayloadBytes{return errors.New("mapped queue region shorter than ABI layout")}
 for _,b:=range [][]byte{q.RequestControl,q.CompletionControl}{
  if uintptr(unsafe.Pointer(&b[0]))%4!=0{return errors.New("unaligned ring control")}
  if binary.LittleEndian.Uint32(b[8:12])!=l.Entries||binary.LittleEndian.Uint64(b[16:24])!=q.Info.QueueGeneration{return ErrQueueGeneration}
 }
 return nil
}

// Drain reserves Completion Ring capacity before executing a request. Atomics
// are stronger than acquire/release and preserve the SPSC publication contract.
func(q *QueueMemory) Drain(ctx context.Context,p Processor,limit int)(consumed,produced int,err error){
 if p==nil||limit<=0{return 0,0,errors.New("processor and positive drain limit are required")}
 if err=q.validate();err!=nil{return}
 n:=uint32(1)<<q.Info.RingOrder;mask:=n-1
 for consumed<limit{
  if err=ctx.Err();err!=nil{return}
  rh:=atomic.LoadUint32(idx(q.RequestControl,0));rt:=atomic.LoadUint32(idx(q.RequestControl,4))
  if rt-rh>n{err=ErrRingCorrupt;return};if rh==rt{return}
  ch:=atomic.LoadUint32(idx(q.CompletionControl,0));ct:=atomic.LoadUint32(idx(q.CompletionControl,4))
  if ct-ch>n{err=ErrRingCorrupt;return};if ct-ch==n{return}
  rs:=rh&mask;raw:=make([]byte,uapi.RequestSize)
  copy(raw,q.RequestRing[int(rs)*uapi.RequestSize:int(rs+1)*uapi.RequestSize])
  req,de:=uapi.UnmarshalRequest(raw)
  if req.RequestID==0||req.QueueGeneration!=q.Info.QueueGeneration{
   err=fmt.Errorf("%w: request=%d generation=%d queue=%d",ErrMalformedRequest,req.RequestID,req.QueueGeneration,q.Info.QueueGeneration);return
  }
  var result Result;var pe error
  if de!=nil{pe=ErrInvalid}else{result,pe=p.Process(ctx,req)}
  code:=resultCode(pe);if pe!=nil{result=Result{}}
  if code==uapi.CodeOK{
   switch req.Operation{
   case uapi.OpLookupBatch,uapi.OpAllocateZeroPublish:
    if result.Mapping==nil||result.Mapping.LogicalBlock!=req.LogicalStart||result.Mapping.BlockCount!=req.BlockCount{code=uapi.CodeInternal;result=Result{}}
    if code==uapi.CodeOK{if _,e:=result.Mapping.MarshalBinary();e!=nil{code=uapi.CodeInternal;result=Result{}}}
   case uapi.OpDiscard,uapi.OpFlush:
    if result.Mapping!=nil{code=uapi.CodeInternal;result=Result{}}
   default:code=uapi.CodeInvalid;result=Result{}
   }
  }
  cs:=ct&mask;po,mc,pb:=uint32(0),uint32(0),uint32(0)
  if code==uapi.CodeOK&&result.Mapping!=nil{
   po=cs*uapi.MaxBatch*uapi.MappingSize
   enc,e:=result.Mapping.MarshalBinary();if e!=nil{err=e;return}
   off:=int(po);copy(q.Payload[off:off+uapi.MappingSize],enc);mc,pb=1,uapi.MappingSize
  }
  c:=uapi.Completion{Code:code,RequestID:req.RequestID,QueueGeneration:req.QueueGeneration,
   OwnerEpoch:req.OwnerEpoch,RootGeneration:req.RootGeneration,PayloadOffset:po,MappingCount:mc,PayloadBytes:pb}
  enc,e:=c.MarshalBinary();if e!=nil{err=e;return}
  off:=int(cs)*uapi.CompletionSize;copy(q.CompletionRing[off:off+uapi.CompletionSize],enc)
  // Completion is visible before request head acknowledges consumption.
  atomic.StoreUint32(idx(q.CompletionControl,4),ct+1)
  atomic.StoreUint32(idx(q.RequestControl,0),rh+1)
  consumed++;produced++
 }
 return
}
