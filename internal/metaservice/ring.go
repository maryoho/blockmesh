package metaservice
import("context";"encoding/binary";"errors";"fmt";"sync";"sync/atomic";"time";"unsafe";"blockmesh/internal/uapi")
var(ErrQueueGeneration=errors.New("queue generation mismatch");ErrRingCorrupt=errors.New("shared ring indices or layout are corrupt");ErrMalformedRequest=errors.New("malformed request without usable correlation identity"))
const DefaultRequestTimeout = 30 * time.Second
type requestIdentity struct { generation, id uint64 }
type QueueMemory struct {
 Info uapi.SetupQueue
 RequestControl,RequestRing,CompletionControl,CompletionRing,Payload []byte
 RequestTimeout time.Duration
 CancelNotify func(requestID,generation uint64) error
 activeMu sync.Mutex
 active map[requestIdentity]context.CancelFunc
}
func (q *QueueMemory) beginRequest(id,generation uint64,cancel context.CancelFunc) bool {
 if id==0 || generation!=q.Info.QueueGeneration { return false }
 q.activeMu.Lock(); defer q.activeMu.Unlock()
 if q.active==nil { q.active=make(map[requestIdentity]context.CancelFunc) }
 key:=requestIdentity{generation:generation,id:id}
 if _,ok:=q.active[key];ok { return false }
 q.active[key]=cancel
 return true
}
func (q *QueueMemory) endRequest(id,generation uint64) {
 q.activeMu.Lock(); delete(q.active,requestIdentity{generation:generation,id:id}); q.activeMu.Unlock()
}
func (q *QueueMemory) notifyCancel(id,generation uint64) error {
 if q.CancelNotify==nil { return nil }
 return q.CancelNotify(id,generation)
}
func (q *QueueMemory) Cancel(id,generation uint64) bool {
 q.activeMu.Lock(); cancel,ok:=q.active[requestIdentity{generation:generation,id:id}]
 if ok && generation==q.Info.QueueGeneration { cancel() } else { ok=false }
 q.activeMu.Unlock()
 return ok
}
func idx(b []byte,o int)*uint32{return (*uint32)(unsafe.Pointer(&b[o]))}
func(q *QueueMemory)validate()error{if e:=q.Info.Validate();e!=nil{return e};var endian uint16=1;if *(*byte)(unsafe.Pointer(&endian))!=1{return errors.New("shared ring requires little-endian host")};l,e:=uapi.Layout(q.Info.RingOrder);if e!=nil{return e};if len(q.RequestControl)<4096||len(q.CompletionControl)<4096||uint64(len(q.RequestRing))<uint64(l.Entries)*uapi.RequestSize||uint64(len(q.CompletionRing))<uint64(l.Entries)*uapi.CompletionSize||uint64(len(q.Payload))<l.PayloadBytes{return errors.New("mapped queue region shorter than ABI layout")};for _,b:=range [][]byte{q.RequestControl,q.CompletionControl}{if uintptr(unsafe.Pointer(&b[0]))%4!=0{return errors.New("unaligned ring control")};if binary.LittleEndian.Uint32(b[8:12])!=l.Entries||binary.LittleEndian.Uint64(b[16:24])!=q.Info.QueueGeneration{return ErrQueueGeneration}};return nil}
func(q *QueueMemory)Drain(ctx context.Context,p Processor,limit int)(consumed,produced int,err error){if p==nil||limit<=0{return 0,0,errors.New("processor and positive drain limit are required")};if err=q.validate();err!=nil{return};n:=uint32(1)<<q.Info.RingOrder;mask:=n-1;for consumed<limit{if err=ctx.Err();err!=nil{return};rh:=atomic.LoadUint32(idx(q.RequestControl,0));rt:=atomic.LoadUint32(idx(q.RequestControl,4));if rt-rh>n{err=ErrRingCorrupt;return};if rh==rt{return};ch:=atomic.LoadUint32(idx(q.CompletionControl,0));ct:=atomic.LoadUint32(idx(q.CompletionControl,4));if ct-ch>n{err=ErrRingCorrupt;return};if ct-ch==n{return};rs:=rh&mask;raw:=make([]byte,uapi.RequestSize);copy(raw,q.RequestRing[int(rs)*uapi.RequestSize:int(rs+1)*uapi.RequestSize]);req,de:=uapi.UnmarshalRequest(raw);if req.RequestID==0||req.QueueGeneration!=q.Info.QueueGeneration{err=fmt.Errorf("%w: request=%d generation=%d queue=%d",ErrMalformedRequest,req.RequestID,req.QueueGeneration,q.Info.QueueGeneration);return};var result Result;var pe error;requestCtx,cancel:=context.WithCancel(ctx)
        if q.RequestTimeout>0 { timedCtx,timedCancel:=context.WithTimeout(requestCtx,q.RequestTimeout); requestCtx=timedCtx; parentCancel:=cancel; cancel=func(){timedCancel();parentCancel()} }
        if !q.beginRequest(req.RequestID,req.QueueGeneration,cancel) { pe=ErrQueueGeneration } else {
          if de!=nil { pe=ErrInvalid } else { result,pe=p.Process(requestCtx,req) }
          q.endRequest(req.RequestID,req.QueueGeneration)
        }
        if requestCtx.Err()!=nil {
          _ = q.notifyCancel(req.RequestID,req.QueueGeneration)
        }
        cancel();code:=resultCode(pe);if pe!=nil{result=Result{}};var mappings []uapi.Mapping;if result.Mapping!=nil{mappings=[]uapi.Mapping{*result.Mapping}};if len(result.Mappings)>0{if len(mappings)>0{code=uapi.CodeInternal;result=Result{}}else{mappings=result.Mappings}};if code==uapi.CodeOK{switch req.Operation{case uapi.OpLookupBatch,uapi.OpAllocateZeroPublish:if len(mappings)==0{code=uapi.CodeInternal}else if _,e:=uapi.MarshalMappings(mappings);e!=nil{code=uapi.CodeInternal}else{first,last:=mappings[0],mappings[len(mappings)-1];if first.LogicalBlock!=req.LogicalStart||last.LogicalBlock+uint64(last.BlockCount)!=req.LogicalStart+uint64(req.BlockCount){code=uapi.CodeInternal};if req.Operation==uapi.OpAllocateZeroPublish&&len(mappings)!=1{code=uapi.CodeInternal}};case uapi.OpDiscard,uapi.OpFlush:if len(mappings)!=0{code=uapi.CodeInternal};default:code=uapi.CodeInvalid};if code!=uapi.CodeOK{result=Result{};mappings=nil}};if err=q.validate();err!=nil{return};cs:=ct&mask;po,mc,pb:=uint32(0),uint32(0),uint32(0);if code==uapi.CodeOK&&len(mappings)>0{po=cs*uapi.MaxBatch*uapi.MappingSize;enc,e:=uapi.MarshalMappings(mappings);if e!=nil{err=e;return};pb=uint32(len(enc));mc=uint32(len(mappings));off:=int(po);copy(q.Payload[off:off+len(enc)],enc)};responseGeneration:=req.RootGeneration;if code==uapi.CodeOK&&result.RootGeneration!=0{responseGeneration=result.RootGeneration};c:=uapi.Completion{Code:code,RequestID:req.RequestID,QueueGeneration:req.QueueGeneration,OwnerEpoch:req.OwnerEpoch,RootGeneration:responseGeneration,PayloadOffset:po,MappingCount:mc,PayloadBytes:pb};enc,e:=c.MarshalBinary();if e!=nil{err=e;return};off:=int(cs)*uapi.CompletionSize;copy(q.CompletionRing[off:off+uapi.CompletionSize],enc);if err=q.validate();err!=nil{return};atomic.StoreUint32(idx(q.CompletionControl,4),ct+1);atomic.StoreUint32(idx(q.RequestControl,0),rh+1);consumed++;produced++};return}