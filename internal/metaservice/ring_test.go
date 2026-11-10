package metaservice
import("context";"encoding/binary";"errors";"testing";"time";"unsafe";"blockmesh/internal/uapi")
func fixture(t *testing.T,op uapi.Operation,id uint64)*QueueMemory{t.Helper();l,e:=uapi.Layout(4);if e!=nil{t.Fatal(e)};qid:=uint32(9);info:=uapi.SetupQueue{RingOrder:4,QueueGeneration:77,QueueID:qid,MaxBatch:uapi.MaxBatch,RequestControlOffset:uapi.EncodeMmapOffset(qid,l.RequestControl),RequestRingOffset:uapi.EncodeMmapOffset(qid,l.RequestRing),CompletionControlOffset:uapi.EncodeMmapOffset(qid,l.CompletionControl),CompletionRingOffset:uapi.EncodeMmapOffset(qid,l.CompletionRing),PayloadOffset:uapi.EncodeMmapOffset(qid,l.Payload)};q:=&QueueMemory{Info:info,RequestControl:make([]byte,4096),RequestRing:make([]byte,l.Entries*uapi.RequestSize),CompletionControl:make([]byte,4096),CompletionRing:make([]byte,l.Entries*uapi.CompletionSize),Payload:make([]byte,l.PayloadBytes)};for _,b:=range [][]byte{q.RequestControl,q.CompletionControl}{binary.LittleEndian.PutUint32(b[8:12],l.Entries);binary.LittleEndian.PutUint64(b[16:24],77)};r:=uapi.Request{Operation:op,RequestID:id,QueueGeneration:77,OwnerEpoch:12,LogicalStart:40,BlockCount:2};r.VolumeID[0]=1;r.NodeID[0]=2;r.BootID[0]=3;if op==uapi.OpAllocateZeroPublish||op==uapi.OpDiscard{r.IdempotencyKey[0]=1};if op==uapi.OpFlush{r.BlockCount=0};raw,e:=r.MarshalBinary();if e!=nil{t.Fatal(e)};copy(q.RequestRing,raw);*(*uint32)(unsafe.Pointer(&q.RequestControl[4]))=1;return q}
func TestDrainPublishesMapping(t *testing.T){q:=fixture(t,uapi.OpLookupBatch,5);n,w,e:=q.Drain(context.Background(),ProcessorFunc(func(_ context.Context,r uapi.Request)(Result,error){if r.RequestID!=5{t.Fatalf("request %+v",r)};return Result{Mapping:&uapi.Mapping{LogicalBlock:40,PhysicalBlock:900,BlockCount:2}},nil}),4);if e!=nil||n!=1||w!=1{t.Fatalf("n=%d w=%d err=%v",n,w,e)};if binary.LittleEndian.Uint32(q.CompletionControl[4:8])!=1{t.Fatal("completion tail not published")};c,e:=uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize]);if e!=nil{t.Fatal(e)};m,e:=uapi.UnmarshalMapping(q.Payload[:uapi.MappingSize]);if e!=nil{t.Fatal(e)};if c.Code!=uapi.CodeOK||c.MappingCount!=1||m.PhysicalBlock!=900{t.Fatalf("completion=%+v mapping=%+v",c,m)}}
func TestDrainMapsUnmapped(t *testing.T){q:=fixture(t,uapi.OpLookupBatch,6);_,_,e:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},ErrUnmapped}),1);if e!=nil{t.Fatal(e)};c,e:=uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize]);if e!=nil{t.Fatal(e)};if c.Code!=uapi.CodeUnmapped||c.MappingCount!=0{t.Fatalf("%+v",c)}}
func TestDiscardCompletionHasNoMapping(t *testing.T){q:=fixture(t,uapi.OpDiscard,7);_,_,e:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),1);if e!=nil{t.Fatal(e)};c,e:=uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize]);if e!=nil{t.Fatal(e)};if c.Code!=uapi.CodeOK||c.MappingCount!=0{t.Fatalf("%+v",c)}}
func TestFullCompletionRingDoesNotExecuteRequest(t *testing.T){q:=fixture(t,uapi.OpFlush,8);binary.LittleEndian.PutUint32(q.CompletionControl[:4],1);binary.LittleEndian.PutUint32(q.CompletionControl[4:8],17);n,w,e:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){t.Fatal("processor ran");return Result{},nil}),1);if e!=nil||n!=0||w!=0{t.Fatalf("n=%d w=%d err=%v",n,w,e)}}
func TestGenerationMismatchFailsClosed(t *testing.T){q:=fixture(t,uapi.OpFlush,9);binary.LittleEndian.PutUint64(q.RequestRing[16:24],78);_,_,e:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),1);if !errors.Is(e,ErrMalformedRequest){t.Fatalf("got %v",e)}}
func TestGenerationChangeDuringProcessorDoesNotPublishCompletion(t *testing.T){q:=fixture(t,uapi.OpFlush,10);_,produced,err:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){binary.LittleEndian.PutUint64(q.RequestControl[16:24],78);binary.LittleEndian.PutUint64(q.CompletionControl[16:24],78);return Result{},nil}),1);if !errors.Is(err,ErrQueueGeneration){t.Fatalf("got %v",err)};if produced!=0{t.Fatalf("published %d completions",produced)};if got:=binary.LittleEndian.Uint32(q.CompletionControl[4:8]);got!=0{t.Fatalf("completion tail advanced to %d",got)};if got:=binary.LittleEndian.Uint32(q.RequestControl[:4]);got!=0{t.Fatalf("request head advanced to %d",got)}}

func TestDrainRequestDeadlineCancelsProcessor(t *testing.T) {
 q:=fixture(t,uapi.OpAllocateZeroPublish,31)
 q.RequestTimeout=10*time.Millisecond
 started:=make(chan struct{})
 _,produced,err:=q.Drain(context.Background(),ProcessorFunc(func(ctx context.Context,_ uapi.Request)(Result,error){
  close(started)
  <-ctx.Done()
  return Result{},ctx.Err()
 }),1)
 if err!=nil { t.Fatalf("Drain: %v",err) }
 <-started
 if produced!=1 { t.Fatalf("produced=%d want 1",produced) }
 c,err:=uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize])
 if err!=nil { t.Fatal(err) }
 if c.Code!=uapi.CodeRetry { t.Fatalf("completion code=%v want retry",c.Code) }
}
func TestDrainParentCancellationReachesProcessor(t *testing.T) {
 q:=fixture(t,uapi.OpFlush,32)
 ctx,cancel:=context.WithCancel(context.Background())
 started:=make(chan struct{})
 done:=make(chan error,1)
 go func(){ _,_,err:=q.Drain(ctx,ProcessorFunc(func(ctx context.Context,_ uapi.Request)(Result,error){
  close(started);<-ctx.Done();return Result{},ctx.Err()
 }),1);done<-err }()
 <-started
 cancel()
 if err:=<-done;err!=nil { t.Fatalf("Drain: %v",err) }
 c,err:=uapi.UnmarshalCompletion(q.CompletionRing[:uapi.CompletionSize])
 if err!=nil { t.Fatal(err) }
 if c.Code!=uapi.CodeRetry { t.Fatalf("completion code=%v want retry",c.Code) }
}

func TestQueueMemoryCancelRegistryIsDrained(t *testing.T) {
 q:=fixture(t,uapi.OpFlush,43)
 _,_,err:=q.Drain(context.Background(),ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),1)
 if err!=nil { t.Fatal(err) }
 q.activeMu.Lock();n:=len(q.active);q.activeMu.Unlock()
 if n!=0 { t.Fatalf("active cancellation registry leaked %d entries",n) }
}
