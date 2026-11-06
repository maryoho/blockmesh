package uapi
import("encoding/binary";"testing")
func setupFixture()SetupQueue{return SetupQueue{RingOrder:10,QueueGeneration:99,RequestControlOffset:RequestControlOffset,RequestRingOffset:RequestRingOffset,CompletionControlOffset:CompletionControlOffset,CompletionRingOffset:CompletionRingOffset,PayloadOffset:MappingPayloadOffset,MaxBatch:MaxBatch}}
func TestQueueLayoutsDoNotOverlapAtEveryOrder(t *testing.T){
 for order:=uint32(MinRingOrder);order<=MaxRingOrder;order++{l,e:=Layout(order);if e!=nil{t.Fatal(e)};if RequestRingOffset+uint64(l.Entries)*RequestSize>CompletionControlOffset{t.Fatalf("request overlap order %d",order)};if CompletionRingOffset+uint64(l.Entries)*CompletionSize>MappingPayloadOffset{t.Fatalf("completion overlap order %d",order)};if l.TotalBytes%PageSize!=0||l.PayloadBytes!=uint64(l.Entries)*MaxBatch*MappingSize{t.Fatalf("bad layout %+v",l)}}
 if _,e:=Layout(MaxRingOrder+1);e==nil{t.Fatal("accepted excessive order")}
}
func TestSetupQueueRoundTripAndRejectBadOffset(t *testing.T){w:=setupFixture();b,e:=w.MarshalBinary();if e!=nil{t.Fatal(e)};g,e:=UnmarshalSetupQueue(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v want %+v",g,w)};binary.LittleEndian.PutUint64(b[32:40],0x20000);if _,e=UnmarshalSetupQueue(b);e==nil{t.Fatal("accepted invalid offset")}}
func TestCancelRoundTripAndReserved(t *testing.T){w:=CancelRequest{RequestID:7,QueueGeneration:8,Flags:1};b,e:=w.MarshalBinary();if e!=nil{t.Fatal(e)};g,e:=UnmarshalCancelRequest(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v want %+v",g,w)};b[39]=1;if _,e=UnmarshalCancelRequest(b);e==nil{t.Fatal("accepted reserved bytes")}}
func TestABIInfoRoundTripAndMismatch(t *testing.T){w:=ABIInfo{Features:3};b:=w.MarshalBinary();g,e:=UnmarshalABIInfo(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v",g)};binary.LittleEndian.PutUint32(b[8:12],0);if _,e=UnmarshalABIInfo(b);e==nil{t.Fatal("accepted ABI mismatch")}}
func TestCorrelatorCancelLateCompletionAndReset(t *testing.T){
 c,e:=NewCorrelator(10);if e!=nil{t.Fatal(e)};if e=c.Register(1,10);e!=nil{t.Fatal(e)};if e=c.Register(1,10);e==nil{t.Fatal("accepted duplicate")}
 if !c.Cancel(1,10)||c.Cancel(1,10){t.Fatal("cancel idempotency violated")};if c.Complete(Completion{RequestID:1,QueueGeneration:10}){t.Fatal("accepted cancelled completion")}
 if e=c.Register(2,10);e!=nil{t.Fatal(e)};if e=c.Reset(11);e!=nil{t.Fatal(e)};if c.Complete(Completion{RequestID:2,QueueGeneration:10}){t.Fatal("accepted stale completion")};if e=c.Register(2,10);e==nil{t.Fatal("accepted stale request")}
 if e=c.Register(2,11);e!=nil{t.Fatal(e)};if !c.Complete(Completion{RequestID:2,QueueGeneration:11})||c.Complete(Completion{RequestID:2,QueueGeneration:11}){t.Fatal("completion not exactly once")}
}
