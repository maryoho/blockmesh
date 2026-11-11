package uapi
import("encoding/binary";"testing")
func setupFixture()SetupQueue{return SetupQueue{RingOrder:10,QueueGeneration:99,RequestControlOffset:EncodeMmapOffset(7,RequestControlOffset),RequestRingOffset:EncodeMmapOffset(7,RequestRingOffset),CompletionControlOffset:EncodeMmapOffset(7,CompletionControlOffset),CompletionRingOffset:EncodeMmapOffset(7,CompletionRingOffset),PayloadOffset:EncodeMmapOffset(7,MappingPayloadOffset),QueueID:7,MaxBatch:MaxBatch}}
func TestQueueLayoutsDoNotOverlapAtEveryOrder(t *testing.T){
 for order:=uint32(MinRingOrder);order<=MaxRingOrder;order++{l,e:=Layout(order);if e!=nil{t.Fatal(e)};if RequestRingOffset+uint64(l.Entries)*RequestSize>CompletionControlOffset{t.Fatalf("request overlap order %d",order)};if CompletionRingOffset+uint64(l.Entries)*CompletionSize>MappingPayloadOffset{t.Fatalf("completion overlap order %d",order)};if l.TotalBytes%PageSize!=0||l.PayloadBytes!=uint64(l.Entries)*MaxBatch*MappingSize{t.Fatalf("bad layout %+v",l)}}
 if _,e:=Layout(MaxRingOrder+1);e==nil{t.Fatal("accepted excessive order")}
}
func TestSetupQueueRoundTripAndRejectBadOffset(t *testing.T){w:=setupFixture();b,e:=w.MarshalBinary();if e!=nil{t.Fatal(e)};g,e:=UnmarshalSetupQueue(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v want %+v",g,w)};binary.LittleEndian.PutUint64(b[32:40],0x20000);if _,e=UnmarshalSetupQueue(b);e==nil{t.Fatal("accepted invalid offset")}}
func TestCancelRoundTripAndReserved(t *testing.T){w:=CancelRequest{QueueID:1,RequestID:7,QueueGeneration:8};b,e:=w.MarshalBinary();if e!=nil{t.Fatal(e)};g,e:=UnmarshalCancelRequest(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v want %+v",g,w)};b[39]=1;if _,e=UnmarshalCancelRequest(b);e==nil{t.Fatal("accepted reserved bytes")}}
func TestABIInfoRoundTripAndMismatch(t *testing.T){w:=ABIInfo{Features:3};b:=w.MarshalBinary();g,e:=UnmarshalABIInfo(b);if e!=nil{t.Fatal(e)};if g!=w{t.Fatalf("got %+v",g)};binary.LittleEndian.PutUint32(b[8:12],0);if _,e=UnmarshalABIInfo(b);e==nil{t.Fatal("accepted ABI mismatch")}}
func TestCorrelatorCancelLateCompletionAndReset(t *testing.T){
 c,e:=NewCorrelator(10);if e!=nil{t.Fatal(e)};if e=c.Register(1,10);e!=nil{t.Fatal(e)};if e=c.Register(1,10);e==nil{t.Fatal("accepted duplicate")}
 if !c.Cancel(1,10)||c.Cancel(1,10){t.Fatal("cancel idempotency violated")};if c.Complete(Completion{RequestID:1,QueueGeneration:10}){t.Fatal("accepted cancelled completion")}
 if e=c.Register(2,10);e!=nil{t.Fatal(e)};if e=c.Reset(11);e!=nil{t.Fatal(e)};if c.Complete(Completion{RequestID:2,QueueGeneration:10}){t.Fatal("accepted stale completion")};if e=c.Register(2,10);e==nil{t.Fatal("accepted stale request")}
 if e=c.Register(2,11);e!=nil{t.Fatal(e)};if !c.Complete(Completion{RequestID:2,QueueGeneration:11})||c.Complete(Completion{RequestID:2,QueueGeneration:11}){t.Fatal("completion not exactly once")}
}

func TestMmapOffsetsAreQueueScoped(t *testing.T) {
    a, err := EncodeMmapOffsetChecked(11, RequestRingOffset)
    if err != nil { t.Fatal(err) }
    b, err := EncodeMmapOffsetChecked(12, RequestRingOffset)
    if err != nil { t.Fatal(err) }
    if a == b { t.Fatal("different queues aliased the same mmap offset") }
    id, off, err := DecodeMmapOffset(a)
    if err != nil { t.Fatal(err) }
    if id != 11 || off != RequestRingOffset { t.Fatalf("decoded (%d,%x)", id, off) }
    if _, err := EncodeMmapOffsetChecked(0, 0); err == nil { t.Fatal("accepted zero queue ID") }
    if _, err := EncodeMmapOffsetChecked(1, 1); err == nil { t.Fatal("accepted unaligned offset") }
}
func TestQueueManagerServiceRestartFencesGenerations(t *testing.T) {
    m := NewQueueManager()
    q1, err := m.Create("target-a", 4); if err != nil { t.Fatal(err) }
    q2, err := m.Create("target-b", 5); if err != nil { t.Fatal(err) }
    if q1.QueueID == q2.QueueID { t.Fatal("queue IDs reused") }
    if err := m.OpenService(100); err != nil { t.Fatal(err) }
    a, _ := m.Get(q1.QueueID); b, _ := m.Get(q2.QueueID)
    if !a.Attached || !b.Attached || a.Generation <= q1.Generation || b.Generation <= q2.Generation { t.Fatal("service attach did not advance queue generations") }
    if err := m.OpenService(101); err == nil { t.Fatal("allowed a second service session") }
    if err := m.CloseService(100); err != nil { t.Fatal(err) }
    stale, _ := m.Get(q1.QueueID)
    if stale.Attached || stale.Generation <= a.Generation { t.Fatal("close did not detach and fence queue") }
    if err := m.OpenService(101); err != nil { t.Fatal(err) }
    fresh, _ := m.Get(q1.QueueID)
    if !fresh.Attached || fresh.Generation <= stale.Generation { t.Fatal("restart did not create a fresh generation") }
    if err := m.Destroy(q1.QueueID, "target-b"); err == nil { t.Fatal("destroy accepted wrong target owner") }
    if err := m.Destroy(q1.QueueID, "target-a"); err != nil { t.Fatal(err) }
    if _, ok := m.Get(q1.QueueID); ok { t.Fatal("destroyed queue remains visible") }
}

func TestMmapQueueOffsetFitsSignedOffT(t *testing.T) {
    off, err := EncodeMmapOffsetChecked(MaxQueueID, MappingPayloadOffset)
    if err != nil { t.Fatal(err) }
    if off > uint64(^uint64(0)>>1) { t.Fatalf("mmap offset %#x exceeds signed off_t", off) }
}
