package metastore

import (
 "errors"
 "testing"
)

type testCommitter struct{ d Device }
func (c testCommitter) CommitImages(images []PageImage)error{
 for _,image:=range images {if err:=writeFullAt(c.d,image.Data,int64(image.Offset));err!=nil{return err}}
 return c.d.Sync()
}

func TestPersistentArenaRecoversRetiredPagesAndReclaimsOnlyAfterProof(t *testing.T){
 d:=newMemdev(20)
 s,err:=Format(d,Layout{VolumeID:"persistent",TotalPages:20,ArenaStart:2,ArenaPages:14,LifecycleStart:16,LifecyclePages:2})
 if err!=nil{t.Fatal(err)}
 if err=s.WritePage(Page{ID:2,Type:1,Generation:1,Payload:[]byte("root-1")});err!=nil{t.Fatal(err)}
 if err=s.Sync();err!=nil{t.Fatal(err)}
 if err=s.Publish(2,1);err!=nil{t.Fatal(err)}
 if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
 if _,err=NewArena(s,nil);!errors.Is(err,ErrPersistentArenaRequired){t.Fatalf("volatile Arena must reject persistent layout: %v",err)}
 committer:=testCommitter{d:d}
 pa,err:=NewPersistentArena(s,[]uint64{2},committer);if err!=nil{t.Fatal(err)}
 reservation,err:=pa.Reserve(1);if err!=nil{t.Fatal(err)}
 newRoot:=reservation.Pages()[0]
 if err=pa.CommitPagesAndPublish(reservation,[]Page{{ID:newRoot,Type:1,Generation:2,Payload:[]byte("root-2")}},[]uint64{newRoot},newRoot,2,1,[]uint64{2});err!=nil{t.Fatal(err)}
 // Simulate a process crash after the durable WAL transaction but before
 // the in-memory Reserved -> Live transition.
 if got:=pa.Pending();len(got)!=1||got[0].RetireGeneration!=1||len(got[0].PageIDs)!=1||got[0].PageIDs[0]!=2{t.Fatalf("unexpected retire queue: %+v",got)}

 reopened,err:=Open(d,"persistent");if err!=nil{t.Fatal(err)}
 pa2,err:=NewPersistentArena(reopened,[]uint64{newRoot},committer);if err!=nil{t.Fatal(err)}
 if got:=pa2.Stats();got.LivePages!=2||got.FreePages!=12{t.Fatalf("retired page not pinned after reopen: %+v",got)}
 r,err:=pa2.Reserve(1);if err!=nil{t.Fatal(err)}
 if r.Pages()[0]==2||r.Pages()[0]==newRoot{t.Fatalf("allocated pinned page %d",r.Pages()[0])}
 if err=r.Abort();err!=nil{t.Fatal(err)}
 if _,err=pa2.Collect(ReclaimProof{SafeGeneration:2,IOsDrained:true});!errors.Is(err,ErrUnsafeReclaim){t.Fatalf("missing fencing proof: %v",err)}
 n,err:=pa2.Collect(ReclaimProof{SafeGeneration:2,IOsDrained:true,FencingConfirmed:true})
 if err!=nil||n!=1{t.Fatalf("collect returned %d, %v",n,err)}

 reopened2,err:=Open(d,"persistent");if err!=nil{t.Fatal(err)}
 pa3,err:=NewPersistentArena(reopened2,[]uint64{newRoot},committer);if err!=nil{t.Fatal(err)}
 if len(pa3.Pending())!=0{t.Fatalf("retire queue not durable after collect: %+v",pa3.Pending())}
 r,err=pa3.Reserve(1);if err!=nil{t.Fatal(err)}
 if r.Pages()[0]!=2{t.Fatalf("reclaimed page not reusable: got %d want 2",r.Pages()[0])}
}

func TestPersistentArenaFailsClosedOnLifecycleCorruption(t *testing.T){
 d:=newMemdev(12)
 s,err:=Format(d,Layout{VolumeID:"bad-life",TotalPages:12,ArenaStart:2,ArenaPages:8,LifecycleStart:10,LifecyclePages:2})
 if err!=nil{t.Fatal(err)}
 if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
 if err=FormatLifecycle(s);!errors.Is(err,ErrLifecycleAlreadyInitialized){t.Fatalf("expected reinitialization guard, got %v",err)}
 d.corrupt(int(10*PageSize+100))
 if _,err=NewPersistentArena(s,nil,testCommitter{d:d});!errors.Is(err,ErrLifecycleCorrupt){t.Fatalf("expected lifecycle corruption, got %v",err)}
}

func TestPersistentRetireQueueCapacityIsBounded(t *testing.T){
 ids:=make([]uint64,MaxPersistentRetiredPages+1)
 for i:=range ids{ids[i]=uint64(i+2)}
 if _,err:=encodeRetireQueue([]RetireBatch{{RetireGeneration:1,PageIDs:ids}},1);!errors.Is(err,ErrRetireQueueFull){t.Fatalf("expected bounded queue error, got %v",err)}
}
