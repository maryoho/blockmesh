package metastore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"blockmesh/internal/fence"
)

type testReaderCoordinator struct { mu sync.Mutex }
func (c *testReaderCoordinator) LockReaderSet(string) (func(),error) {
	c.mu.Lock()
	return c.mu.Unlock,nil
}

type recordingFencer struct { called bool }
func (f *recordingFencer) FenceNode(_ context.Context,nodeID,bootID string) error {
	f.called=true
	if nodeID!="node-b" || bootID!="boot-b" { panic("unexpected fencing identity") }
	return nil
}

func TestReaderGenerationTracksMinimumAndFencing(t *testing.T) {
	d:=newMemdev(32)
	s,err:=Format(d,Layout{VolumeID:"reader-test",TotalPages:32,ArenaStart:2,ArenaPages:12,LifecycleStart:16,LifecyclePages:2})
	if err!=nil {t.Fatal(err)}
	if err=s.Publish(NoPage,1);err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:d}
	coordinator:=&testReaderCoordinator{}
	pa,err:=NewPersistentArena(s,nil,committer,coordinator);if err!=nil{t.Fatal(err)}
	for seq:=uint64(2);seq<=12;seq++ {
		if err=pa.CommitPagesAndPublish(nil,nil,nil,NoPage,seq,seq-1,nil);err!=nil{t.Fatal(err)}
	}
	readers,err:=NewReaderTable(s,18,4,committer,coordinator);if err!=nil{t.Fatal(err)}
	if err=readers.Register(0,"node-a","boot-a",1,12);err!=nil{t.Fatal(err)}
	if err=readers.Register(1,"node-b","boot-b",1,12);err!=nil{t.Fatal(err)}
	if got,err:=readers.SafeGeneration();err!=nil||got!=12{t.Fatalf("safe generation=%d err=%v",got,err)}

	remote,err:=Open(d,"reader-test");if err!=nil{t.Fatal(err)}
	remoteArena,err:=NewPersistentArena(remote,nil,committer,coordinator);if err!=nil{t.Fatal(err)}
	if err=remoteArena.CommitPagesAndPublish(nil,nil,nil,NoPage,13,12,nil);err!=nil{t.Fatal(err)}
	if err=readers.Heartbeat(0,"node-a","boot-a",1,12);err!=nil{t.Fatal(err)}
	if err=readers.Heartbeat(0,"node-a","boot-a",1,13);!errors.Is(err,ErrReaderGeneration){t.Fatalf("heartbeat must not advance root generation: %v",err)}
	if err=readers.AdvanceGeneration(0,"node-a","boot-a",1,13,false);err==nil{t.Fatal("generation advance without old I/O drain must fail")}
	if err=readers.AdvanceGeneration(1,"node-b","boot-b",1,13,true);err!=nil{t.Fatal(err)}
	if got,err:=readers.SafeGeneration();err!=nil||got!=12{t.Fatalf("slow reader must pin old generation: %d %v",got,err)}
	if err=readers.AdvanceGeneration(0,"node-a","boot-a",1,13,true);err!=nil{t.Fatal(err)}
	if s.Superblock().MappingSequence!=13{t.Fatalf("reader store did not refresh committed root: %+v",s.Superblock())}
	if got,err:=readers.SafeGeneration();err!=nil||got!=13{t.Fatalf("all readers advanced: %d %v",got,err)}

	if err=readers.BeginLeave(0,"node-a","boot-a",1);err!=nil{t.Fatal(err)}
	if got,err:=readers.SafeGeneration();err!=nil||got!=13{t.Fatalf("Leaving reader generation=%d err=%v",got,err)}
	if err=readers.CloseDrained(0,"node-a","boot-a",1,false);err==nil{t.Fatal("expected undrained close rejection")}
	if err=readers.CloseDrained(0,"node-a","boot-a",1,true);err!=nil{t.Fatal(err)}
	if got,err:=readers.SafeGeneration();err!=nil||got!=13{t.Fatalf("safe generation after clean close=%d err=%v",got,err)}

	f:=&recordingFencer{}
	if err=readers.FenceAndClose(context.Background(),1,"node-b","boot-b",1,f);err!=nil{t.Fatal(err)}
	if !f.called{t.Fatal("fencer was not called")}
	if got,err:=readers.SafeGeneration();err!=nil||got!=^uint64(0){t.Fatalf("all readers inactive: safe=%d err=%v",got,err)}
	if err=readers.FenceAndClose(context.Background(),1,"node-b","boot-b",1,f);err==nil{t.Fatal("already fenced reader must not be fenced twice")}
	if err=readers.Register(1,"node-b","boot-c",2,13);err!=nil{t.Fatalf("inactive slot should be reusable with a new epoch: %v",err)}

	reopened,err:=Open(d,"reader-test");if err!=nil{t.Fatal(err)}
	readers2,err:=NewReaderTable(reopened,18,4,committer);if err!=nil{t.Fatal(err)}
	snapshot,err:=readers2.Snapshot();if err!=nil{t.Fatal(err)}
	if len(snapshot)!=1 || snapshot[0].BootID!="boot-c" || snapshot[0].Epoch!=2 {t.Fatalf("unexpected recovered reader table: %+v",snapshot)}
}

func TestPersistentRetireQueueSpansMultiplePages(t *testing.T) {
	d:=newMemdev(1000)
	s,err:=Format(d,Layout{VolumeID:"multi-queue",TotalPages:1000,ArenaStart:2,ArenaPages:700,LifecycleStart:800,LifecyclePages:4})
	if err!=nil{t.Fatal(err)}
	if err=s.Publish(NoPage,1);err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:d}
	pa,err:=NewPersistentArena(s,nil,committer);if err!=nil{t.Fatal(err)}
	if err=pa.CommitPagesAndPublish(nil,nil,nil,NoPage,2,1,nil);err!=nil{t.Fatal(err)}
	ids:=make([]uint64,600)
	for i:=range ids{ids[i]=uint64(i+2)}
	pages,err:=encodeRetireQueuePages([]RetireBatch{{RetireGeneration:1,PageIDs:ids}},7,3)
	if err!=nil{t.Fatal(err)}
	if len(pages)!=3{t.Fatalf("got %d queue pages, want 3",len(pages))}
	for i,page:=range pages {
		if err=writeFullAt(d,page,int64((801+uint64(i))*PageSize));err!=nil{t.Fatal(err)}
	}
	if err=d.Sync();err!=nil{t.Fatal(err)}
	batches,generation,err:=readRetireQueuePages(s)
	if err!=nil{t.Fatal(err)}
	if generation!=7 || len(batches)!=1 || len(batches[0].PageIDs)!=600 {t.Fatalf("decoded queue generation=%d batches=%+v",generation,batches)}
	if batches[0].PageIDs[0]!=2 || batches[0].PageIDs[599]!=601 {t.Fatal("queue page ordering was not preserved")}
	if err=writeFullAt(d,encodeLifecycleState(7,2),int64(800*PageSize));err!=nil{t.Fatal(err)}
	if err=d.Sync();err!=nil{t.Fatal(err)}
	pa,err=NewPersistentArena(s,nil,committer);if err!=nil{t.Fatal(err)}
	if stats:=pa.Stats();stats.LivePages!=600{t.Fatalf("multi-page retire records not pinned after reopen: %+v",stats)}
	if pending:=pa.Pending();len(pending)!=1||len(pending[0].PageIDs)!=600{t.Fatalf("multi-page queue recovery mismatch: %+v",pending)}
}

func TestReaderTableRejectsOverlappingReservedRegions(t *testing.T) {
	d:=newMemdev(24)
	s,err:=Format(d,Layout{VolumeID:"reader-overlap",TotalPages:24,ArenaStart:2,ArenaPages:10,LifecycleStart:16,LifecyclePages:2})
	if err!=nil{t.Fatal(err)}
	if _,err=NewReaderTable(s,10,2,testCommitter{d:d});err==nil{t.Fatal("reader table must not overlap Arena")}
	if _,err=NewReaderTable(s,17,2,testCommitter{d:d});err==nil{t.Fatal("reader table must not overlap lifecycle region")}
}

var _ fence.Fencer = (*recordingFencer)(nil)

func TestCollectWithReadersRequiresSharedCoordinator(t *testing.T) {
	d:=newMemdev(32)
	s,err:=Format(d,Layout{VolumeID:"reader-coordinator",TotalPages:32,ArenaStart:2,ArenaPages:12,LifecycleStart:16,LifecyclePages:2})
	if err!=nil{t.Fatal(err)}
	if err=s.Publish(NoPage,1);err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:d}
	pa0,err:=NewPersistentArena(s,nil,committer);if err!=nil{t.Fatal(err)}
	readers0,err:=NewReaderTable(s,18,4,committer);if err!=nil{t.Fatal(err)}
	if _,err=pa0.CollectWithReaders(readers0,true);!errors.Is(err,ErrReaderCoordinatorRequired){t.Fatalf("missing cluster barrier should fail closed: %v",err)}
	coordinator:=&testReaderCoordinator{}
	pa,err:=NewPersistentArena(s,nil,committer,coordinator);if err!=nil{t.Fatal(err)}
	readers,err:=NewReaderTable(s,18,4,committer,coordinator);if err!=nil{t.Fatal(err)}
	if n,err:=pa.CollectWithReaders(readers,true);err!=nil||n!=0{t.Fatalf("empty coordinated collection returned %d, %v",n,err)}
}

func TestPersistentCollectionWaitsForReaderGeneration(t *testing.T) {
	d:=newMemdev(32)
	s,err:=Format(d,Layout{VolumeID:"reader-collect",TotalPages:32,ArenaStart:2,ArenaPages:12,LifecycleStart:16,LifecyclePages:2})
	if err!=nil{t.Fatal(err)}
	if err=s.WritePage(Page{ID:2,Type:1,Generation:1,Payload:[]byte("old-root")});err!=nil{t.Fatal(err)}
	if err=s.Sync();err!=nil{t.Fatal(err)}
	if err=s.Publish(2,1);err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:d}
	coordinator:=&testReaderCoordinator{}
	pa,err:=NewPersistentArena(s,[]uint64{2},committer,coordinator);if err!=nil{t.Fatal(err)}
	readers,err:=NewReaderTable(s,18,4,committer,coordinator);if err!=nil{t.Fatal(err)}
	if err=readers.Register(0,"node-reader","boot-1",1,1);err!=nil{t.Fatal(err)}

	reservation,err:=pa.Reserve(1);if err!=nil{t.Fatal(err)}
	newRoot:=reservation.Pages()[0]
	err=pa.CommitPagesAndPublish(reservation,[]Page{{ID:newRoot,Type:1,Generation:2,Payload:[]byte("new-root")}},[]uint64{newRoot},newRoot,2,1,[]uint64{2})
	if err!=nil{t.Fatal(err)}
	if err=reservation.Commit();err!=nil{t.Fatal(err)}

	if n,err:=pa.CollectWithReaders(readers,true);err!=nil||n!=0{t.Fatalf("old reader must pin retired page: released=%d err=%v",n,err)}
	if err=readers.AdvanceGeneration(0,"node-reader","boot-1",1,2,true);err!=nil{t.Fatal(err)}
	if n,err:=pa.CollectWithReaders(readers,true);err!=nil||n!=1{t.Fatalf("advanced reader should permit reclamation: released=%d err=%v",n,err)}
	if pending:=pa.Pending();len(pending)!=0{t.Fatalf("retire queue not cleared: %+v",pending)}
}
