package metastore

import "testing"

func TestPersistentRetireQueueReadsLegacySinglePageFormat(t *testing.T) {
	d:=newMemdev(64)
	s,err:=Format(d,Layout{VolumeID:"legacy-queue",TotalPages:64,ArenaStart:2,ArenaPages:32,LifecycleStart:40,LifecyclePages:4})
	if err!=nil{t.Fatal(err)}
	if err=s.Publish(NoPage,1);err!=nil{t.Fatal(err)}
	if err=FormatLifecycle(s);err!=nil{t.Fatal(err)}
	committer:=testCommitter{d:d}
	pa,err:=NewPersistentArena(s,nil,committer);if err!=nil{t.Fatal(err)}
	if err=pa.CommitPagesAndPublish(nil,nil,nil,NoPage,2,1,nil);err!=nil{t.Fatal(err)}
	legacy,err:=encodeRetireQueue([]RetireBatch{{RetireGeneration:1,PageIDs:[]uint64{2,3}}},5)
	if err!=nil{t.Fatal(err)}
	if err=writeFullAt(d,legacy,int64(41*PageSize));err!=nil{t.Fatal(err)}
	for page:=uint64(42);page<44;page++ {
		if err=writeFullAt(d,make([]byte,PageSize),int64(page*PageSize));err!=nil{t.Fatal(err)}
	}
	if err=writeFullAt(d,encodeLifecycleState(5,2),int64(40*PageSize));err!=nil{t.Fatal(err)}
	if err=d.Sync();err!=nil{t.Fatal(err)}
	batches,generation,err:=readRetireQueuePages(s)
	if err!=nil{t.Fatal(err)}
	if generation!=5 || len(batches)!=1 || len(batches[0].PageIDs)!=2 || batches[0].PageIDs[1]!=3 {
		t.Fatalf("legacy queue was not decoded: generation=%d batches=%+v",generation,batches)
	}
}
