package metastore

import "testing"

func TestPersistentRetireQueueReadsLegacySinglePageFormat(t *testing.T) {
	d:=newMemdev(64)
	s,err:=Format(d,Layout{VolumeID:"legacy-queue",TotalPages:64,ArenaStart:2,ArenaPages:32,LifecycleStart:40,LifecyclePages:4})
	if err!=nil{t.Fatal(err)}
	if err=s.Publish(NoPage,2);err!=nil{t.Fatal(err)}
	legacy,err:=encodeRetireQueue([]RetireBatch{{RetireGeneration:1,PageIDs:[]uint64{2,3}}},5)
	if err!=nil{t.Fatal(err)}
	if err=writeFullAt(d,legacy,int64(41*PageSize));err!=nil{t.Fatal(err)}
	if err=d.Sync();err!=nil{t.Fatal(err)}
	batches,generation,err:=readRetireQueuePages(s)
	if err!=nil{t.Fatal(err)}
	if generation!=5 || len(batches)!=1 || len(batches[0].PageIDs)!=2 || batches[0].PageIDs[1]!=3 {
		t.Fatalf("legacy queue was not decoded: generation=%d batches=%+v",generation,batches)
	}
}
