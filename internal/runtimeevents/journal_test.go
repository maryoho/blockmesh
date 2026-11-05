package runtimeevents

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testEvent() Event {
	return Event{VolumeID:"vol",NodeID:"node-b",BootID:"boot-b",OwnerEpoch:9,EventType:"owner_record.durable",Phase:"publish",Result:"success",Proof:"test"}
}

func TestJournalAppendReadAndReopen(t *testing.T) {
	path:=filepath.Join(t.TempDir(),"events.jsonl")
	j,err:=Open(path,"operation-1");if err!=nil{t.Fatal(err)}
	for i:=0;i<3;i++{e:=testEvent();e.Detail=string(rune('a'+i));if err=j.Record(e);err!=nil{t.Fatal(err)}}
	if err=j.Close();err!=nil{t.Fatal(err)}
	records,err:=Read(path);if err!=nil{t.Fatal(err)}
	if len(records)!=3 || records[0].Sequence!=1 || records[2].Sequence!=3 || records[2].PreviousHash!=records[1].Hash{t.Fatalf("invalid records: %+v",records)}
	j,err=Open(path,"operation-1");if err!=nil{t.Fatal(err)}
	e:=testEvent();if err=j.Record(e);err!=nil{t.Fatal(err)}
	_ = j.Close()
	records,err=Read(path);if err!=nil{t.Fatal(err)}
	if len(records)!=4 || records[3].Sequence!=4{t.Fatalf("append after reopen failed: %+v",records)}
}

func TestJournalRejectsCorruptionAndIncompleteTail(t *testing.T) {
	path:=filepath.Join(t.TempDir(),"events.jsonl")
	j,err:=Open(path,"operation-1");if err!=nil{t.Fatal(err)}
	if err=j.Record(testEvent());err!=nil{t.Fatal(err)}
	_ = j.Close()
	b,err:=os.ReadFile(path);if err!=nil{t.Fatal(err)}
	b=strings.Replace(string(b),"\"result\":\"success\"","\"result\":\"failure\"",1)
	if err=os.WriteFile(path,[]byte(b),0600);err!=nil{t.Fatal(err)}
	if _,err=Read(path);err==nil{t.Fatal("tampered event accepted")}
	if err=os.WriteFile(path,[]byte("{"),0600);err!=nil{t.Fatal(err)}
	if _,err=Open(path,"operation-1");err==nil{t.Fatal("incomplete tail accepted")}
}

func TestJournalExclusiveWriterAndRequiredFields(t *testing.T) {
	path:=filepath.Join(t.TempDir(),"events.jsonl")
	j,err:=Open(path,"operation-1");if err!=nil{t.Fatal(err)}
	if _,err=Open(path,"operation-1");err==nil{t.Fatal("second writer acquired journal lock")}
	if err=j.Record(Event{VolumeID:"vol"});err==nil{t.Fatal("incomplete event accepted")}
	_ = j.Close()
}
