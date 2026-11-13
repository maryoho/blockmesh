package lock

import (
 "context"
 "errors"
 "strings"
 "testing"
)

type rindexTestRunner struct { calls [][]string; responses []struct{out string;err error} }
func(r *rindexTestRunner)Run(_ context.Context,_ string,args ...string)([]byte,error){r.calls=append(r.calls,append([]string(nil),args...));if len(r.responses)==0{return nil,errors.New("unexpected command")};v:=r.responses[0];r.responses=r.responses[1:];return []byte(v.out),v.err}
func TestSanlockRIndexResolveUsesPersistentLookup(t *testing.T){
 runner:=&rindexTestRunner{responses:[]struct{out string;err error}{{out:"lookup done 0\nname owner/vol-a offset 1048576"}}}
 r:=&SanlockRIndexResolver{Lockspace:"bm",Device:"/dev/lock",Offset:8<<20,Runner:runner}
 got,err:=r.Resolve("owner/vol-a");if err!=nil{t.Fatal(err)};if got!=1048576{t.Fatalf("offset=%d",got)}
 if len(runner.calls)!=1||runner.calls[0][1]!="lookup"{t.Fatalf("unexpected commands: %#v",runner.calls)}
}

func TestSanlockRIndexResolveCreatesMissingResource(t *testing.T){
 runner:=&rindexTestRunner{responses:[]struct{out string;err error}{{err:errors.New("not found")},{out:"create_resource done 0\noffset 2097152"}}}
 r:=&SanlockRIndexResolver{Lockspace:"bm",Device:"/dev/lock",Offset:8<<20,Runner:runner}
 got,err:=r.Resolve("wal/pool/0");if err!=nil{t.Fatal(err)};if got!=2097152{t.Fatalf("offset=%d",got)}
 if len(runner.calls)!=2||runner.calls[1][1]!="create"{t.Fatalf("unexpected commands: %#v",runner.calls)}
}

func TestSanlockRIndexResolveRetriesLookupAfterCreateRace(t *testing.T){
 runner:=&rindexTestRunner{responses:[]struct{out string;err error}{{err:errors.New("not found")},{err:errors.New("already exists")},{out:"lookup done 0\nname owner/vol-b offset 3145728"}}}
 r:=&SanlockRIndexResolver{Lockspace:"bm",Device:"/dev/lock",Offset:8<<20,Runner:runner}
 got,err:=r.Resolve("owner/vol-b");if err!=nil{t.Fatal(err)};if got!=3145728{t.Fatalf("offset=%d",got)}
 if len(runner.calls)!=3{t.Fatalf("expected lookup/create/lookup, got %#v",runner.calls)}
}

func TestSanlockRIndexResolveFailsClosedWhenIndexUnavailable(t *testing.T){
 runner:=&rindexTestRunner{responses:[]struct{out string;err error}{{err:errors.New("bad rindex")},{err:errors.New("bad rindex")},{err:errors.New("bad rindex")}}}
 r:=&SanlockRIndexResolver{Lockspace:"bm",Device:"/dev/lock",Offset:8<<20,Runner:runner}
 if _,err:=r.Resolve("owner/vol");err==nil||!strings.Contains(err.Error(),"RINDEX"){t.Fatalf("expected RINDEX error, got %v",err)}
}
