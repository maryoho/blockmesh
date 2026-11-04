package sanqual

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRunner struct{ outputs map[string][]byte; failures map[string]error; calls []string }
func (f *fakeRunner) Run(_ context.Context,binary string,args ...string)([]byte,error){
	key:=binary+" "+strings.Join(args," ")
	f.calls=append(f.calls,key)
	if err:=f.failures[key];err!=nil{return f.outputs[key],err}
	if out,ok:=f.outputs[key];ok{return out,nil}
	return nil,errors.New("unexpected command: "+key)
}
func testCollector(r Runner)Collector{return Collector{Runner:r,ValidateDevice:func(p string)error{if !strings.HasPrefix(p,"/dev/"){return errors.New("not a device")};return nil}}}

func TestCollectCapturesEveryPathAndOnlyUsesReadCommands(t *testing.T){
	r:=&fakeRunner{outputs:map[string][]byte{}}
	for _,p:=range []string{"/dev/sda","/dev/sdb"}{
		r.outputs["udevadm info --query=property --name "+p]=[]byte("ID_WWN=naa.6000000000000001\n")
		r.outputs["sg_persist --in --read-keys --device="+p]=[]byte("  key = 0x00000000000000aa\n  key = 0x00000000000000bb\n")
		r.outputs["sg_persist --in --read-reservation --device="+p]=[]byte("  Reservation holder = 0x00000000000000aa\n")
	}
	r.outputs["multipath -ll /dev/mapper/mpatha"]=[]byte("mpatha (3600000000000001) dm-0\n|- 2:0:0:1 sda 8:0 active ready running\n`- 3:0:0:1 sdb 8:16 active ready running\n")
	for _,cmd:=range []string{
		"dmsetup info -c --noheadings -o name,uuid,attr,open,segments,events /dev/mapper/mpatha",
		"dmsetup status /dev/mapper/mpatha","sanlock client status","sanlock status",
		"lsblk -J -O /dev/mapper/mpatha",
	}{if _,ok:=r.outputs[cmd];!ok{r.outputs[cmd]=[]byte("ok")}}
	e,err:=testCollector(r).Collect(context.Background(),"/dev/mapper/mpatha","vol-1","node-a","boot-a",[]string{"/dev/sda","/dev/sdb"})
	if err!=nil{t.Fatal(err)}
	if len(e.Paths)!=2{t.Fatalf("got %d paths",len(e.Paths))}
	if e.Paths[0].WWID!="naa.6000000000000001"{t.Fatalf("WWID=%q",e.Paths[0].WWID)}
	if len(e.Paths[0].RegisteredKeys)!=2{t.Fatalf("keys=%v",e.Paths[0].RegisteredKeys)}
	if len(r.calls)!=12{t.Fatalf("got %d commands: %v",len(r.calls),r.calls)}
	for _,call:=range r.calls{
		if strings.Contains(call,"--out")||strings.Contains(call,"preempt")||strings.Contains(call,"register"){t.Fatalf("mutating command issued: %s",call)}
	}
}
func TestCollectRejectsMissingOrDuplicatePaths(t *testing.T){
	c:=testCollector(&fakeRunner{})
	if _,err:=c.Collect(context.Background(),"/dev/mapper/mpatha","","","",nil);err==nil{t.Fatal("expected missing paths error")}
	if _,err:=c.Collect(context.Background(),"/dev/mapper/mpatha","","","",[]string{"/dev/sda","/dev/sda"});err==nil{t.Fatal("expected duplicate path error")}
}
func TestVerifyTargetKeyAbsentRequiresEveryPathReadable(t *testing.T){
	e:=Evidence{PathCoverageVerified:true,Paths:[]PathEvidence{
		{Device:"/dev/sda",ReadKeys:CommandResult{ExitCode:0},KeysParsed:true,RegisteredKeys:[]string{"aa","bb"}},
		{Device:"/dev/sdb",ReadKeys:CommandResult{ExitCode:0},RegisteredKeys:[]string{"aa"}},
	}}
	v,err:=VerifyTargetKeyAbsent(e,"0x00000000000000bb")
	if err!=nil{t.Fatal(err)}
	if v.AbsentOnAllPaths||len(v.PresentPaths)!=1||v.PresentPaths[0]!="/dev/sda"{t.Fatalf("unexpected result: %+v",v)}
	e.Paths[0].RegisteredKeys=[]string{"aa"};e.Paths[1].ReadKeys.ExitCode=1
	v,err=VerifyTargetKeyAbsent(e,"aa")
	if err!=nil{t.Fatal(err)}
	if v.AbsentOnAllPaths||len(v.UnknownPaths)!=1{t.Fatalf("unreadable path accepted: %+v",v)}
}
func TestVerifyTargetKeyAbsentPassesOnlyWhenAbsentEverywhere(t *testing.T){
	e:=Evidence{PathCoverageVerified:true,Paths:[]PathEvidence{
		{Device:"/dev/sda",ReadKeys:CommandResult{ExitCode:0},RegisteredKeys:[]string{"aa"}},
		{Device:"/dev/sdb",ReadKeys:CommandResult{ExitCode:0},RegisteredKeys:[]string{"aa"}},
	}}
	v,err:=VerifyTargetKeyAbsent(e,"0x00000000000000bb")
	if err!=nil{t.Fatal(err)}
	if !v.AbsentOnAllPaths{t.Fatalf("expected absence: %+v",v)}
}

func TestVerifyTargetKeyAbsentRejectsIncompleteMultipathCoverage(t *testing.T) {
	e := Evidence{Paths: []PathEvidence{{Device: "/dev/sda", ReadKeys: CommandResult{ExitCode: 0}, KeysParsed: true}}}
	if _, err := VerifyTargetKeyAbsent(e, "aa"); err == nil { t.Fatal("incomplete path coverage must not produce a fencing conclusion") }
}
