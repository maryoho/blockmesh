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
		r.outputs["sg_persist --in --read-reservation --device="+p]=[]byte("PR generation=0x00000001\nReservation follows:\n  Key=0x00000000000000aa\n  scope: LU_SCOPE, type: Exclusive Access, registrants only\n")
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
		{Device:"/dev/sdb",ReadKeys:CommandResult{ExitCode:0},KeysParsed:true,RegisteredKeys:[]string{"aa"}},
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
		{Device:"/dev/sda",ReadKeys:CommandResult{ExitCode:0},KeysParsed:true,RegisteredKeys:[]string{"aa"}},
		{Device:"/dev/sdb",ReadKeys:CommandResult{ExitCode:0},KeysParsed:true,RegisteredKeys:[]string{"aa"}},
	}}
	v,err:=VerifyTargetKeyAbsent(e,"0x00000000000000bb")
	if err!=nil{t.Fatal(err)}
	if !v.AbsentOnAllPaths{t.Fatalf("expected absence: %+v",v)}
}

func TestVerifyTargetKeyAbsentRejectsIncompleteMultipathCoverage(t *testing.T) {
	e := Evidence{Paths: []PathEvidence{{Device: "/dev/sda", ReadKeys: CommandResult{ExitCode: 0}, KeysParsed: true}}}
	if _, err := VerifyTargetKeyAbsent(e, "aa"); err == nil { t.Fatal("incomplete path coverage must not produce a fencing conclusion") }
}

func TestParseReservationTypeAndHolder(t *testing.T) {
	tests := []struct { output string; key string; typ uint8; all bool }{
		{"Reservation follows:\n Key=0x00000000000000aa\n scope: LU_SCOPE, type: Write Exclusive", "aa", 1, false},
		{"Reservation follows:\n Key=0x00000000000000aa\n scope: LU_SCOPE, type: Exclusive Access", "aa", 3, false},
		{"Reservation follows:\n Key=0xaa\n scope: LU_SCOPE, type: Write Exclusive, registrants only", "aa", 5, false},
		{"Reservation follows:\n Key=0xaa\n scope: LU_SCOPE, type: Exclusive Access, registrants only", "aa", 6, false},
		{"Reservation follows:\n Key=0xaa\n scope: LU_SCOPE, type: Write Exclusive, all registrants", "aa", 7, true},
		{"Reservation follows:\n Key=0xaa\n scope: LU_SCOPE, type: Exclusive Access, all registrants", "aa", 8, true},
	}
	for _, tt := range tests {
		got := parseReservation(tt.output)
		if !got.Parsed || !got.Present || got.HolderKey != tt.key || got.Type != tt.typ || got.AllRegistrants != tt.all {
			t.Errorf("parseReservation(%q) = %+v", tt.output, got)
		}
	}
}

func TestParseReservationNoReservationAndMalformed(t *testing.T) {
	absent := parseReservation("PR generation=0x1\nReservation follows: No reservation held")
	if !absent.Parsed || absent.Present { t.Fatalf("no-reservation response not recognized: %+v", absent) }
	for _, output := range []string{
		"PR generation=0x1\nReservation follows:\n Key=0xaa",
		"Reservation follows:\n Key=0xaa\n Key=0xbb\n type: Exclusive Access",
		"Reservation follows:\n Key=0xaa\n type: vendor-specific",
	} {
		if got := parseReservation(output); got.Parsed { t.Errorf("malformed reservation accepted: %+v", got) }
	}
}

func TestVerifyReservationRequiresExpectedHolderAndTypeOnEveryPath(t *testing.T) {
	e := Evidence{PathCoverageVerified: true, Paths: []PathEvidence{
		{Device: "/dev/sda", ReadReservation: CommandResult{ExitCode: 0}, Reservation: ReservationEvidence{Present: true, Parsed: true, HolderKey: "aa", Type: 6}},
		{Device: "/dev/sdb", ReadReservation: CommandResult{ExitCode: 0}, Reservation: ReservationEvidence{Present: true, Parsed: true, HolderKey: "aa", Type: 6}},
	}}
	got, err := VerifyReservation(e, "0x000000aa", 6)
	if err != nil || !got.Verified { t.Fatalf("expected verified reservation: %+v, %v", got, err) }
	e.Paths[1].Reservation.HolderKey = "bb"
	got, err = VerifyReservation(e, "aa", 6)
	if err != nil || got.Verified || len(got.MismatchPaths) != 1 || got.MismatchPaths[0] != "/dev/sdb" {
		t.Fatalf("holder mismatch accepted: %+v, %v", got, err)
	}
	e.Paths[1].Reservation.HolderKey = "aa"
	e.Paths[1].Reservation.Type = 3
	got, err = VerifyReservation(e, "aa", 6)
	if err != nil || got.Verified || len(got.MismatchPaths) != 1 { t.Fatalf("type mismatch accepted: %+v, %v", got, err) }
	e.Paths[1].ReadReservation.ExitCode = 1
	got, err = VerifyReservation(e, "aa", 6)
	if err != nil || got.Verified || len(got.UnknownPaths) != 1 { t.Fatalf("unreadable path accepted: %+v, %v", got, err) }
}

func TestVerifyReservationFailsClosedForAbsentOrIncompleteReservation(t *testing.T) {
	e := Evidence{PathCoverageVerified: true, Paths: []PathEvidence{
		{Device: "/dev/sda", ReadReservation: CommandResult{ExitCode: 0}, Reservation: ReservationEvidence{Parsed: true, Present: false}},
	}}
	got, err := VerifyReservation(e, "aa", 6)
	if err != nil || got.Verified || len(got.MismatchPaths) != 1 { t.Fatalf("absent reservation accepted: %+v, %v", got, err) }
	e.PathCoverageVerified = false
	if _, err := VerifyReservation(e, "aa", 6); err == nil { t.Fatal("incomplete path coverage must fail") }
	if _, err := VerifyReservation(e, "aa", 2); err == nil { t.Fatal("unsupported PR type must fail") }
	if _, err := VerifyReservation(e, "0", 6); err == nil { t.Fatal("zero holder key must fail") }
}
