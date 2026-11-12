package dmowner

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type call struct { name string; args []string }
type fakeRunner struct { calls []call; err error; status string }
func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, call{name:name,args:append([]string(nil),args...)})
	if f.err != nil { return nil, f.err }
	if len(args)>0 && args[0]=="status" { return []byte(f.status), nil }
	return nil, nil
}
func TestDMSetupGateUsesTableInstanceAndEpochScopedMessages(t *testing.T) {
	r:=&fakeRunner{status:"0 100 blockmesh instance=00000000000000ab epoch=0 gate=0 fenced=1 inflight=0"}
	g:=DMSetupGate{Runner:r,Binary:"/sbin/dmsetup"}; ctx:=context.Background()
	instance,err:=g.Prepare(ctx,"/dev/mapper/bm-vol",41);if err!=nil{t.Fatal(err)}
	if instance!=0xab{t.Fatalf("instance=%x",instance)}
	if err=g.Enable(ctx,"/dev/mapper/bm-vol",41,instance);err!=nil{t.Fatal(err)}
	if err=g.Fence(ctx,"/dev/mapper/bm-vol",41,instance);err!=nil{t.Fatal(err)}
	if err=g.Drain(ctx,"/dev/mapper/bm-vol",41,instance);err!=nil{t.Fatal(err)}
	want:=[]call{
		{name:"/sbin/dmsetup",args:[]string{"status","/dev/mapper/bm-vol"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_prepare 41 ab"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_enable 41 ab"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_fence 41 ab"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_fence 41 ab"}},
	}
	if !reflect.DeepEqual(r.calls,want){t.Fatalf("calls=%+v want=%+v",r.calls,want)}
}
func TestDMSetupGateFenceCurrentClosesCurrentTargetAndLeavesFreshTargetClosed(t *testing.T){
	r:=&fakeRunner{status:"0 100 blockmesh instance=99 epoch=17 gate=1 fenced=0 inflight=0"}
	g:=DMSetupGate{Runner:r}
	if err:=g.FenceCurrent(context.Background(),"/dev/mapper/bm-vol");err!=nil{t.Fatal(err)}
	if len(r.calls)!=2{t.Fatalf("expected one status read and fence message, got %+v",r.calls)}
	if got:=r.calls[1].args[3];got!="owner_fence 17 99"{t.Fatalf("message=%q",got)}
	r.calls=nil;r.status="0 100 blockmesh instance=100 epoch=0 gate=0 fenced=1 inflight=0"
	if err:=g.FenceCurrent(context.Background(),"/dev/mapper/bm-vol");err!=nil{t.Fatal(err)}
	if len(r.calls)!=1{t.Fatalf("fresh target should only need one status read: %+v",r.calls)}
}
func TestDMSetupGateReaderEpochMessagesAndFenceCurrent(t *testing.T) {
	r:=&fakeRunner{status:"0 100 blockmesh instance=ab epoch=0 gate=0 fenced=0 inflight=0 readonly=1 reader_epoch=9 root_generation=44 reader_gate=1"}
	g:=DMSetupGate{Runner:r,Binary:"/sbin/dmsetup"}
	ctx:=context.Background()
	instance,err:=g.PrepareReader(ctx,"/dev/mapper/bm-ro",10,45)
	if err!=nil{t.Fatal(err)}
	if instance!=0xab{t.Fatalf("instance=%x",instance)}
	if err=g.EnableReader(ctx,"/dev/mapper/bm-ro",10,45,instance);err!=nil{t.Fatal(err)}
	if err=g.FenceReader(ctx,"/dev/mapper/bm-ro",10,instance);err!=nil{t.Fatal(err)}
	want:=[]call{
		{name:"/sbin/dmsetup",args:[]string{"status","/dev/mapper/bm-ro"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-ro","0","reader_prepare 10 45 ab"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-ro","0","reader_enable 10 45 ab"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-ro","0","reader_fence 10 ab"}},
	}
	if !reflect.DeepEqual(r.calls,want){t.Fatalf("calls=%+v want=%+v",r.calls,want)}
	r.calls=nil
	if err=g.FenceCurrent(ctx,"/dev/mapper/bm-ro");err!=nil{t.Fatal(err)}
	if len(r.calls)!=2||r.calls[1].args[3]!="reader_fence 9 ab"{t.Fatalf("FenceCurrent did not select Reader gate: %+v",r.calls)}
}

func TestDMSetupGateRejectsMissingOrInvalidInstance(t *testing.T){
	for _,status:=range []string{"0 100 blockmesh epoch=0 gate=0","0 100 blockmesh instance=0 epoch=0","0 100 blockmesh instance=xyz epoch=0"}{
		r:=&fakeRunner{status:status};g:=DMSetupGate{Runner:r}
		if _,err:=g.Prepare(context.Background(),"/dev/mapper/bm-vol",1);err==nil{t.Fatalf("accepted status %q",status)}
	}
	r:=&fakeRunner{status:"0 100 blockmesh instance=ab epoch=0"}
	g:=DMSetupGate{Runner:r}
	if err:=g.Enable(context.Background(),"/dev/mapper/bm-vol",0,0);err==nil{t.Fatal("zero epoch accepted")}
	if err:=g.Enable(context.Background(),"/dev/mapper/bm-vol",1,0);err==nil{t.Fatal("zero instance accepted")}
	if err:=g.Enable(context.Background(),"bad\ndevice",1,1);err==nil{t.Fatal("multiline device accepted")}
	r.err=errors.New("dmsetup failed")
	if _,err:=g.Prepare(context.Background(),"/dev/mapper/bm-vol",2);err==nil{t.Fatal("status error ignored")}
}

func TestDMSetupGateRejectsStatusWithoutEpoch(t *testing.T) {
	r:=&fakeRunner{status:"0 100 blockmesh instance=99 gate=0"}
	g:=DMSetupGate{Runner:r}
	if err:=g.FenceCurrent(context.Background(),"/dev/mapper/bm-vol");err==nil{t.Fatal("status without epoch treated as a fresh target")}
}
