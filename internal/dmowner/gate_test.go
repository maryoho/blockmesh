package dmowner

import (
	"context"
	"reflect"
	"testing"
)
type call struct { name string; args []string }
type fakeRunner struct { calls []call; err error }
func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, call{name:name,args:append([]string(nil),args...)})
	return nil, f.err
}
func TestDMSetupGateUsesEpochScopedMessagesAndArgv(t *testing.T) {
	r:=&fakeRunner{}; g:=DMSetupGate{Runner:r,Binary:"/sbin/dmsetup"}; ctx:=context.Background()
	if err:=g.Prepare(ctx,"/dev/mapper/bm-vol",41);err!=nil{t.Fatal(err)}
	if err:=g.Enable(ctx,"/dev/mapper/bm-vol",41);err!=nil{t.Fatal(err)}
	if err:=g.Fence(ctx,"/dev/mapper/bm-vol",41);err!=nil{t.Fatal(err)}
	if err:=g.Drain(ctx,"/dev/mapper/bm-vol",41);err!=nil{t.Fatal(err)}
	want:=[]call{
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_prepare 41"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_enable 41"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_fence 41"}},
		{name:"/sbin/dmsetup",args:[]string{"message","/dev/mapper/bm-vol","0","owner_fence 41"}},
	}
	if !reflect.DeepEqual(r.calls,want){t.Fatalf("calls=%+v want=%+v",r.calls,want)}
}
func TestDMSetupGateRejectsInvalidInputs(t *testing.T) {
	r:=&fakeRunner{}; g:=DMSetupGate{Runner:r}
	if err:=g.Enable(context.Background(),"/dev/mapper/bm-vol",0);err==nil{t.Fatal("zero epoch accepted")}
	if err:=g.Enable(context.Background(),"bad\ndevice",1);err==nil{t.Fatal("multiline device accepted")}
	if len(r.calls)!=0{t.Fatalf("invalid input executed commands: %+v",r.calls)}
}
