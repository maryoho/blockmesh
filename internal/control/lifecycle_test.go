package control

import (
 "context"
 "encoding/json"
 "strings"
 "testing"
)

type testLifecycleBackend struct{ called string; create VolumeCreateArgs }
func(b *testLifecycleBackend)PoolCreate(context.Context,PoolCreateArgs)(any,error){b.called="poolcreate";return "ok",nil}
func(b *testLifecycleBackend)PoolList(context.Context)(any,error){b.called="poollist";return []string{},nil}
func(b *testLifecycleBackend)PoolInfo(context.Context,string)(any,error){b.called="poolinfo";return nil,nil}
func(b *testLifecycleBackend)PoolDestroy(context.Context,string)(any,error){b.called="pooldestroy";return nil,nil}
func(b *testLifecycleBackend)VolumeCreate(_ context.Context,a VolumeCreateArgs)(any,error){b.called="volumecreate";b.create=a;return a,nil}
func(b *testLifecycleBackend)VolumeList(context.Context)(any,error){b.called="volumelist";return nil,nil}
func(b *testLifecycleBackend)VolumeInfo(context.Context,string)(any,error){b.called="volumeinfo";return nil,nil}
func(b *testLifecycleBackend)VolumeDelete(context.Context,string)(any,error){b.called="volumedelete";return nil,nil}
func(b *testLifecycleBackend)VolumeActivate(context.Context,string,string)(any,error){b.called="volumeactivate";return nil,nil}
func(b *testLifecycleBackend)VolumeDeactivate(context.Context,string)(any,error){b.called="volumedeactivate";return nil,nil}

func TestLifecycleHandlerDecodesStableCreateToken(t *testing.T){
 b:=&testLifecycleBackend{};h:=LifecycleHandler{Backend:b}
 raw:=json.RawMessage(`{"pool":"pool0","name":"disk0","size_bytes":419430400,"create_token":"000102030405060708090a0b0c0d0e0f"}`)
 if _,err:=h.Handle(context.Background(),"volumecreate",raw);err!=nil{t.Fatal(err)}
 if b.called!="volumecreate"||b.create.CreateToken[0]!=0||b.create.CreateToken[15]!=15{t.Fatalf("backend args=%+v called=%s",b.create,b.called)}
}
func TestLifecycleHandlerRejectsUnknownOrTrailingArguments(t *testing.T){
 h:=LifecycleHandler{Backend:&testLifecycleBackend{}}
 for _,raw:=range []string{`{"name":"pool0","unknown":1}`,`{"name":"pool0"} {}`,`[]`}{
  if _,err:=h.Handle(context.Background(),"poolinfo",json.RawMessage(raw));err==nil{t.Errorf("accepted invalid args %s",raw)}
 }
}
func TestLifecycleHandlerRequiresProductionBackend(t *testing.T){
 if _,err:=(LifecycleHandler{}).Handle(context.Background(),"poollist",json.RawMessage(`{}`));err==nil||!strings.Contains(err.Error(),"not configured"){t.Fatalf("got %v",err)}
}

func TestLifecycleHandlerRequiresExplicitPoolCreateConfirmation(t *testing.T) {
	b:=&testLifecycleBackend{}
	h:=LifecycleHandler{Backend:b}
	for _,raw:=range []string{ `{"name":"pool0","device":"/dev/mapper/mpatha"}`, `{"name":"pool0","device":"/dev/mapper/mpatha","confirmed":false}` } {
		if _,err:=h.Handle(context.Background(),"poolcreate",json.RawMessage(raw));err==nil||!strings.Contains(err.Error(),"confirmation") {
			t.Fatalf("expected explicit Pool create confirmation requirement, got %v",err)
		}
	}
	if b.called!="" { t.Fatalf("backend invoked without confirmation: %s",b.called) }
	if _,err:=h.Handle(context.Background(),"poolcreate",json.RawMessage(`{"name":"pool0","device":"/dev/mapper/mpatha","confirmed":true}`));err!=nil { t.Fatal(err) }
	if b.called!="poolcreate" { t.Fatalf("backend was not called after confirmation: %s",b.called) }
}
