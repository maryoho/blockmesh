//go:build linux

package control

import (
 "context"
 "encoding/json"
 "errors"
 "net"
 "os"
 "path/filepath"
 "strings"
 "testing"
 "time"
)
func TestServerLifecycleRequest(t *testing.T){
 path:=filepath.Join(t.TempDir(),"control.sock")
 s,err:=New(Config{Path:path,Handler:HandlerFunc(func(_ context.Context,a string,_ json.RawMessage)(any,error){if a!="poollist"{return nil,errors.New("unexpected action")};return map[string]any{"pools":1},nil})});if err!=nil{t.Fatal(err)}
 if err=s.Listen();err!=nil{t.Fatal(err)};defer s.Close()
 if info,err:=os.Stat(path);err!=nil{t.Fatal(err)}else if info.Mode().Perm()!=0660{t.Fatalf("socket mode=%o",info.Mode().Perm())}
 ctx,cancel:=context.WithCancel(context.Background());done:=make(chan error,1);go func(){done<-s.Serve(ctx)}()
 conn,err:=net.DialTimeout("unix",path,time.Second);if err!=nil{t.Fatal(err)}
 _,err=conn.Write([]byte("{\"version\":1,\"id\":\"req-1\",\"action\":\"poollist\",\"args\":{}}\n"));if err!=nil{t.Fatal(err)}
 var resp Response;if err=json.NewDecoder(conn).Decode(&resp);err!=nil{t.Fatal(err)};_=conn.Close()
 if !resp.OK||resp.ID!="req-1"||resp.Version!=1||!strings.Contains(string(resp.Result),"\"pools\":1"){t.Fatalf("unexpected response: %+v",resp)}
 cancel();select{case err=<-done:if err!=nil{t.Fatal(err)};case <-time.After(time.Second):t.Fatal("Serve did not stop")}
}
func TestListenRejectsNonSocketPath(t *testing.T){
 path:=filepath.Join(t.TempDir(),"control.sock");if err:=os.WriteFile(path,[]byte("do not unlink"),0600);err!=nil{t.Fatal(err)}
 s,err:=New(Config{Path:path,Handler:HandlerFunc(func(context.Context,string,json.RawMessage)(any,error){return nil,nil})});if err!=nil{t.Fatal(err)}
 if err=s.Listen();err==nil{t.Fatal("non-socket path accepted")}
 b,err:=os.ReadFile(path);if err!=nil||string(b)!="do not unlink"{t.Fatalf("existing file changed: %q %v",b,err)}
}
func TestServerRejectsInvalidArguments(t *testing.T){
 path:=filepath.Join(t.TempDir(),"control.sock");called:=false
 s,err:=New(Config{Path:path,Handler:HandlerFunc(func(context.Context,string,json.RawMessage)(any,error){called=true;return nil,nil})});if err!=nil{t.Fatal(err)}
 if err=s.Listen();err!=nil{t.Fatal(err)};defer s.Close()
 ctx,cancel:=context.WithCancel(context.Background());defer cancel();go s.Serve(ctx)
 conn,err:=net.Dial("unix",path);if err!=nil{t.Fatal(err)}
 _,_=conn.Write([]byte("{\"version\":1,\"id\":\"x\",\"action\":\"poolcreate\",\"args\":[]}\n"))
 var resp Response;if err=json.NewDecoder(conn).Decode(&resp);err!=nil{t.Fatal(err)};_=conn.Close()
 if resp.OK||called{t.Fatalf("invalid request reached handler: %+v",resp)}
}
