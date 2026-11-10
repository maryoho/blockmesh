//go:build linux
package metaservice
import("context";"errors";"path/filepath";"sync";"testing";"time";"blockmesh/internal/uapi")
type fakeServiceDevice struct{mu sync.Mutex;reconcileErr error;runStarted chan struct{};runRelease chan struct{};closed bool}
func(d *fakeServiceDevice)Reconcile()error{d.mu.Lock();defer d.mu.Unlock();return d.reconcileErr}
func(d *fakeServiceDevice)Run(ctx context.Context)error{if d.runStarted!=nil{close(d.runStarted)};if d.runRelease==nil{<-ctx.Done();return ctx.Err()};select{case<-d.runRelease:return nil;case<-ctx.Done():return ctx.Err()}}
func(d *fakeServiceDevice)Close()error{d.mu.Lock();defer d.mu.Unlock();d.closed=true;return nil}
func newTestService(t *testing.T,lock string,dev *fakeServiceDevice,recoverFn func(context.Context)error)*Service{
 t.Helper();s,err:=NewService(ServiceConfig{DevicePath:"/dev/fake-blockmesh",LockPath:lock,Processor:ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),OpenDevice:func(string,Processor)(ServiceDevice,error){return dev,nil},Recover:recoverFn});if err!=nil{t.Fatal(err)};return s
}
func TestServiceStartReadinessAndSingletonLock(t *testing.T){
 lock:=filepath.Join(t.TempDir(),"run","metaservice.lock");first:=newTestService(t,lock,&fakeServiceDevice{},func(context.Context)error{return nil})
 if first.Ready(){t.Fatal("ready before Start")};if err:=first.Start(context.Background());err!=nil{t.Fatal(err)};if !first.Ready()||first.State()!=ServiceReady{t.Fatalf("state=%s",first.State())}
 second:=newTestService(t,lock,&fakeServiceDevice{},nil);if err:=second.Start(context.Background());err==nil{t.Fatal("second service acquired singleton lock")};if !first.Ready(){t.Fatal("duplicate start disturbed first service")}
 if err:=first.Shutdown();err!=nil{t.Fatal(err)};if first.State()!=ServiceClosed||first.Ready(){t.Fatalf("state=%s",first.State())}
 third:=newTestService(t,lock,&fakeServiceDevice{},nil);if err:=third.Start(context.Background());err!=nil{t.Fatalf("lock not released: %v",err)};if err:=third.Shutdown();err!=nil{t.Fatal(err)}
}
func TestServiceStartupFailureReleasesLockAndDevice(t *testing.T){
 lock:=filepath.Join(t.TempDir(),"metaservice.lock");dev:=&fakeServiceDevice{reconcileErr:errors.New("bad ring")};s:=newTestService(t,lock,dev,nil)
 if err:=s.Start(context.Background());err==nil{t.Fatal("expected startup failure")};if s.State()!=ServiceFailed||s.Ready(){t.Fatalf("state=%s",s.State())}
 dev.mu.Lock();closed:=dev.closed;dev.mu.Unlock();if !closed{t.Fatal("device not closed")}
 next:=newTestService(t,lock,&fakeServiceDevice{},nil);if err:=next.Start(context.Background());err!=nil{t.Fatalf("lock leaked: %v",err)};if err:=next.Shutdown();err!=nil{t.Fatal(err)}
}
func TestServiceRunShutdownWaitsForDevice(t *testing.T){
 lock:=filepath.Join(t.TempDir(),"metaservice.lock");dev:=&fakeServiceDevice{runStarted:make(chan struct{}),runRelease:make(chan struct{})};s:=newTestService(t,lock,dev,nil)
 runDone:=make(chan error,1);go func(){runDone<-s.Run(context.Background())}()
 select{case<-dev.runStarted:case<-time.After(time.Second):t.Fatal("Run did not start")}
 shutdownDone:=make(chan error,1);go func(){shutdownDone<-s.Shutdown()}()
 select{case err:=<-shutdownDone:if err!=nil{t.Fatal(err)};case<-time.After(time.Second):t.Fatal("Shutdown did not cancel and join Run")}
 if err:=<-runDone;err!=nil{t.Fatal(err)};if s.State()!=ServiceClosed{t.Fatalf("state=%s",s.State())}
}