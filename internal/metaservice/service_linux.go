//go:build linux

package metaservice

import (
 "context"
 "errors"
 "fmt"
 "os"
 "path/filepath"
 "sync"
 "syscall"
 "time"
)

type ServiceState uint32
const (
 ServiceCreated ServiceState=iota
 ServiceOpening
 ServiceRecovering
 ServiceReady
 ServiceDraining
 ServiceClosed
 ServiceFailed
)
func(s ServiceState)String()string{switch s{case ServiceCreated:return"created";case ServiceOpening:return"opening";case ServiceRecovering:return"recovering";case ServiceReady:return"ready";case ServiceDraining:return"draining";case ServiceClosed:return"closed";case ServiceFailed:return"failed";default:return"unknown"}}

type ServiceDevice interface{Reconcile()error;Run(context.Context)error;Close()error}
type ServiceConfig struct{
 DevicePath string
 LockPath string
 Processor Processor
 RequestTimeout time.Duration
 Recover func(context.Context)error
 // StopRuntime fences and drains all active DM Volumes before closing /dev/blockmesh.
 StopRuntime func(context.Context)error
 ShutdownTimeout time.Duration
 OpenDevice func(string,Processor)(ServiceDevice,error)
}
type Service struct{
 mu sync.Mutex
 shutdownMu sync.Mutex
 stopMu sync.Mutex
 cfg ServiceConfig
 state ServiceState
 lockFile *os.File
 device ServiceDevice
 cancel context.CancelFunc
 done chan struct{}
 runtimeStopped bool
}
func NewService(cfg ServiceConfig)(*Service,error){
 if cfg.DevicePath==""||cfg.LockPath==""||cfg.Processor==nil{return nil,errors.New("device path, lock path and processor are required")}
 if cfg.RequestTimeout<0||cfg.ShutdownTimeout<0{return nil,errors.New("request and shutdown timeouts cannot be negative")}
 if cfg.RequestTimeout==0{cfg.RequestTimeout=DefaultRequestTimeout}
 if cfg.ShutdownTimeout==0{cfg.ShutdownTimeout=30*time.Second}
 return &Service{cfg:cfg,state:ServiceCreated},nil
}
func(s *Service)State()ServiceState{s.mu.Lock();defer s.mu.Unlock();return s.state}
func(s *Service)Ready()bool{return s.State()==ServiceReady}

func(s *Service)Start(ctx context.Context)error{
 if ctx==nil{return errors.New("service start context is required")}
 s.mu.Lock();defer s.mu.Unlock()
 if s.state!=ServiceCreated{return fmt.Errorf("cannot start service in state %s",s.state)}
 s.state=ServiceOpening
 if err:=os.MkdirAll(filepath.Dir(s.cfg.LockPath),0750);err!=nil{s.state=ServiceFailed;return fmt.Errorf("create lock directory: %w",err)}
 lf,err:=os.OpenFile(s.cfg.LockPath,os.O_CREATE|os.O_RDWR,0600);if err!=nil{s.state=ServiceFailed;return fmt.Errorf("open service lock: %w",err)}
 if err=syscall.Flock(int(lf.Fd()),syscall.LOCK_EX|syscall.LOCK_NB);err!=nil{_=lf.Close();s.state=ServiceFailed;return fmt.Errorf("metadata service lock %q is already held: %w",s.cfg.LockPath,err)}
 s.lockFile=lf
 opener:=s.cfg.OpenDevice
 if opener==nil{opener=func(path string,p Processor)(ServiceDevice,error){return OpenDeviceWithTimeout(path,p,s.cfg.RequestTimeout)}}
 dev,err:=opener(s.cfg.DevicePath,s.cfg.Processor);if err!=nil{s.failLocked();return fmt.Errorf("open metadata device: %w",err)}
 if dev==nil{s.failLocked();return errors.New("device opener returned nil device")}
 s.device=dev
 s.state=ServiceRecovering
 if err=dev.Reconcile();err!=nil{s.failLocked();return fmt.Errorf("initial queue reconciliation: %w",err)}
 if s.cfg.Recover!=nil{
  if err=s.cfg.Recover(ctx);err!=nil{
   stopErr:=s.stopRuntime()
   if stopErr!=nil{s.state=ServiceFailed;return errors.Join(fmt.Errorf("runtime recovery: %w",err),fmt.Errorf("stop partial Runtime activation: %w",stopErr))}
   s.failLocked();return fmt.Errorf("runtime recovery: %w",err)
  }
 }
 if err=ctx.Err();err!=nil{
  stopErr:=s.stopRuntime()
  if stopErr!=nil{s.state=ServiceFailed;return errors.Join(err,fmt.Errorf("stop Runtime after canceled startup: %w",stopErr))}
  s.failLocked();return err
 }
 s.state=ServiceReady
 return nil
}

func(s *Service)Run(ctx context.Context)error{
 if err:=s.Start(ctx);err!=nil{return err}
 s.mu.Lock()
 if s.state!=ServiceReady{state:=s.state;s.mu.Unlock();return fmt.Errorf("cannot run service in state %s",state)}
 runCtx,cancel:=context.WithCancel(ctx);done:=make(chan struct{});s.cancel=cancel;s.done=done;dev:=s.device
 s.mu.Unlock()
 runErr:=dev.Run(runCtx);cancel()
 s.mu.Lock();close(done);s.done=nil;s.cancel=nil
 if s.state==ServiceDraining{s.mu.Unlock();return nil}
 if runErr!=nil&&!errors.Is(runErr,context.Canceled)&&ctx.Err()==nil{
  s.state=ServiceDraining;s.mu.Unlock()
  stopErr:=s.stopRuntime()
  s.mu.Lock()
  if stopErr!=nil{s.state=ServiceFailed;s.mu.Unlock();return errors.Join(runErr,fmt.Errorf("stop active Volume runtimes after device failure: %w",stopErr))}
  cleanupErr:=s.cleanupLocked();if cleanupErr!=nil{s.state=ServiceFailed}else{s.state=ServiceClosed}
  s.mu.Unlock();return errors.Join(runErr,cleanupErr)
 }
 s.mu.Unlock()
 return s.Shutdown()
}

func(s *Service)stopRuntime()error{
 s.stopMu.Lock();defer s.stopMu.Unlock()
 if s.runtimeStopped{return nil}
 if s.cfg.StopRuntime==nil{s.runtimeStopped=true;return nil}
 ctx,cancel:=context.WithTimeout(context.Background(),s.cfg.ShutdownTimeout);defer cancel()
 if err:=s.cfg.StopRuntime(ctx);err!=nil{return err}
 s.runtimeStopped=true
 return nil
}

func(s *Service)Shutdown()error{
 if s==nil{return nil}
 s.shutdownMu.Lock();defer s.shutdownMu.Unlock()
 s.mu.Lock()
 if s.state==ServiceClosed{s.mu.Unlock();return nil}
 if s.state==ServiceCreated{s.state=ServiceClosed;s.mu.Unlock();return nil}
 if s.state==ServiceReady{s.state=ServiceDraining}
 cancel,done:=s.cancel,s.done
 s.mu.Unlock()
 if cancel!=nil{cancel()}
 if done!=nil{<-done}
 if err:=s.stopRuntime();err!=nil{
  s.mu.Lock();s.state=ServiceFailed;s.mu.Unlock()
  // Retain the char device and singleton lock until DM admission is drained.
  return fmt.Errorf("stop active Volume runtimes: %w",err)
 }
 s.mu.Lock();defer s.mu.Unlock()
 if s.state==ServiceClosed{return nil}
 if err:=s.cleanupLocked();err!=nil{s.state=ServiceFailed;return err}
 s.state=ServiceClosed
 return nil
}
func(s *Service)failLocked(){s.state=ServiceFailed;_=s.cleanupLocked()}
func(s *Service)cleanupLocked()error{
 var errs []error
 if s.device!=nil{if err:=s.device.Close();err!=nil{errs=append(errs,err)};s.device=nil}
 if s.lockFile!=nil{if err:=syscall.Flock(int(s.lockFile.Fd()),syscall.LOCK_UN);err!=nil{errs=append(errs,err)};if err:=s.lockFile.Close();err!=nil{errs=append(errs,err)};s.lockFile=nil}
 return errors.Join(errs...)
}
