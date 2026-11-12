//go:build linux

package metaservice

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"blockmesh/internal/uapi"
)

func TestServiceStopsRuntimeBeforeClosingDevice(t *testing.T) {
	lockPath:=filepath.Join(t.TempDir(),"run","metaservice.lock")
	dev:=&fakeServiceDevice{}
	stopped:=false
	s,err:=NewService(ServiceConfig{
		DevicePath:"/dev/fake-blockmesh",LockPath:lockPath,
		Processor:ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),
		OpenDevice:func(string,Processor)(ServiceDevice,error){return dev,nil},
		StopRuntime:func(context.Context)error{
			dev.mu.Lock();defer dev.mu.Unlock()
			if dev.closed{return errors.New("device was closed before Runtime shutdown")}
			stopped=true
			return nil
		},
	})
	if err!=nil{t.Fatal(err)}
	if err=s.Start(context.Background());err!=nil{t.Fatal(err)}
	if err=s.Shutdown();err!=nil{t.Fatal(err)}
	dev.mu.Lock();closed:=dev.closed;dev.mu.Unlock()
	if !stopped||!closed{t.Fatalf("runtime stopped=%v device closed=%v",stopped,closed)}
}

func TestServiceRetainsDeviceAndLockWhenRuntimeDrainFails(t *testing.T) {
	lockPath:=filepath.Join(t.TempDir(),"run","metaservice.lock")
	dev:=&fakeServiceDevice{}
	allowStop:=false
	s,err:=NewService(ServiceConfig{
		DevicePath:"/dev/fake-blockmesh",LockPath:lockPath,
		Processor:ProcessorFunc(func(context.Context,uapi.Request)(Result,error){return Result{},nil}),
		OpenDevice:func(string,Processor)(ServiceDevice,error){return dev,nil},
		StopRuntime:func(context.Context)error{if !allowStop{return errors.New("DM drain failed")};return nil},
	})
	if err!=nil{t.Fatal(err)}
	if err=s.Start(context.Background());err!=nil{t.Fatal(err)}
	if err=s.Shutdown();err==nil{t.Fatal("expected Runtime drain failure")}
	dev.mu.Lock();closed:=dev.closed;dev.mu.Unlock()
	if closed{t.Fatal("device closed despite failed DM drain")}
	allowStop=true
	if err=s.Shutdown();err!=nil{t.Fatalf("retry shutdown: %v",err)}
	dev.mu.Lock();closed=dev.closed;dev.mu.Unlock()
	if !closed{t.Fatal("device was not closed after successful retry")}
}
