//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"blockmesh/internal/control"
	"blockmesh/internal/device"
	"blockmesh/internal/poolspace"
)

// catalogLifecycleBackend exposes read-only Pool catalog discovery until the
// Pool/Volume runtime managers can provide real SAN-fenced mutation semantics.
type catalogLifecycleBackend struct { devices []string }

func newCatalogLifecycleBackend(paths []string) *catalogLifecycleBackend {
	return &catalogLifecycleBackend{devices: append([]string(nil), paths...)}
}

type discoveredPool struct {
	poolspace.PoolCatalogEntry
	Device string `json:"device"`
}

func (b *catalogLifecycleBackend) scan(ctx context.Context) ([]discoveredPool,error) {
	if b==nil { return nil,errors.New("Pool catalog backend is unavailable") }
	out:=make([]discoveredPool,0,len(b.devices))
	for _,path:=range b.devices {
		if err:=ctx.Err();err!=nil{return nil,err}
		dev,err:=device.OpenBlockDevice(path,false,false)
		if err!=nil{return nil,fmt.Errorf("open Pool discovery device %s: %w",path,err)}
		entry,readErr:=poolspace.ReadPoolCatalog(dev)
		closeErr:=dev.Close()
		if readErr!=nil||closeErr!=nil{return nil,errors.Join(fmt.Errorf("read Pool catalog on %s: %w",path,readErr),closeErr)}
		out=append(out,discoveredPool{PoolCatalogEntry:entry,Device:path})
	}
	sort.Slice(out,func(i,j int)bool{return strings.Compare(out[i].Name,out[j].Name)<0})
	for i:=1;i<len(out);i++ { if out[i-1].Name==out[i].Name{return nil,fmt.Errorf("duplicate Pool name %q discovered on %s and %s",out[i].Name,out[i-1].Device,out[i].Device)} }
	return out,nil
}
func (b *catalogLifecycleBackend) PoolList(ctx context.Context)(any,error){return b.scan(ctx)}
func (b *catalogLifecycleBackend) PoolInfo(ctx context.Context,name string)(any,error){
	pools,err:=b.scan(ctx);if err!=nil{return nil,err}
	for _,p:=range pools{if p.Name==name{return p,nil}}
	return nil,fmt.Errorf("Pool %q was not found in configured discovery devices",name)
}
func (*catalogLifecycleBackend) unsupported(action string)(any,error){return nil,fmt.Errorf("%s is not available yet: Pool/Volume mutation and runtime integration is still in progress",action)}
func(b *catalogLifecycleBackend) PoolCreate(context.Context,control.PoolCreateArgs)(any,error){return b.unsupported("pool create")}
func(b *catalogLifecycleBackend) PoolDestroy(context.Context,string)(any,error){return b.unsupported("pool destroy")}
func(b *catalogLifecycleBackend) VolumeCreate(context.Context,control.VolumeCreateArgs)(any,error){return b.unsupported("volume create")}
func(b *catalogLifecycleBackend) VolumeList(context.Context)(any,error){return b.unsupported("volume list")}
func(b *catalogLifecycleBackend) VolumeInfo(context.Context,string)(any,error){return b.unsupported("volume info")}
func(b *catalogLifecycleBackend) VolumeDelete(context.Context,string)(any,error){return b.unsupported("volume delete")}
func(b *catalogLifecycleBackend) VolumeActivate(context.Context,string,string)(any,error){return b.unsupported("volume activate")}
func(b *catalogLifecycleBackend) VolumeDeactivate(context.Context,string)(any,error){return b.unsupported("volume deactivate")}
