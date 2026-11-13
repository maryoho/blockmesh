//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"blockmesh/internal/control"
	"blockmesh/internal/allocator"
	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
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
type discoveredVolume struct {
 Pool string `json:"pool"`
 PoolUUID string `json:"pool_uuid"`
 Device string `json:"device"`
 Index uint32 `json:"index"`
 Descriptor format.DynamicVolumeDescriptor `json:"descriptor"`
}

func (b *catalogLifecycleBackend) volumesForPool(ctx context.Context, pool discoveredPool) ([]discoveredVolume,error) {
 if err:=ctx.Err();err!=nil{return nil,err}
 parent,err:=device.OpenBlockDevice(pool.Device,false,false);if err!=nil{return nil,fmt.Errorf("open Pool LUN %s: %w",pool.Device,err)};defer parent.Close()
 bootstrap,err:=poolspace.OpenBootstrap(parent);if err!=nil{return nil,fmt.Errorf("open Pool %s Bootstrap: %w",pool.Name,err)}
 record:=bootstrap.Record();if fmt.Sprintf("%x",record.PoolUUID)!=pool.UUID||record.PoolName!=pool.Name{return nil,fmt.Errorf("Pool catalog identity changed while discovering Volumes on %s",pool.Device)}
 metadata,data,generation,err:=bootstrap.OpenViews();if err!=nil{return nil,fmt.Errorf("open Pool %s views: %w",pool.Name,err)};defer metadata.Close();if data!=nil{defer data.Close()}
 if generation==0{return nil,errors.New("Pool MetadataView has zero generation")}
 sb,err:=allocator.OpenDynamicSuperblockPair(metadata);if err!=nil{return nil,fmt.Errorf("open Pool %s superblock: %w",pool.Name,err)}
 if sb.PoolUUID!=record.PoolUUID||sb.ViewGeneration!=generation{return nil,fmt.Errorf("Pool %s superblock identity or MetadataView generation mismatch",pool.Name)}
 catalog,err:=metastore.OpenDynamicVolumeAllocatorReadOnly(metadata,sb);if err!=nil{return nil,fmt.Errorf("open Pool %s Volume catalog: %w",pool.Name,err)}
 entries,err:=catalog.ListVolumes();if err!=nil{return nil,fmt.Errorf("list Pool %s Volume descriptors: %w",pool.Name,err)}
 out:=make([]discoveredVolume,0,len(entries));for _,entry:=range entries{if err:=ctx.Err();err!=nil{return nil,err};out=append(out,discoveredVolume{Pool:pool.Name,PoolUUID:pool.UUID,Device:pool.Device,Index:entry.Index,Descriptor:entry.Descriptor})};return out,nil
}

func (b *catalogLifecycleBackend) scanVolumes(ctx context.Context)([]discoveredVolume,error) {
 pools,err:=b.scan(ctx);if err!=nil{return nil,err};out:=make([]discoveredVolume,0);ids:=make(map[string]string)
 for _,pool:=range pools{items,e:=b.volumesForPool(ctx,pool);if e!=nil{return nil,e};for _,item:=range items{if previous,ok:=ids[item.Descriptor.VolumeID];ok{return nil,fmt.Errorf("duplicate Volume UUID %s found on %s and %s",item.Descriptor.VolumeID,previous,item.Device)};ids[item.Descriptor.VolumeID]=item.Device};out=append(out,items...)}
 return out,nil
}
func (b *catalogLifecycleBackend) VolumeList(ctx context.Context)(any,error){items,err:=b.scanVolumes(ctx);if err!=nil{return nil,err};sort.Slice(items,func(i,j int)bool{if items[i].Pool!=items[j].Pool{return items[i].Pool<items[j].Pool};return items[i].Index<items[j].Index});return items,nil}
func (b *catalogLifecycleBackend) VolumeInfo(ctx context.Context,id string)(any,error){items,err:=b.scanVolumes(ctx);if err!=nil{return nil,err};for _,item:=range items{if item.Descriptor.VolumeID==id{return item,nil}};return nil,fmt.Errorf("Volume %q was not found in configured Pool devices",id)}
func (*catalogLifecycleBackend) unsupported(action string)(any,error){return nil,fmt.Errorf("%s is not available yet: production Pool/Volume mutation and runtime integration is still in progress",action)}
func(b *catalogLifecycleBackend) PoolCreate(context.Context,control.PoolCreateArgs)(any,error){return b.unsupported("pool create")}
func(b *catalogLifecycleBackend) PoolDestroy(context.Context,string)(any,error){return b.unsupported("pool destroy")}
func(b *catalogLifecycleBackend) VolumeCreate(context.Context,control.VolumeCreateArgs)(any,error){return b.unsupported("volume create")}
func(b *catalogLifecycleBackend) VolumeDelete(context.Context,string)(any,error){return b.unsupported("volume delete")}
func(b *catalogLifecycleBackend) VolumeActivate(context.Context,string,string)(any,error){return b.unsupported("volume activate")}
func(b *catalogLifecycleBackend) VolumeDeactivate(context.Context,string)(any,error){return b.unsupported("volume deactivate")}
