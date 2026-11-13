//go:build linux

package main

import (
	"context"
	"strings"
	"testing"

	"blockmesh/internal/control"
)

func TestCatalogLifecycleBackendMutationFailsClosed(t *testing.T) {
	b:=newCatalogLifecycleBackend(nil,nil)
	if _,err:=b.PoolCreate(context.Background(),control.PoolCreateArgs{Name:"p",Device:"/dev/sdb"});err==nil||!strings.Contains(err.Error(),"distributed sanlock"){t.Fatalf("expected Pool create to require distributed sanlock, got %v",err)}
	if _,err:=b.VolumeActivate(context.Background(),"vol-a","rw");err==nil||!strings.Contains(err.Error(),"Runtime manager is not configured"){t.Fatalf("expected explicit fail-closed error, got %v",err)}
	if _,err:=b.VolumeCreate(context.Background(),control.VolumeCreateArgs{Pool:"p",Name:"v",SizeBytes:4<<20});err==nil||!strings.Contains(err.Error(),"distributed sanlock"){t.Fatalf("expected create to require distributed sanlock, got %v",err)}
}
func TestDaemonConfigAllowsNoStaticVolumes(t *testing.T) {
	cfg:=validTestConfig();cfg.Volumes=nil
	if err:=cfg.validate();err!=nil{t.Fatalf("empty static volume list should be valid for catalog-only daemon: %v",err)}
}

func TestCatalogLifecycleBackendEmptyVolumeList(t *testing.T) {
 b:=newCatalogLifecycleBackend(nil,nil)
 got,err:=b.VolumeList(context.Background());if err!=nil{t.Fatalf("empty discovery list: %v",err)}
 items,ok:=got.([]discoveredVolume);if !ok||len(items)!=0{t.Fatalf("expected empty Volume catalog, got %#v",got)}
}
