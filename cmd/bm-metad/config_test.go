//go:build linux

package main

import "testing"

func validTestConfig() daemonConfig {
 return daemonConfig{
  NodeID:"node-a",BootID:"boot-a",
  Service:serviceConfig{DevicePath:"/dev/blockmesh",LockPath:"/run/blockmesh/metad.lock"},
  Sanlock:sanlockConfig{Lockspace:"bm",LockDevice:"/dev/mapper/lock",ResourceOffsets:map[string]uint64{
   "owner/vol-a":512,"reader-set/vol-a":1024,"reader/vol-a/0":1536,"wal/00112233445566778899aabbccddeeff/0":2048,
  }},
  SAN:sanConfig{Paths:[]prPathConfig{{Device:"/dev/sda",LocalKey:11}},OwnerDrainHelper:"/usr/local/bin/owner-drain"},
  Volumes:[]volumeConfig{{VolumeID:"vol-a",DevicePath:"/dev/mapper/mpatha",DMDevice:"bm-vol-a",Mode:"rw"}},
 }
}
func TestDaemonConfigValidation(t *testing.T){if err:=validTestConfig().validate();err!=nil{t.Fatalf("valid config rejected: %v",err)}}
func TestDaemonConfigRequiresPerVolumeResources(t *testing.T){
 cfg:=validTestConfig();delete(cfg.Sanlock.ResourceOffsets,"owner/vol-a")
 if err:=cfg.validate();err==nil{t.Fatal("expected missing Owner sanlock resource rejection")}
}
func TestDaemonConfigRejectsDuplicateSanlockOffsets(t *testing.T){
 cfg:=validTestConfig();cfg.Sanlock.ResourceOffsets["reader/vol-a/0"]=512
 if err:=cfg.validate();err==nil{t.Fatal("expected duplicate sanlock offset rejection")}
}
func TestDaemonConfigRequiresROReaderEpoch(t *testing.T){
 cfg:=validTestConfig();cfg.Volumes[0].Mode="ro"
 if err:=cfg.validate();err==nil{t.Fatal("expected missing ReaderEpoch rejection")}
 cfg.Volumes[0].ReaderEpoch=7
 if err:=cfg.validate();err!=nil{t.Fatalf("valid RO config rejected: %v",err)}
}

func TestDaemonConfigSupportsDynamicResourcesWithoutStaticVolumes(t *testing.T) {
 cfg:=validTestConfig();cfg.Volumes=nil;cfg.Sanlock.ResourceOffsets=nil
 cfg.Sanlock.DynamicResourceBase=1<<20;cfg.Sanlock.DynamicResourceSlots=1<<16
 if err:=cfg.validate();err!=nil{t.Fatalf("dynamic resource config rejected without static Volumes: %v",err)}
}

func TestDaemonConfigRejectsStaticDynamicResourceOverlap(t *testing.T) {
 cfg:=validTestConfig();cfg.Sanlock.DynamicResourceBase=1<<20;cfg.Sanlock.DynamicResourceSlots=16
 cfg.Sanlock.ResourceOffsets["fixed"]=cfg.Sanlock.DynamicResourceBase+512
 if err:=cfg.validate();err==nil{t.Fatal("expected static resource overlap rejection")}
}
