//go:build linux

package main

import (
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "strings"
 "time"

)

type daemonConfig struct {
 NodeID string `json:"node_id"`
 BootID string `json:"boot_id"`
 Service serviceConfig `json:"service"`
 Control controlConfig `json:"control"`
 DiscoveryDevices []string `json:"discovery_devices"`
 Sanlock sanlockConfig `json:"sanlock"`
 SAN sanConfig `json:"san"`
 Runtime runtimeConfig `json:"runtime"`
 Volumes []volumeConfig `json:"volumes"`
}
type serviceConfig struct{DevicePath string `json:"device_path"`;LockPath string `json:"lock_path"`;RequestTimeout string `json:"request_timeout"`;ShutdownTimeout string `json:"shutdown_timeout"`}
type controlConfig struct{SocketPath string `json:"socket_path"`}
type sanlockConfig struct{Binary string `json:"binary"`;HelperBinary string `json:"helper_binary"`;ReadyDir string `json:"ready_dir"`;Lockspace string `json:"lockspace"`;LockDevice string `json:"lock_device"`;ResourceOffsets map[string]uint64 `json:"resource_offsets"`;ResourceIndexOffset uint64 `json:"resource_index_offset"`;ResourceAlignmentBytes uint64 `json:"resource_alignment_bytes"`}
type sanConfig struct{SGPersistBinary string `json:"sg_persist_binary"`;ReservationType uint8 `json:"reservation_type"`;Paths []prPathConfig `json:"paths"`;OwnerKeys map[string][]uint64 `json:"owner_keys"`;OwnerDrainHelper string `json:"owner_drain_helper"`}
type prPathConfig struct{Device string `json:"device"`;LocalKey uint64 `json:"local_key"`}
type runtimeConfig struct{ReaderHeartbeatInterval string `json:"reader_heartbeat_interval"`}
type volumeConfig struct{VolumeID string `json:"volume_id"`;DevicePath string `json:"device_path"`;DMDevice string `json:"dm_device"`;VolumeIndex uint32 `json:"volume_index"`;Mode string `json:"mode"`;ReaderSlot uint32 `json:"reader_slot"`;ReaderEpoch uint64 `json:"reader_epoch"`}

func loadConfig(path string)(daemonConfig,error){
 var cfg daemonConfig
 f,err:=os.Open(path);if err!=nil{return cfg,err};defer f.Close()
 dec:=json.NewDecoder(f);dec.DisallowUnknownFields()
 if err:=dec.Decode(&cfg);err!=nil{return cfg,fmt.Errorf("decode config: %w",err)}
 if err:=cfg.validate();err!=nil{return cfg,err}
 return cfg,nil
}
func(c daemonConfig)validate()error{
 if strings.TrimSpace(c.NodeID)==""||strings.TrimSpace(c.BootID)==""{return errors.New("node_id and boot_id are required")}
 if strings.TrimSpace(c.Service.DevicePath)==""||strings.TrimSpace(c.Service.LockPath)==""{return errors.New("service.device_path and service.lock_path are required")}
 if _,err:=parseDuration("service.request_timeout",c.Service.RequestTimeout,30*time.Second);err!=nil{return err}
 if _,err:=parseDuration("service.shutdown_timeout",c.Service.ShutdownTimeout,30*time.Second);err!=nil{return err}
 if _,err:=parseDuration("runtime.reader_heartbeat_interval",c.Runtime.ReaderHeartbeatInterval,250*time.Millisecond);err!=nil{return err}
 if strings.TrimSpace(c.Sanlock.Lockspace)==""||strings.TrimSpace(c.Sanlock.LockDevice)==""{return errors.New("sanlock.lockspace and sanlock.lock_device are required")}
 rindexEnabled:=c.Sanlock.ResourceIndexOffset!=0
 if rindexEnabled { if c.Sanlock.ResourceAlignmentBytes!=1<<20&&c.Sanlock.ResourceAlignmentBytes!=2<<20&&c.Sanlock.ResourceAlignmentBytes!=4<<20&&c.Sanlock.ResourceAlignmentBytes!=8<<20{return errors.New("sanlock.resource_alignment_bytes must be 1, 2, 4 or 8 MiB when resource_index_offset is configured")};if c.Sanlock.ResourceIndexOffset%c.Sanlock.ResourceAlignmentBytes!=0{return errors.New("sanlock.resource_index_offset must align to resource_alignment_bytes")} } else if len(c.Sanlock.ResourceOffsets)==0{return errors.New("configure sanlock.resource_offsets or sanlock.resource_index_offset for dynamic resource allocation") }
 used:=map[uint64]string{}
resourceAlignment:=uint64(512);if rindexEnabled{resourceAlignment=c.Sanlock.ResourceAlignmentBytes}
var rindexEnd uint64
if rindexEnabled {
 if c.Sanlock.ResourceIndexOffset>^uint64(0)-2*resourceAlignment{return errors.New("sanlock RINDEX region overflows address space")}
 rindexEnd=c.Sanlock.ResourceIndexOffset+2*resourceAlignment
}
 for name,off:=range c.Sanlock.ResourceOffsets{
  if strings.TrimSpace(name)==""||off==0||off%resourceAlignment!=0{return fmt.Errorf("invalid sanlock resource offset for %q: offsets must be nonzero and aligned to the configured sanlock resource alignment",name)}
  
  if rindexEnabled&&off>=c.Sanlock.ResourceIndexOffset&&off<rindexEnd{return fmt.Errorf("static sanlock resource %q overlaps the RINDEX metadata region",name)}
  if prev,ok:=used[off];ok{return fmt.Errorf("sanlock resources %q and %q share offset %d",prev,name,off)}
  used[off]=name
 }
 if len(c.SAN.Paths)==0{return errors.New("san.paths must contain every local SAN path")}
 devices:=map[string]bool{};localKeys:=map[uint64]bool{}
 for _,p:=range c.SAN.Paths{
  if strings.TrimSpace(p.Device)==""||p.LocalKey==0||devices[p.Device]||localKeys[p.LocalKey]{return errors.New("SAN paths require unique device paths and unique nonzero local keys")}
  devices[p.Device],localKeys[p.LocalKey]=true,true
 }
 if c.SAN.ReservationType!=0&&c.SAN.ReservationType!=1&&c.SAN.ReservationType!=3&&c.SAN.ReservationType!=5&&c.SAN.ReservationType!=6{return errors.New("san.reservation_type must be one of 1, 3, 5 or 6")}
 if strings.TrimSpace(c.SAN.OwnerDrainHelper)==""{return errors.New("san.owner_drain_helper is required; PR fencing alone is not an old-owner I/O drain proof")}
 for identity,ownerKeys:=range c.SAN.OwnerKeys{
  if !strings.Contains(identity,"/")||len(ownerKeys)==0{return fmt.Errorf("invalid SAN owner key entry %q",identity)}
  seen:=map[uint64]bool{}
  for _,key:=range ownerKeys{if key==0||localKeys[key]||seen[key]{return fmt.Errorf("invalid or colliding PR key %x for %s",key,identity)};seen[key]=true}
 }
 seenDevices:=map[string]bool{};for _,path:=range c.DiscoveryDevices{if strings.TrimSpace(path)==""||seenDevices[path]{return fmt.Errorf("discovery_devices contains an empty or duplicate path %q",path)};seenDevices[path]=true}
 ids:=map[string]bool{};dmNames:=map[string]bool{}
 for _,v:=range c.Volumes{
  if strings.TrimSpace(v.VolumeID)==""||strings.TrimSpace(v.DevicePath)==""||strings.TrimSpace(v.DMDevice)==""{return errors.New("each volume requires volume_id, device_path and dm_device")}
  if ids[v.VolumeID]||dmNames[v.DMDevice]{return fmt.Errorf("duplicate Volume ID or DM device for %q",v.VolumeID)}
  ids[v.VolumeID],dmNames[v.DMDevice]=true,true
  required:=[]string{"owner/"+v.VolumeID,"reader-set/"+v.VolumeID,fmt.Sprintf("reader/%s/%d",v.VolumeID,v.ReaderSlot)}
  if !rindexEnabled { for _,name:=range required{if _,ok:=c.Sanlock.ResourceOffsets[name];!ok{return fmt.Errorf("sanlock.resource_offsets is missing required resource %q",name)}} }
  switch v.Mode{case "rw":if v.ReaderEpoch!=0{return fmt.Errorf("RW Volume %s must not configure reader_epoch",v.VolumeID)}
  case "ro":if v.ReaderEpoch==0{return fmt.Errorf("RO Volume %s requires nonzero reader_epoch",v.VolumeID)}
  default:return fmt.Errorf("Volume %s mode must be rw or ro",v.VolumeID)}
 }
 return nil
}
func parseDuration(name,value string,fallback time.Duration)(time.Duration,error){
 if strings.TrimSpace(value)==""{return fallback,nil}
 d,err:=time.ParseDuration(value);if err!=nil||d<=0{return 0,fmt.Errorf("%s must be a positive Go duration",name)}
 return d,nil
}
