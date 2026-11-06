package main

import (
	"encoding/json"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blockmesh/internal/device"
	"blockmesh/internal/lock"
	"blockmesh/internal/poolspace"
)

type poolConfig struct {
	DevicePath string `json:"device_path"`
	VolumeSlots uint32 `json:"volume_slots"`
	LogicalSectorBytes uint32 `json:"logical_sector_bytes"`
	Lockspace string `json:"lockspace"`
	LockDevice string `json:"lock_device"`
	LockOffsetBytes uint64 `json:"lock_offset_bytes"`
	SanlockBinary string `json:"sanlock_binary"`
	LockHelperBinary string `json:"lock_helper_binary"`
	ReadyDir string `json:"ready_dir"`
	MetadataUsageFile string `json:"metadata_usage_file"`
	MetadataUsageMaxAgeSeconds int64 `json:"metadata_usage_max_age_seconds"`
}

type usageSnapshot struct {
	UsedBytes uint64 `json:"used_bytes"`
	GrowthBytes uint64 `json:"growth_bytes"`
	ObservedAt time.Time `json:"observed_at"`
}

type poolStatus struct {
	PoolID string `json:"pool_id"`
	PoolUUID string `json:"pool_uuid"`
	Generation uint64 `json:"generation"`
	DeviceBytes uint64 `json:"device_bytes"`
	BootstrapBytes uint64 `json:"bootstrap_bytes"`
	MetadataBytes uint64 `json:"metadata_bytes"`
	FreeBytes uint64 `json:"free_bytes"`
	DataBytes uint64 `json:"data_bytes"`
	LogicalSectorBytes uint32 `json:"logical_sector_bytes"`
	ExtentCount int `json:"extent_count"`
}

func main(){
	if len(os.Args)<2||os.Args[1]!="pool"{usage();os.Exit(2)}
	if err:=runPool(os.Args[2:]);err!=nil{fmt.Fprintln(os.Stderr,"bmctl:",err);os.Exit(1)}
}
func usage(){fmt.Fprintln(os.Stderr,"Usage: bmctl pool <format|rescan|metadata-expand|metadata-expand-status> --pool <id> [--config-dir /etc/blockmesh/pools]")}
func runPool(args []string)error{
	if len(args)==0{usage();return errors.New("pool operation is required")}
	op:=args[0]
	fs:=flag.NewFlagSet("pool "+op,flag.ContinueOnError)
	poolID:=fs.String("pool","","Pool identifier")
	configDir:=fs.String("config-dir","/etc/blockmesh/pools","Pool configuration directory")
	confirm:=fs.Bool("confirm-format",false,"confirm creation of a new BMPOOL03 Pool")
	if err:=fs.Parse(args[1:]);err!=nil{return err}
	if fs.NArg()!=0{return errors.New("unexpected positional arguments")}
	if strings.TrimSpace(*poolID)==""||strings.ContainsAny(*poolID,"/\\:"){return errors.New("a valid --pool identifier is required")}
	cfg,err:=loadConfig(*configDir,*poolID);if err!=nil{return err}
	switch op{
	case "format":
		if !*confirm{return errors.New("refusing destructive Pool format without --confirm-format")}
		return formatPool(*poolID,cfg)
	case "rescan":
		return rescanPool(*poolID,cfg)
	case "metadata-expand":
		return expandPoolMetadata(*poolID,cfg)
	case "metadata-expand-status":
		return statusPool(*poolID,cfg)
	default:
		usage();return fmt.Errorf("unknown Pool operation %q",op)
	}
}
func loadConfig(dir,id string)(poolConfig,error){
	var c poolConfig
	path:=filepath.Join(dir,id+".json")
	b,err:=os.ReadFile(path);if err!=nil{return c,fmt.Errorf("read Pool config %s: %w",path,err)}
	if err=json.Unmarshal(b,&c);err!=nil{return c,fmt.Errorf("decode Pool config: %w",err)}
	if c.DevicePath==""||c.Lockspace==""||c.LockDevice==""||c.LockOffsetBytes==0{return c,errors.New("Pool config requires device_path, lockspace, lock_device and non-zero lock_offset_bytes")}
	if c.LogicalSectorBytes==0{c.LogicalSectorBytes=512}
	if c.MetadataUsageMaxAgeSeconds<=0{c.MetadataUsageMaxAgeSeconds=3600}
	return c,nil
}
func newSanlock(c poolConfig)*lock.SanlockLocker{
	return &lock.SanlockLocker{Binary:c.SanlockBinary,HelperBinary:c.LockHelperBinary,ReadyDir:c.ReadyDir,
		ResourceFor:func(resource string)(string,error){
			name:=strings.NewReplacer(":","-","/","-").Replace(resource)
			return lock.SanlockResource(c.Lockspace,name,c.LockDevice,c.LockOffsetBytes)
		}}
}
func openPoolDevice(c poolConfig,writable bool)(*device.BlockDevice,error){return device.OpenBlockDevice(c.DevicePath,writable,false)}
func formatPool(id string,c poolConfig)error{
	dev,err:=openPoolDevice(c,true);if err!=nil{return err};defer dev.Close()
	locker:=newSanlock(c)
	resource:="blockmesh:pool:"+id+":format"
	ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second);defer cancel()
	release,err:=locker.Lock(ctx,resource);if err!=nil{return fmt.Errorf("acquire distributed format lock: %w",err)}
	store,formatErr:=poolspace.FormatBootstrap(dev,c.VolumeSlots,c.LogicalSectorBytes)
	releaseErr:=release()
	if formatErr!=nil{return formatErr};if releaseErr!=nil{return releaseErr}
	return printStatus(id,store)
}
func openStore(c poolConfig,writable bool)(*device.BlockDevice,*poolspace.BootstrapStore,error){
	dev,err:=openPoolDevice(c,writable);if err!=nil{return nil,nil,err}
	store,err:=poolspace.OpenPoolSpace(dev);if err!=nil{dev.Close();return nil,nil,err}
	if writable{if err=store.SetCommitLocker(newSanlock(c));err!=nil{dev.Close();return nil,nil,err}}
	return dev,store,nil
}
func rescanPool(id string,c poolConfig)error{
	dev,store,err:=openStore(c,true);if err!=nil{return err};defer dev.Close()
	actual,err:=store.Rescan();if err!=nil{return err}
	fmt.Printf("{\"pool_id\":%q,\"device_bytes\":%d,\"generation\":%d}\n",id,actual,store.Record().Generation)
	return nil
}
func expandPoolMetadata(id string,c poolConfig)error{
	if c.MetadataUsageFile==""{return errors.New("metadata_usage_file is required; usage telemetry must be supplied by the metadata service")}
	b,err:=os.ReadFile(c.MetadataUsageFile);if err!=nil{return fmt.Errorf("read metadata usage snapshot: %w",err)}
	var usage usageSnapshot;if err=json.Unmarshal(b,&usage);err!=nil{return fmt.Errorf("decode metadata usage snapshot: %w",err)}
	if usage.ObservedAt.IsZero()||time.Since(usage.ObservedAt)>time.Duration(c.MetadataUsageMaxAgeSeconds)*time.Second||usage.ObservedAt.After(time.Now().Add(5*time.Minute)){
		return errors.New("metadata usage snapshot is missing, stale, or from the future")
	}
	dev,store,err:=openStore(c,true);if err!=nil{return err};defer dev.Close()
	target,err:=store.ExpandMetadata(usage.UsedBytes,usage.GrowthBytes);if err!=nil{return err}
	fmt.Printf("{\"pool_id\":%q,\"metadata_bytes\":%d,\"generation\":%d}\n",id,target,store.Record().Generation)
	return nil
}
func statusPool(id string,c poolConfig)error{
	dev,store,err:=openStore(c,false);if err!=nil{return err};defer dev.Close()
	return printStatus(id,store)
}
func printStatus(id string,s *poolspace.BootstrapStore)error{
	r:=s.Record();var free,data uint64
	for _,e:=range s.Extents(){switch e.Kind{case poolspace.Free:free+=e.Length;case poolspace.Data:data+=e.Length}}
	out:=poolStatus{PoolID:id,PoolUUID:fmt.Sprintf("%x",r.PoolUUID),Generation:r.Generation,DeviceBytes:r.DeviceBytes,
		BootstrapBytes:r.BootstrapBytes,MetadataBytes:r.MetadataBytes,FreeBytes:free,DataBytes:data,
		LogicalSectorBytes:r.LogicalSectorBytes,ExtentCount:len(s.Extents())}
	return json.NewEncoder(os.Stdout).Encode(out)
}
