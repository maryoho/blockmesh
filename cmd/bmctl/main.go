package main

import (
	"encoding/json"
	"context"
	"bufio"
	"io"
	"os/exec"
	"sync"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blockmesh/internal/allocator"
	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
	"blockmesh/internal/lock"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/wal"
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
	OwnerLeaseHelper string `json:"owner_lease_helper"`
	MetadataUsageFile string `json:"metadata_usage_file"`
	MetadataUsageMaxAgeSeconds int64 `json:"metadata_usage_max_age_seconds"`
	AGSizeBytes uint64 `json:"ag_size_bytes"`
	AGCapacity uint32 `json:"ag_capacity"`
	WALSegmentBytes uint64 `json:"wal_segment_bytes"`
	MappingBytes uint64 `json:"mapping_bytes"`
	CheckpointBytes uint64 `json:"checkpoint_bytes"`
	InitialArenaPages uint64 `json:"initial_arena_pages"`
	LifecyclePages uint64 `json:"lifecycle_pages"`
	ArenaGrowthPages uint64 `json:"arena_growth_pages"`
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
func usage(){fmt.Fprintln(os.Stderr,"Usage: bmctl pool <format|dynamic-format|dynamic-status|dynamic-recover|rescan|metadata-expand|metadata-expand-status> --pool <id> [--config-dir /etc/blockmesh/pools]")}
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
	case "dynamic-format":
		if !*confirm{return errors.New("refusing destructive BMPOOL03 format without --confirm-format")}
		return dynamicFormatPool(*poolID,cfg)
	case "dynamic-status":
		return dynamicStatusPool(*poolID,cfg)
	case "dynamic-recover":
		return dynamicRecoverPool(*poolID,cfg)
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
type ownerLeaseReady struct { Epoch uint64 `json:"epoch"` }
type helperOwnerLease struct { cmd *exec.Cmd; stdin io.WriteCloser; done chan struct{}; mu sync.Mutex; waitErr error; epoch uint64; resource string }
func acquireOwnerLease(c poolConfig,poolID string)(*helperOwnerLease,error){
 if strings.TrimSpace(c.OwnerLeaseHelper)==""{return nil,errors.New("owner_lease_helper is required for Pool metadata mutation")}
 cmd:=exec.Command(c.OwnerLeaseHelper,"--pool",poolID,"--device",c.DevicePath)
 stdout,err:=cmd.StdoutPipe();if err!=nil{return nil,err}
 stdin,err:=cmd.StdinPipe();if err!=nil{return nil,err}
 cmd.Stderr=os.Stderr
 if err=cmd.Start();err!=nil{return nil,fmt.Errorf("start Owner lease helper: %w",err)}
 lease:=&helperOwnerLease{cmd:cmd,stdin:stdin,done:make(chan struct{}),resource:"volume:"+poolID}
 go func(){err:=cmd.Wait();lease.mu.Lock();lease.waitErr=err;lease.mu.Unlock();close(lease.done)}()
 var ready ownerLeaseReady
 if err=json.NewDecoder(bufio.NewReader(stdout)).Decode(&ready);err!=nil{_ = lease.Close();return nil,fmt.Errorf("read Owner lease helper readiness: %w",err)}
 if ready.Epoch==0{_ = lease.Close();return nil,errors.New("Owner lease helper returned zero OwnerEpoch")}
 lease.epoch=ready.Epoch
 if !lease.Valid(){return nil,errors.New("Owner lease helper exited immediately after readiness")}
 return lease,nil
}
func(l *helperOwnerLease)Epoch()uint64{if l==nil{return 0};return l.epoch}
func(l *helperOwnerLease)Resource()string{if l==nil{return ""};return l.resource}
func(l *helperOwnerLease)VolumeID()string{if l==nil{return ""};return strings.TrimPrefix(l.resource,"volume:")}
func(l *helperOwnerLease)Valid()bool{if l==nil{return false};select{case <-l.done:return false;default:return true}}
func(l *helperOwnerLease)Close()error{
 if l==nil{return nil}
 if l.stdin!=nil{_ = l.stdin.Close()}
 select{case <-l.done:l.mu.Lock();defer l.mu.Unlock();return l.waitErr
 case <-time.After(5*time.Second):_ = l.cmd.Process.Kill();<-l.done;l.mu.Lock();defer l.mu.Unlock();return l.waitErr}
}
func openStore(c poolConfig,poolID string,writable bool)(*device.BlockDevice,*poolspace.BootstrapStore,*helperOwnerLease,error){
 dev,err:=openPoolDevice(c,writable);if err!=nil{return nil,nil,nil,err}
 store,err:=poolspace.OpenPoolSpace(dev);if err!=nil{dev.Close();return nil,nil,nil,err}
 if !writable{return dev,store,nil,nil}
 if err=store.SetCommitLocker(newSanlock(c));err!=nil{dev.Close();return nil,nil,nil,err}
 owner,err:=acquireOwnerLease(c,poolID);if err!=nil{dev.Close();return nil,nil,nil,err}
 if err=store.SetOwnerLease(owner);err!=nil{_ = owner.Close();dev.Close();return nil,nil,nil,err}
 return dev,store,owner,nil
}
type dynamicPoolStatus struct {
	PoolID string `json:"pool_id"`
	PoolUUID string `json:"pool_uuid"`
	ViewGeneration uint64 `json:"view_generation"`
	SuperblockGeneration uint64 `json:"superblock_generation"`
	MetadataBytes uint64 `json:"metadata_bytes"`
	DataBytes uint64 `json:"data_bytes"`
	AGCount uint32 `json:"ag_count"`
	AGCapacity uint32 `json:"ag_capacity"`
	WALSegmentBytes uint64 `json:"wal_segment_bytes"`
	ReservedAllocator bool `json:"reserved_allocator"`
}

func dynamicGeometryConfig(c poolConfig,metadataBytes,dataBytes uint64)(format.DynamicGeometryConfig,error){
	agSize:=c.AGSizeBytes;if agSize==0{agSize=1<<40}
	walBytes:=c.WALSegmentBytes;if walBytes==0{walBytes=4<<20}
	mappingBytes:=c.MappingBytes;if mappingBytes==0{mappingBytes=8<<20}
	agCapacity:=c.AGCapacity;if agCapacity==0{agCapacity=8}
	checkpointBytes:=c.CheckpointBytes
	minCheckpoint:=uint64(agCapacity)*2*format.PageSize
	if checkpointBytes==0{checkpointBytes=minCheckpoint}
	return format.DynamicGeometryConfig{DataBytes:dataBytes,MetadataBytes:metadataBytes,AGSizeBytes:agSize,
		AGCapacity:agCapacity,WALSegmentBytes:walBytes,MappingBytes:mappingBytes,CheckpointBytes:checkpointBytes},nil
}

func dynamicFormatPool(id string,c poolConfig)error{
	dev,bootstrap,owner,err:=openStore(c,id,true);if err!=nil{return err}
	defer dev.Close();defer owner.Close()
	metadata,data,viewGeneration,err:=bootstrap.OpenViews();if err!=nil{return err}
	if data==nil||data.Size()<=0{return errors.New("Pool has no DataView capacity; allocate Data extents before BMPOOL03 format")}
	cfg,err:=dynamicGeometryConfig(c,uint64(metadata.Size()),uint64(data.Size()));if err!=nil{return err}
	g,err:=format.PlanDynamicGeometry(cfg);if err!=nil{return fmt.Errorf("plan BMPOOL03 geometry: %w",err)}
	record:=bootstrap.Record()
	sb,err:=format.NewDynamicSuperblock(g,record.PoolUUID,1,viewGeneration);if err!=nil{return err}
	if err=allocator.FormatDynamicPool(metadata,sb);err!=nil{return err}
	if err=metastore.FormatDynamicVolumeAllocator(metadata,sb);err!=nil{return err}
	volumeAllocator,err:=metastore.OpenDynamicVolumeAllocator(metadata,sb);if err!=nil{return err}
	lifecyclePages:=c.LifecyclePages;if lifecyclePages==0{lifecyclePages=8}
	arenaPages:=c.InitialArenaPages
	if arenaPages==0{
		regionPages:=(g.MetadataBytes-g.ReservedOffset)/format.PageSize
		bitmapPages:=(regionPages+uint64(format.PageSize)*8-1)/(uint64(format.PageSize)*8)
		overhead:=uint64(2)+bitmapPages+lifecyclePages
		if regionPages<=overhead+16{return errors.New("BMPOOL03 Reserved region is too small for an initial Volume Arena")}
		arenaPages=(regionPages-overhead)*3/4
	}
	descriptor:=format.DynamicVolumeDescriptor{VolumeID:id,AG:0,Generation:1,VolumeSequence:1,
		MappingRootOffset:g.MappingOffset,BitmapSequence:1,OwnerEpoch:owner.Epoch(),ArenaPages:arenaPages,
		RootPage:^uint64(0),LifecyclePages:lifecyclePages}
	locker:=newSanlock(c)
	resource:=fmt.Sprintf("blockmesh:pool:%x:ag:%03d:wal",record.PoolUUID,0)
	committer,err:=wal.NewDynamicSharedCommitter(metadata,sb,0,locker,resource);if err!=nil{return err}
	if err=wal.GuardDynamicSharedCommitter(committer,owner,id,nil);err!=nil{return err}
	if _,err=volumeAllocator.AllocateVolume(0,descriptor,committer);err!=nil{return fmt.Errorf("allocate initial BMPOOL03 Volume: %w",err)}
	epochReader,err:=metastore.NewDynamicOwnerEpochReader(metadata,sb,0,id);if err!=nil{return err}
	if err=wal.GuardDynamicSharedCommitter(committer,owner,id,epochReader.ReadOwnerEpoch);err!=nil{return err}
	volume,err:=metastore.OpenDynamicPoolVolume(metadata,sb,0,id);if err!=nil{return err}
	if err=metastore.FormatLifecycle(volume);err!=nil{return fmt.Errorf("initialize initial Volume lifecycle: %w",err)}
	out:=dynamicPoolStatus{PoolID:id,PoolUUID:fmt.Sprintf("%x",record.PoolUUID),ViewGeneration:viewGeneration,
		SuperblockGeneration:sb.Generation,MetadataBytes:g.MetadataBytes,DataBytes:g.DataBytes,
		AGCount:g.AGCount,AGCapacity:g.AGCapacity,WALSegmentBytes:g.WALSegmentBytes,ReservedAllocator:true}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func dynamicStatusPool(id string,c poolConfig)error{
	dev,bootstrap,_,err:=openStore(c,id,false);if err!=nil{return err};defer dev.Close()
	metadata,data,generation,err:=bootstrap.OpenViews();if err!=nil{return err}
	sb,err:=allocator.OpenDynamicSuperblockPair(metadata);if err!=nil{return err}
	reserved:=metastore.DynamicVolumeAllocatorPresent(metadata,sb)
	out:=dynamicPoolStatus{PoolID:id,PoolUUID:fmt.Sprintf("%x",sb.PoolUUID),ViewGeneration:generation,
		SuperblockGeneration:sb.Generation,MetadataBytes:sb.Geometry.MetadataBytes,DataBytes:sb.Geometry.DataBytes,
		AGCount:sb.Geometry.AGCount,AGCapacity:sb.Geometry.AGCapacity,WALSegmentBytes:sb.Geometry.WALSegmentBytes,
		ReservedAllocator:reserved}
	if data==nil||uint64(data.Size())<sb.Geometry.DataBytes{return errors.New("DataView is smaller than persisted BMPOOL03 geometry")}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func dynamicRecoverPool(id string,c poolConfig)error{
	dev,bootstrap,owner,err:=openStore(c,id,true);if err!=nil{return err}
	defer dev.Close();defer owner.Close()
	metadata,data,viewGeneration,err:=bootstrap.OpenViews();if err!=nil{return err}
	sb,err:=allocator.OpenDynamicSuperblockPair(metadata)
	if errors.Is(err,allocator.ErrDynamicFormatIncomplete){
		if data==nil{return errors.New("Pool has no DataView")}
		cfg,e:=dynamicGeometryConfig(c,uint64(metadata.Size()),uint64(data.Size()));if e!=nil{return e}
		g,e:=format.PlanDynamicGeometry(cfg);if e!=nil{return e}
		sb,e=format.NewDynamicSuperblock(g,bootstrap.Record().PoolUUID,1,viewGeneration);if e!=nil{return e}
		if e=allocator.RecoverDynamicFormat(metadata,sb);e!=nil{return e}
		sb,err=allocator.OpenDynamicSuperblockPair(metadata)
	}
	if err!=nil{return err}
	locker:=newSanlock(c)
	for ag:=uint32(0);ag<sb.Geometry.AGCount;ag++{
		resource:=fmt.Sprintf("blockmesh:pool:%x:ag:%03d:wal",sb.PoolUUID,ag)
		committer,e:=wal.NewDynamicSharedCommitter(metadata,sb,ag,locker,resource);if e!=nil{return e}
		if e=wal.GuardDynamicSharedCommitter(committer,owner,id,nil);e!=nil{return e}
		if e=committer.Recover();e!=nil{return fmt.Errorf("recover AG %d WAL: %w",ag,e)}
	}
	if metastore.DynamicVolumeAllocatorPresent(metadata,sb){
		volumeAllocator,e:=metastore.OpenDynamicVolumeAllocator(metadata,sb)
		if errors.Is(e,metastore.ErrDynamicVolumeAllocatorUnformatted){
			if e=metastore.RecoverDynamicVolumeAllocatorFormat(metadata,sb);e!=nil{return e}
			volumeAllocator,e=metastore.OpenDynamicVolumeAllocator(metadata,sb)
		}
		if e!=nil{return e}
		resource:=fmt.Sprintf("blockmesh:pool:%x:ag:%03d:wal",sb.PoolUUID,0)
		committer,e:=wal.NewDynamicSharedCommitter(metadata,sb,0,locker,resource);if e!=nil{return e}
		volume,e:=metastore.OpenDynamicPoolVolume(metadata,sb,0,id);if e!=nil{return fmt.Errorf("open Volume after WAL recovery: %w",e)}
		epochReader,e:=metastore.NewDynamicOwnerEpochReader(metadata,sb,0,id);if e!=nil{return e}
		if e=wal.GuardDynamicSharedCommitter(committer,owner,id,epochReader.ReadOwnerEpoch);e!=nil{return e}
		if e=volumeAllocator.ReconcileRegion(committer);e!=nil{return fmt.Errorf("reconcile Reserved allocator geometry: %w",e)}
	}
	if !owner.Valid(){return errors.New("Owner lease lost during Pool recovery")}
	out:=dynamicPoolStatus{PoolID:id,PoolUUID:fmt.Sprintf("%x",sb.PoolUUID),ViewGeneration:viewGeneration,
		SuperblockGeneration:sb.Generation,MetadataBytes:sb.Geometry.MetadataBytes,DataBytes:sb.Geometry.DataBytes,
		AGCount:sb.Geometry.AGCount,AGCapacity:sb.Geometry.AGCapacity,WALSegmentBytes:sb.Geometry.WALSegmentBytes,
		ReservedAllocator:metastore.DynamicVolumeAllocatorPresent(metadata,sb)}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func rescanPool(id string,c poolConfig)error{
	dev,store,owner,err:=openStore(c,id,true);if err!=nil{return err};defer dev.Close();defer owner.Close()
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
	dev,bootstrap,owner,err:=openStore(c,id,true);if err!=nil{return err};defer dev.Close();defer owner.Close()
	metadataOld,dataOld,oldViewGeneration,err:=bootstrap.OpenViews();if err!=nil{return err}
	if dataOld==nil{return errors.New("Pool has no DataView")}
	currentSB,err:=allocator.OpenDynamicSuperblockPair(metadataOld);if err!=nil{return fmt.Errorf("open BMPOOL03 before expansion: %w",err)}
	record:=bootstrap.Record()
	if currentSB.ViewGeneration!=oldViewGeneration{return errors.New("BMPOOL03 and Bootstrap view generations are inconsistent before expansion")}
	var currentMetadataTotal uint64
	for _,extent:=range bootstrap.Extents(){if extent.Kind==poolspace.Metadata{currentMetadataTotal+=extent.Length}}
	if currentMetadataTotal<record.BootstrapBytes{return poolspace.ErrInvalidGeometry}
	currentMetadata:=currentMetadataTotal-record.BootstrapBytes
	targetMetadata,err:=poolspace.MetadataExpansionTarget(currentMetadata,usage.UsedBytes,usage.GrowthBytes,poolspace.ExtentSize);if err!=nil{return err}
	targetTotal:=targetMetadata+record.BootstrapBytes
	if record.Generation==^uint64(0){return errors.New("Bootstrap generation exhausted")}
	nextViewGeneration:=record.Generation+1
	nextSB,err:=allocator.BuildDynamicGeometryUpdate(currentSB,targetTotal,uint64(dataOld.Size()),nextViewGeneration)
	if err!=nil{return fmt.Errorf("preflight BMPOOL03 geometry expansion: %w",err)}
	var volumeAllocator *metastore.DynamicVolumeAllocator
	reservedAllocator:=metastore.DynamicVolumeAllocatorPresent(metadataOld,currentSB)
	if reservedAllocator{
		volumeAllocator,err=metastore.OpenDynamicVolumeAllocator(metadataOld,currentSB);if err!=nil{return err}
		if err=volumeAllocator.CanExpandRegion(nextSB);err!=nil{return fmt.Errorf("preflight Reserved allocator expansion: %w",err)}
	}
	target,err:=bootstrap.ExpandMetadata(usage.UsedBytes,usage.GrowthBytes);if err!=nil{return err}
	if target!=targetTotal{return fmt.Errorf("metadata expansion plan changed under execution: planned=%d actual=%d",targetTotal,target)}
	metadata,data,viewGeneration,err:=bootstrap.OpenViews();if err!=nil{return err}
	if data==nil||viewGeneration!=nextViewGeneration||uint64(metadata.Size())!=targetTotal{return errors.New("Bootstrap did not publish the planned MetadataView generation/capacity")}
	locker:=newSanlock(c)
	ctx,cancel:=context.WithTimeout(context.Background(),2*time.Minute);defer cancel()
	if !owner.Valid(){return errors.New("Owner lease lost before BMPOOL03 geometry publication")}
	if err=allocator.PublishDynamicGeometryUnderLease(ctx,metadata,currentSB,nextSB,owner,locker,nil);err!=nil{return fmt.Errorf("publish expanded BMPOOL03 geometry: %w",err)}
	walResource:=fmt.Sprintf("blockmesh:pool:%x:ag:%03d:wal",record.PoolUUID,0)
	globalCommitter,err:=wal.NewDynamicSharedCommitter(metadata,nextSB,0,locker,walResource);if err!=nil{return err}
	volume,err:=metastore.OpenDynamicPoolVolume(metadata,nextSB,0,id);if err!=nil{return err}
	epochReader,err:=metastore.NewDynamicOwnerEpochReader(metadata,nextSB,0,id);if err!=nil{return err}
	if err=wal.GuardDynamicSharedCommitter(globalCommitter,owner,id,epochReader.ReadOwnerEpoch);err!=nil{return err}
	if reservedAllocator{
		if !owner.Valid(){return errors.New("Owner lease lost before Reserved allocator expansion")}
		if err=volumeAllocator.ExpandRegion(metadata,nextSB,globalCommitter);err!=nil{return fmt.Errorf("expand Reserved allocator: %w",err)}
		if !owner.Valid(){return errors.New("Owner lease lost during Reserved allocator expansion")}
	}
	descriptor,err:=volume.DynamicPoolDescriptor();if err!=nil{return err}
	volumeCommitter:=globalCommitter
	if descriptor.AG!=0{
		resource:=fmt.Sprintf("blockmesh:pool:%x:ag:%03d:wal",record.PoolUUID,descriptor.AG)
		volumeCommitter,err=wal.NewDynamicSharedCommitter(metadata,nextSB,descriptor.AG,locker,resource);if err!=nil{return err}
		if err=wal.GuardDynamicSharedCommitter(volumeCommitter,owner,id,epochReader.ReadOwnerEpoch);err!=nil{return err}
	}
	if reservedAllocator{
		pages:=c.ArenaGrowthPages;if pages==0{pages=256}
		unlock,e:=locker.Lock(ctx,fmt.Sprintf("ag:%03d",descriptor.AG));if e!=nil{return e}
		if !owner.Valid(){_ = unlock();return errors.New("Owner lease lost before Volume Arena growth")}
		e=volume.ExpandDynamicArenaBy(pages,volumeCommitter)
		unlockErr:=unlock()
		if e!=nil{return e};if unlockErr!=nil{return unlockErr}
		if !owner.Valid(){return errors.New("Owner lease lost during Volume Arena growth")}
	}else{
		if err=volume.ExpandDynamicArenaUnderLease(ctx,owner,locker,volumeCommitter);err!=nil{return err}
	}
	fmt.Printf("{\"pool_id\":%q,\"metadata_bytes\":%d,\"view_generation\":%d,\"superblock_generation\":%d,\"arena_pages_added\":%d}\n",
		id,target,viewGeneration,nextSB.Generation,func()uint64{if reservedAllocator{if c.ArenaGrowthPages>0{return c.ArenaGrowthPages};return 256};return 0}())
	return nil
}
func statusPool(id string,c poolConfig)error{
	dev,store,_,err:=openStore(c,id,false);if err!=nil{return err};defer dev.Close()
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
