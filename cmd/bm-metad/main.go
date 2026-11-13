//go:build linux

// bm-metad is the production BMPOOL03 Metadata Service daemon. It deliberately
// requires explicit SAN, sanlock and old-owner drain configuration; it never
// falls back to local locks or no-op fencing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"blockmesh/internal/allocator"
	"blockmesh/internal/control"
	"blockmesh/internal/device"
	"blockmesh/internal/dmowner"
	"blockmesh/internal/dynamicruntime"
	"blockmesh/internal/fence"
	"blockmesh/internal/lock"
	"blockmesh/internal/metaservice"
	"blockmesh/internal/metastore"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/volume"
)

type staticPRKeyResolver map[string][]uint64

func (r staticPRKeyResolver) KeysForNode(_ context.Context,nodeID,bootID string)([]uint64,error) {
	keys:=r[nodeID+"/"+bootID]
	if len(keys)==0{return nil,fmt.Errorf("no configured SCSI PR registration keys for previous Owner %s/%s",nodeID,bootID)}
	return append([]uint64(nil),keys...),nil
}

type ownerRecordRepository struct { volumes map[string]volumeConfig; resolve func(string)(volumeConfig,error) }

type ownerRecordHandle struct {
	parent *device.BlockDevice
	metadata interface{ Close() error }
	data interface{ Close() error }
	records volume.OwnerRecordStore
	epoch *metastore.DynamicOwnerEpochReader
}

func (r *ownerRecordRepository) open(volumeID string)(*ownerRecordHandle,error) {
	v,ok:=r.volumes[volumeID]
	if r.resolve!=nil { var resolveErr error;v,resolveErr=r.resolve(volumeID);if resolveErr!=nil{return nil,resolveErr};ok=true }
	if !ok{return nil,fmt.Errorf("Volume %s is not configured",volumeID)}
	parent,err:=device.OpenBlockDevice(v.DevicePath,true,false)
	if err!=nil{return nil,fmt.Errorf("open shared Pool LUN for OwnerRecord: %w",err)}
	fail:=func(cause error)(*ownerRecordHandle,error){_=parent.Close();return nil,cause}
	bootstrap,err:=poolspace.OpenBootstrap(parent)
	if err!=nil{return fail(fmt.Errorf("open BMPOOL03 Bootstrap for OwnerRecord: %w",err))}
	metadata,data,generation,err:=bootstrap.OpenViews()
	if err!=nil{return fail(err)}
	if metadata==nil || data==nil || generation==0 || metadata.Generation()!=generation || data.Generation()!=generation {
		if metadata!=nil{_=metadata.Close()};if data!=nil{_=data.Close()}
		return fail(errors.New("OwnerRecord access requires initialized, generation-matched BMPOOL03 MetadataView and DataView"))
	}
	sb,err:=allocator.OpenDynamicSuperblockPair(metadata)
	if err!=nil{_=metadata.Close();_=data.Close();return fail(fmt.Errorf("open BMPOOL03 Superblock for OwnerRecord: %w",err))}
	if sb.ViewGeneration!=generation {
		_=metadata.Close();_=data.Close()
		return fail(fmt.Errorf("BMPOOL03 Superblock generation %d differs from Bootstrap views %d",sb.ViewGeneration,generation))
	}
	records,epoch,err:=metastore.NewDynamicOwnerStores(metadata,sb,v.VolumeIndex,volumeID)
	if err!=nil{_=metadata.Close();_=data.Close();return fail(err)}
	return &ownerRecordHandle{parent:parent,metadata:metadata,data:data,records:records,epoch:epoch},nil
}

func (r *ownerRecordRepository) with(volumeID string,fn func(*ownerRecordHandle)error)error {
	h,err:=r.open(volumeID);if err!=nil{return err}
	callErr:=fn(h)
	var closeErrs []error
	if h.metadata!=nil { if e:=h.metadata.Close();e!=nil{closeErrs=append(closeErrs,e)} }
	if h.data!=nil { if e:=h.data.Close();e!=nil{closeErrs=append(closeErrs,e)} }
	if h.parent!=nil { if e:=h.parent.Close();e!=nil{closeErrs=append(closeErrs,e)} }
	return errors.Join(callErr,errors.Join(closeErrs...))
}
func (r *ownerRecordRepository) Load(ctx context.Context,volumeID string)(record volume.OwnerRecord,exists bool,retErr error) {
	if err:=ctx.Err();err!=nil{return record,false,err}
	retErr=r.with(volumeID,func(h *ownerRecordHandle)error{var e error;record,exists,e=h.records.Load(ctx,volumeID);return e})
	return
}
func (r *ownerRecordRepository) CompareAndSwap(ctx context.Context,volumeID string,expected *volume.OwnerRecord,next volume.OwnerRecord)error {
	if err:=ctx.Err();err!=nil{return err}
	return r.with(volumeID,func(h *ownerRecordHandle)error{return h.records.CompareAndSwap(ctx,volumeID,expected,next)})
}
func (r *ownerRecordRepository) ReadEpoch(ctx context.Context,volumeID string)(epoch uint64,retErr error) {
	if err:=ctx.Err();err!=nil{return 0,err}
	retErr=r.with(volumeID,func(h *ownerRecordHandle)error{var e error;epoch,e=h.epoch.ReadOwnerEpoch();return e})
	return
}

func main() {
	configPath:=flag.String("config","/etc/blockmesh/metad.json","daemon JSON configuration")
	flag.Parse()
	cfg,err:=loadConfig(*configPath)
	if err!=nil{log.Fatalf("load configuration: %v",err)}
	if err=run(cfg);err!=nil{log.Fatalf("bm-metad: %v",err)}
}

func run(cfg daemonConfig) error {
	offsets:=cfg.Sanlock.ResourceOffsets
	var resourceIndex *lock.SanlockRIndexResolver
if cfg.Sanlock.ResourceIndexOffset!=0 {
 resourceIndex=&lock.SanlockRIndexResolver{
  Binary:cfg.Sanlock.Binary,Lockspace:cfg.Sanlock.Lockspace,
  Device:cfg.Sanlock.LockDevice,Offset:cfg.Sanlock.ResourceIndexOffset,
 }
}
	locker:=&lock.SanlockLocker{
		Binary:cfg.Sanlock.Binary,HelperBinary:cfg.Sanlock.HelperBinary,ReadyDir:cfg.Sanlock.ReadyDir,
		ResourceFor:func(name string)(string,error){
			off,ok:=offsets[name];if !ok&&resourceIndex!=nil { var e error;off,e=resourceIndex.Resolve(name);if e!=nil{return "",e};ok=true };if !ok{return "",fmt.Errorf("sanlock resource %q has no configured offset (resource_index_offset is not configured)",name)}
			return lock.SanlockResource(cfg.Sanlock.Lockspace,name,cfg.Sanlock.LockDevice,off)
		},
	}
	lifecycleBackend:=newCatalogLifecycleBackend(cfg.DiscoveryDevices,locker)
	staticVolumes:=make(map[string]volumeConfig,len(cfg.Volumes))
	for _,v:=range cfg.Volumes{staticVolumes[v.VolumeID]=v}
	resolveVolume:=func(id string)(volumeConfig,error){
		item,err:=lifecycleBackend.findVolume(context.Background(),id)
		if err==nil { dmName,e:=dmowner.BlockMeshDMName(id);if e!=nil{return volumeConfig{},e};return volumeConfig{VolumeID:id,DevicePath:item.Device,DMDevice:dmName,VolumeIndex:item.Index},nil }
		if v,ok:=staticVolumes[id];ok{return v,nil};return volumeConfig{},err
	}
	paths:=make([]fence.PRPath,0,len(cfg.SAN.Paths))
	for _,p:=range cfg.SAN.Paths{paths=append(paths,fence.PRPath{Device:p.Device,LocalKey:p.LocalKey})}
	lifecycleBackend.attachPoolBootstrap(cfg.NodeID,cfg.BootID,cfg.SAN.SGPersistBinary,cfg.SAN.ReservationType,paths)
	keyResolver:=staticPRKeyResolver(cfg.SAN.OwnerKeys)
	fencer:=&fence.SCSIPRFencer{Binary:cfg.SAN.SGPersistBinary,Paths:paths,Keys:keyResolver,ReservationType:cfg.SAN.ReservationType}
	reservation:=&fence.SCSIPROwnerReservation{Binary:cfg.SAN.SGPersistBinary,Paths:paths,ReservationType:cfg.SAN.ReservationType}
	registrar:=&fence.SCSIPRRegistrar{Binary:cfg.SAN.SGPersistBinary,Paths:paths}
	records:=&ownerRecordRepository{volumes:staticVolumes,resolve:resolveVolume}

	ownerCoordinator,err:=volume.NewOwnerCoordinator(volume.OwnerCoordinatorConfig{
		Locker:locker,ResourceFor:volume.OwnerResourceName,
		Records:records,Fencer:fencer,Reservation:reservation,
		ReadEpoch:records.ReadEpoch,
		DrainOldOwner:func(ctx context.Context,id string,old volume.OwnerRecord)error{
			cmd:=exec.CommandContext(ctx,cfg.SAN.OwnerDrainHelper,
				"--volume-id",id,"--node-id",old.NodeID,"--boot-id",old.BootID,"--owner-epoch",fmt.Sprint(old.Epoch))
			out,e:=cmd.CombinedOutput()
			if e!=nil{return fmt.Errorf("old-owner I/O drain helper: %w: %s",e,strings.TrimSpace(string(out)))}
			return nil
		},
	})
	if err!=nil{return fmt.Errorf("construct P3 OwnerCoordinator: %w",err)}

	readerCoordinator:=&metastore.DistributedReaderCoordinator{
		Locker:locker,ResourceFor:func(id string)string{return "reader-set/"+id},
	}
	readerLease:=volume.ReaderLeaseConfig{
		Locker:locker,ResourceFor:func(id string,slot uint32)string{return fmt.Sprintf("reader/%s/%d",id,slot)},
	}
	backend,err:=dynamicruntime.New(dynamicruntime.Config{
		OpenDevice:func(ctx context.Context,spec metastore.VolumeRuntimeSpec)(device.Device,error){
			if err:=ctx.Err();err!=nil{return nil,err}
			v,e:=resolveVolume(spec.VolumeID);if e!=nil{return nil,fmt.Errorf("resolve Volume %s device: %w",spec.VolumeID,e)}
			return device.OpenBlockDevice(v.DevicePath,true,false)
		},
		Locker:locker,
		WALResourceForAG:func(poolUUID [16]byte,ag uint32)string{return fmt.Sprintf("wal/%x/%d",poolUUID,ag)},
		ReaderLease:readerLease,ReaderRegistrar:registrar,ReaderCoordinator:readerCoordinator,
	})
	if err!=nil{return fmt.Errorf("construct BMPOOL03 Backend: %w",err)}
	dmSetup:=dmowner.DMSetupGate{}
	tables:=dmowner.TableManager{}
	dataViews:=dmowner.LinearViewManager{}
	gate,err:=dynamicruntime.NewDMRequestGateWithRemoval(dmSetup,dmSetup,func(id string)string{
		v,e:=resolveVolume(id);if e!=nil{return ""};return v.DMDevice
	},func(ctx context.Context,id string)error{return tables.Remove(ctx,id)})
	if err!=nil{return err}
	heartbeat,err:=parseDuration("runtime.reader_heartbeat_interval",cfg.Runtime.ReaderHeartbeatInterval,250*time.Millisecond)
	if err!=nil{return err}
	manager,err:=dynamicruntime.NewIntegratedDynamicVolumeRuntimeManager(backend,ownerCoordinator,gate,heartbeat)
	if err!=nil{return fmt.Errorf("construct integrated Volume Runtime manager: %w",err)}
	lifecycleBackend.attachRuntime(manager,tables,dmSetup,dataViews,cfg.NodeID,cfg.BootID)
	processor,err:=metaservice.NewDynamicPoolRuntimeProcessor(manager)
	if err!=nil{return err}
	requestTimeout,err:=parseDuration("service.request_timeout",cfg.Service.RequestTimeout,metaservice.DefaultRequestTimeout)
	if err!=nil{return err}
	shutdownTimeout,err:=parseDuration("service.shutdown_timeout",cfg.Service.ShutdownTimeout,30*time.Second)
	if err!=nil{return err}

	service,err:=metaservice.NewService(metaservice.ServiceConfig{
		DevicePath:cfg.Service.DevicePath,LockPath:cfg.Service.LockPath,Processor:processor,RequestTimeout:requestTimeout,
		Recover:func(ctx context.Context)error{
			// Runtime ownership is not persisted as a local desired-state list.
			// On daemon restart, fence and remove every stale local BlockMesh DM
			// table before accepting lifecycle requests; Volumes remain inactive.
			names,e:=tables.List(ctx);if e!=nil{return e}
			for _,name:=range names{
				if e=dmSetup.FenceCurrent(ctx,name);e!=nil{return fmt.Errorf("fence stale local DM target %s: %w",name,e)}
				if e=tables.RemoveName(ctx,name);e!=nil{return fmt.Errorf("remove stale local DM target %s: %w",name,e)}
			}
			return nil
		},
		StopRuntime:manager.Shutdown,ShutdownTimeout:shutdownTimeout,
	})
	if err!=nil{return err}
	socketPath:=cfg.Control.SocketPath
	if strings.TrimSpace(socketPath)=="" { socketPath="/run/blockmesh/control.sock" }
	lifecycleServer,err:=control.New(control.Config{Path:socketPath,Handler:control.LifecycleHandler{Backend:lifecycleBackend}})
	if err!=nil{return fmt.Errorf("construct lifecycle control server: %w",err)}
	defer lifecycleServer.Close()

	ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM)
	defer stop()
	defer func() {
		if shutdownErr := service.Shutdown(); shutdownErr != nil {
			log.Printf("bm-metad final shutdown retry failed: %v", shutdownErr)
		}
	}()
	serveCtx,cancelServe:=context.WithCancel(ctx)
	defer cancelServe()
	controlErr:=make(chan error,1)
	go func(){if serveErr:=lifecycleServer.Serve(serveCtx);serveErr!=nil{controlErr<-serveErr;stop()}}()
	log.Printf("bm-metad starting: node=%s boot=%s configured_volumes=%d discovery_devices=%d control_socket=%s device=%s",cfg.NodeID,cfg.BootID,len(cfg.Volumes),len(cfg.DiscoveryDevices),socketPath,cfg.Service.DevicePath)
	serviceErr:=service.Run(serveCtx)
	cancelServe()
	select {
	case serveErr:=<-controlErr:
		if serveErr!=nil{return errors.Join(serviceErr,fmt.Errorf("lifecycle control server: %w",serveErr))}
	default:
	}
	if serviceErr!=nil{return serviceErr}
	log.Printf("bm-metad stopped cleanly")
	return nil
}
