//go:build linux

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"blockmesh/internal/allocator"
	"blockmesh/internal/control"
	"blockmesh/internal/dmowner"
	"blockmesh/internal/dynamicruntime"
	"blockmesh/internal/fence"
	"blockmesh/internal/metastore"
	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/poolspace"
	"blockmesh/internal/poolinit"
	"blockmesh/internal/wal"
)

const (
	initialVolumeArenaPages = 64
	initialVolumeLifecyclePages = 8
)

type catalogLifecycleBackend struct {
	devices []string
	locker lock.DistributedLocker
	runtime *dynamicruntime.DynamicVolumeRuntimeManager
	tables dmowner.TableManager
	dmSetup dmowner.DMSetupGate
	dataViews dmowner.LinearViewManager
	nodeID string
	bootID string
	prPaths []fence.PRPath
	prBinary string
	prType uint8
}

func newCatalogLifecycleBackend(paths []string, locker lock.DistributedLocker) *catalogLifecycleBackend {
	return &catalogLifecycleBackend{devices: append([]string(nil), paths...), locker: locker}
}

type discoveredPool struct {
	poolspace.PoolCatalogEntry
	Device string `json:"device"`
}

func (b *catalogLifecycleBackend) scan(ctx context.Context) ([]discoveredPool, error) {
	if b == nil { return nil, errors.New("Pool catalog backend is unavailable") }
	out := make([]discoveredPool, 0, len(b.devices))
	for _, path := range b.devices {
		if err := ctx.Err(); err != nil { return nil, err }
		dev, err := device.OpenBlockDevice(path, false, false)
		if err != nil { return nil, fmt.Errorf("open Pool discovery device %s: %w", path, err) }
		entry, readErr := poolspace.ReadPoolCatalog(dev)
		closeErr := dev.Close()
		if readErr != nil || closeErr != nil { return nil, errors.Join(fmt.Errorf("read Pool catalog on %s: %w", path, readErr), closeErr) }
		out = append(out, discoveredPool{PoolCatalogEntry: entry, Device: path})
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].Name, out[j].Name) < 0 })
	for i := 1; i < len(out); i++ {
		if out[i-1].Name == out[i].Name { return nil, fmt.Errorf("duplicate Pool name %q discovered on %s and %s", out[i].Name, out[i-1].Device, out[i].Device) }
	}
	return out, nil
}

func (b *catalogLifecycleBackend) PoolList(ctx context.Context) (any, error) { return b.scan(ctx) }
func (b *catalogLifecycleBackend) PoolInfo(ctx context.Context, name string) (any, error) {
	pools, err := b.scan(ctx); if err != nil { return nil, err }
	for _, p := range pools { if p.Name == name { return p, nil } }
	return nil, fmt.Errorf("Pool %q was not found in configured discovery devices", name)
}

type discoveredVolume struct {
	Pool string `json:"pool"`
	PoolUUID string `json:"pool_uuid"`
	Device string `json:"device"`
	Index uint32 `json:"index"`
	Descriptor format.DynamicVolumeDescriptor `json:"descriptor"`
}

func (b *catalogLifecycleBackend) volumesForPool(ctx context.Context, pool discoveredPool) ([]discoveredVolume, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	parent, err := device.OpenBlockDevice(pool.Device, false, false)
	if err != nil { return nil, fmt.Errorf("open Pool LUN %s: %w", pool.Device, err) }
	defer parent.Close()
	bootstrap, err := poolspace.OpenBootstrap(parent)
	if err != nil { return nil, fmt.Errorf("open Pool %s Bootstrap: %w", pool.Name, err) }
	record := bootstrap.Record()
	if fmt.Sprintf("%x", record.PoolUUID) != pool.UUID || record.PoolName != pool.Name { return nil, fmt.Errorf("Pool catalog identity changed while discovering Volumes on %s", pool.Device) }
	metadata, data, generation, err := bootstrap.OpenViews()
	if err != nil { return nil, fmt.Errorf("open Pool %s views: %w", pool.Name, err) }
	defer metadata.Close()
	if data != nil { defer data.Close() }
	if generation == 0 { return nil, errors.New("Pool MetadataView has zero generation") }
	sb, err := allocator.OpenDynamicSuperblockPair(metadata)
	if err != nil { return nil, fmt.Errorf("open Pool %s superblock: %w", pool.Name, err) }
	if sb.PoolUUID != record.PoolUUID || sb.ViewGeneration != generation { return nil, fmt.Errorf("Pool %s superblock identity or MetadataView generation mismatch", pool.Name) }
	catalog, err := metastore.OpenDynamicVolumeAllocatorReadOnly(metadata, sb)
	if err != nil { return nil, fmt.Errorf("open Pool %s Volume catalog: %w", pool.Name, err) }
	entries, err := catalog.ListVolumes()
	if err != nil { return nil, fmt.Errorf("list Pool %s Volume descriptors: %w", pool.Name, err) }
	out := make([]discoveredVolume, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil { return nil, err }
		out = append(out, discoveredVolume{Pool: pool.Name, PoolUUID: pool.UUID, Device: pool.Device, Index: entry.Index, Descriptor: entry.Descriptor})
	}
	return out, nil
}

func (b *catalogLifecycleBackend) scanVolumes(ctx context.Context) ([]discoveredVolume, error) {
	pools, err := b.scan(ctx); if err != nil { return nil, err }
	out := make([]discoveredVolume, 0); ids := make(map[string]string)
	for _, pool := range pools {
		items, e := b.volumesForPool(ctx, pool); if e != nil { return nil, e }
		for _, item := range items {
			if previous, ok := ids[item.Descriptor.VolumeID]; ok { return nil, fmt.Errorf("duplicate Volume UUID %s found on %s and %s", item.Descriptor.VolumeID, previous, item.Device) }
			ids[item.Descriptor.VolumeID] = item.Device; out = append(out, item)
		}
	}
	return out, nil
}
func (b *catalogLifecycleBackend) VolumeList(ctx context.Context) (any, error) {
	items, err := b.scanVolumes(ctx); if err != nil { return nil, err }
	sort.Slice(items, func(i, j int) bool { if items[i].Pool != items[j].Pool { return items[i].Pool < items[j].Pool }; return items[i].Index < items[j].Index })
	return items, nil
}
func (b *catalogLifecycleBackend) findVolume(ctx context.Context,id string) (discoveredVolume,error) {
	items,err:=b.scanVolumes(ctx);if err!=nil{return discoveredVolume{},err}
	for _,item:=range items { if item.Descriptor.VolumeID==id { return item,nil } }
	return discoveredVolume{},fmt.Errorf("Volume %q was not found in configured Pool devices",id)
}
func (b *catalogLifecycleBackend) VolumeInfo(ctx context.Context,id string) (any,error) { return b.findVolume(ctx,id) }


func (b *catalogLifecycleBackend) attachPoolBootstrap(nodeID, bootID, binary string, reservationType uint8, paths []fence.PRPath) {
	b.nodeID, b.bootID, b.prBinary, b.prType = nodeID, bootID, binary, reservationType
	b.prPaths = append([]fence.PRPath(nil), paths...)
}

type poolBootstrapOwnerLease struct {
	lease lock.Lease
	reservation *fence.SCSIPROwnerReservation
	nodeID, bootID string
	epoch uint64
	closeErr error
	closed bool
	mu sync.Mutex
	lastVerify time.Time
	verified bool
}

func (l *poolBootstrapOwnerLease) Epoch() uint64 { if l == nil { return 0 }; return l.epoch }
func (l *poolBootstrapOwnerLease) Valid() bool {
	if l == nil || l.lease == nil || l.reservation == nil || !l.lease.Valid() { return false }
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed { return false }
	// Bootstrap format writes many metadata pages. Verify the PR reservation
	// periodically rather than spawning sg_persist for every 4KiB WriteAt;
	// the SCSI target itself still enforces the reservation on each I/O.
	if l.verified && time.Since(l.lastVerify) < time.Second { return l.lease.Valid() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := l.reservation.VerifyOwnerReservation(ctx, l.nodeID, l.bootID)
	l.lastVerify = time.Now()
	l.verified = err == nil
	return l.verified && l.lease.Valid()
}
func (l *poolBootstrapOwnerLease) Close() error {
	if l == nil { return nil }
	l.mu.Lock()
	if l.closed { err := l.closeErr; l.mu.Unlock(); return err }
	l.closed = true
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var prErr error
	if l.reservation != nil { prErr = l.reservation.ReleaseOwnerReservation(ctx, l.nodeID, l.bootID) }
	var lockErr error
	if l.lease != nil { lockErr = l.lease.Close() }
	l.mu.Lock()
	l.closeErr = errors.Join(prErr, lockErr)
	err := l.closeErr
	l.mu.Unlock()
	return err
}

func poolReady(dev device.Device, name string) (poolspace.PoolCatalogEntry, bool, error) {
	version, err := poolspace.DetectFormat(dev)
	if errors.Is(err, poolspace.ErrUnknownPoolFormat) { return poolspace.PoolCatalogEntry{}, false, nil }
	if err != nil { return poolspace.PoolCatalogEntry{}, false, err }
	if version == poolspace.FormatBMPOOL02 { return poolspace.PoolCatalogEntry{}, false, poolspace.ErrLegacyPoolFormat }
	bootstrap, err := poolspace.OpenBootstrap(dev)
	if errors.Is(err, poolspace.ErrBootstrapFormatIncomplete) { return poolspace.PoolCatalogEntry{}, false, nil }
	if err != nil { return poolspace.PoolCatalogEntry{}, false, err }
	record := bootstrap.Record()
	if record.PoolName != name { return poolspace.PoolCatalogEntry{}, false, fmt.Errorf("device already belongs to Pool %q, not %q", record.PoolName, name) }
	metadata, data, generation, err := bootstrap.OpenViews()
	if err != nil { return poolspace.PoolCatalogEntry{}, false, err }
	defer metadata.Close()
	if data != nil { defer data.Close() }
	sb, err := allocator.OpenDynamicSuperblockPair(metadata)
	if errors.Is(err, allocator.ErrDynamicFormatIncomplete) || errors.Is(err, allocator.ErrDynamicSuperblockUnformatted) {
		return poolspace.PoolCatalogEntry{}, false, nil
	}
	if err != nil { return poolspace.PoolCatalogEntry{}, false, err }
	if sb.PoolUUID != record.PoolUUID || sb.ViewGeneration != generation { return poolspace.PoolCatalogEntry{}, false, errors.New("Pool Superblock identity or MetadataView generation mismatch") }
	if _, err = metastore.OpenDynamicVolumeAllocatorReadOnly(metadata, sb); errors.Is(err, metastore.ErrDynamicVolumeAllocatorUnformatted) {
		return poolspace.PoolCatalogEntry{}, false, nil
	} else if err != nil { return poolspace.PoolCatalogEntry{}, false, err }
	entry, err := poolspace.ReadPoolCatalog(dev)
	return entry, err == nil, err
}

func (b *catalogLifecycleBackend) attachRuntime(manager *dynamicruntime.DynamicVolumeRuntimeManager,tables dmowner.TableManager,setup dmowner.DMSetupGate,dataViews dmowner.LinearViewManager,nodeID,bootID string) {
	b.runtime=manager;b.tables=tables;b.dmSetup=setup;b.dataViews=dataViews;b.nodeID=nodeID;b.bootID=bootID
}

func newVolumeUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil { return "", fmt.Errorf("generate Volume UUID: %w", err) }
	s := hex.EncodeToString(raw[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], nil
}

// VolumeCreate is metadata-only. A distributed Pool catalog lock serializes
// descriptor-slot and Reserved-bitmap selection across nodes. The transaction
// itself is committed through the selected AG's shared WAL resource; a retry
// with the CLI's stable CreateToken resolves an uncertain commit.
func (b *catalogLifecycleBackend) VolumeCreate(ctx context.Context, args control.VolumeCreateArgs) (result any, retErr error) {
	if b == nil || b.locker == nil || !b.locker.Distributed() { return nil, errors.New("Volume create requires a configured distributed sanlock backend") }
	if err := ctx.Err(); err != nil { return nil, err }
	if args.SizeBytes == 0 || args.SizeBytes%format.BlockSize != 0 { return nil, fmt.Errorf("Volume size must be a positive multiple of %d bytes", format.BlockSize) }
	if !format.ValidDynamicVolumeName(args.Name) { return nil, errors.New("invalid Volume name") }
	if args.CreateToken == ([16]byte{}) { return nil, errors.New("Volume create requires a nonzero idempotency token") }
	pools, err := b.scan(ctx); if err != nil { return nil, err }
	var pool *discoveredPool
	for i := range pools { if pools[i].Name == args.Pool { pool = &pools[i]; break } }
	if pool == nil { return nil, fmt.Errorf("Pool %q was not found in configured discovery devices", args.Pool) }

	// This RINDEX-backed resource is stable across nodes and independent of /dev path.
	release, err := b.locker.Lock(ctx, "pool-catalog/"+pool.UUID)
	if err != nil { return nil, fmt.Errorf("acquire distributed Pool catalog lock: %w", err) }
	defer func() { retErr = errors.Join(retErr, release()) }()

	parent, err := device.OpenBlockDevice(pool.Device, true, false)
	if err != nil { return nil, fmt.Errorf("open Pool LUN for Volume create: %w", err) }
	defer parent.Close()
	bootstrap, err := poolspace.OpenBootstrap(parent); if err != nil { return nil, err }
	record := bootstrap.Record()
	if record.PoolName != args.Pool || fmt.Sprintf("%x", record.PoolUUID) != pool.UUID { return nil, errors.New("Pool identity changed after catalog lock acquisition") }
	metadata, data, generation, err := bootstrap.OpenViews(); if err != nil { return nil, err }
	defer metadata.Close()
	if data != nil { defer data.Close() }
	if generation == 0 { return nil, errors.New("Pool MetadataView generation is zero") }
	sb, err := allocator.OpenDynamicSuperblockPair(metadata); if err != nil { return nil, err }
	if sb.PoolUUID != record.PoolUUID || sb.ViewGeneration != generation { return nil, errors.New("Pool Superblock does not match Bootstrap identity/generation") }
	catalog, err := metastore.OpenDynamicVolumeAllocator(metadata, sb)
	if err != nil { return nil, fmt.Errorf("open writable Volume catalog: %w", err) }
	committer, err := wal.NewDynamicSharedCommitter(metadata, sb, 0, b.locker, fmt.Sprintf("wal/%x/0", record.PoolUUID))
	if err != nil { return nil, err }
	existing, lookupErr := catalog.FindVolumeByCreateToken(args.CreateToken)
	if lookupErr == nil {
		if existing.Descriptor.VolumeName != args.Name || existing.Descriptor.LogicalSizeBytes != args.SizeBytes {
			return nil, metastore.ErrVolumeCreateTokenConflict
		}
		return discoveredVolume{Pool: pool.Name, PoolUUID: pool.UUID, Device: pool.Device, Index: existing.Index, Descriptor: existing.Descriptor}, nil
	}
	if !errors.Is(lookupErr, metastore.ErrVolumeCatalogNotFound) { return nil, fmt.Errorf("lookup Volume create idempotency token: %w", lookupErr) }
	id, err := newVolumeUUID(); if err != nil { return nil, err }
	created, err := catalog.CreateVolume(metastore.VolumeCreateRequest{
		VolumeID: id, Name: args.Name, SizeBytes: args.SizeBytes, CreateToken: args.CreateToken,
		OwnerEpoch: 1, ArenaPages: initialVolumeArenaPages, LifecyclePages: initialVolumeLifecyclePages,
	}, committer)
	if err != nil { return nil, fmt.Errorf("commit Volume descriptor and Reserved allocation: %w", err) }
	return discoveredVolume{Pool: pool.Name, PoolUUID: pool.UUID, Device: pool.Device, Index: created.Entry.Index, Descriptor: created.Entry.Descriptor}, nil
}

func (*catalogLifecycleBackend) unsupported(action string) (any, error) {
	return nil, fmt.Errorf("%s is not available yet: production SAN-fenced Pool/Volume runtime integration is still in progress", action)
}
func (b *catalogLifecycleBackend) PoolCreate(ctx context.Context, args control.PoolCreateArgs) (any, error) {
	if b == nil || ctx == nil || b.locker == nil || !b.locker.Distributed() { return nil, errors.New("Pool create requires a distributed sanlock backend") }
	if strings.TrimSpace(args.Name) == "" || len(args.Name) > 23 { return nil, errors.New("Pool name must contain 1-23 supported ASCII characters") }
	configured := false
	for _, path := range b.devices { if path == args.Device { configured = true; break } }
	if !configured { return nil, errors.New("Pool create device must be listed in discovery_devices") }
	identity, err := device.StableBlockIdentity(args.Device)
	if err != nil { return nil, fmt.Errorf("resolve stable Pool device identity: %w", err) }
	dev, err := device.OpenBlockDevice(args.Device, true, false)
	if err != nil { return nil, fmt.Errorf("open Pool device for initialization: %w", err) }
	defer dev.Close()
	if entry, ready, readyErr := poolReady(dev, args.Name); readyErr != nil { return nil, readyErr } else if ready {
		return map[string]any{"pool": entry, "device": args.Device, "state": "ready", "idempotent": true}, nil
	}

	paths := make([]fence.PRPath, 0, len(b.prPaths))
	for _, path := range b.prPaths {
		pathID, idErr := device.StableBlockIdentity(path.Device)
		if idErr != nil { return nil, fmt.Errorf("resolve configured SAN path identity for %s: %w", path.Device, idErr) }
		if pathID == identity { paths = append(paths, path) }
	}
	if len(paths) == 0 { return nil, fmt.Errorf("no configured SCSI PR paths match Pool device WWID %s", identity) }
	if strings.TrimSpace(b.nodeID) == "" || strings.TrimSpace(b.bootID) == "" { return nil, errors.New("Pool bootstrap requires configured node_id and boot_id") }
	reservation := &fence.SCSIPROwnerReservation{Binary: b.prBinary, Paths: paths, ReservationType: b.prType}
	ownerLease, err := b.locker.Acquire(ctx, "pool-owner/"+identity)
	if err != nil { return nil, fmt.Errorf("acquire distributed Pool Owner resource: %w", err) }
	if ownerLease == nil || !ownerLease.Valid() {
		if ownerLease != nil { _ = ownerLease.Close() }
		return nil, errors.New("distributed Pool Owner lease is not valid")
	}
	closeLeaseOnError := true
	defer func() { if closeLeaseOnError { _ = ownerLease.Close() } }()
	if err = reservation.VerifyNoForeignRegistrations(ctx); err != nil { return nil, fmt.Errorf("Pool bootstrap cannot prove that no foreign SAN initiator is registered: %w", err) }
	if err = reservation.PrepareOwnerReservation(ctx, b.nodeID, b.bootID); err != nil { return nil, fmt.Errorf("prepare Pool bootstrap SCSI PR registrations: %w", err) }
	if err = reservation.EstablishOwnerReservation(ctx, b.nodeID, b.bootID); err != nil { return nil, fmt.Errorf("establish Pool bootstrap SCSI PR reservation: %w", err) }
	epoch := uint64(1)
	if existing, openErr := poolspace.OpenBootstrap(dev); openErr == nil {
		if existing.Record().OwnerEpoch == ^uint64(0) { return nil, errors.New("Pool OwnerEpoch exhausted") }
		epoch = existing.Record().OwnerEpoch + 1
	} else if !errors.Is(openErr, poolspace.ErrBootstrapUnformatted) && !errors.Is(openErr, poolspace.ErrBootstrapFormatIncomplete) {
		return nil, fmt.Errorf("inspect Pool Bootstrap OwnerEpoch: %w", openErr)
	}
	owner := &poolBootstrapOwnerLease{lease: ownerLease, reservation: reservation, nodeID: b.nodeID, bootID: b.bootID, epoch: epoch}
	initResult, initErr := poolinit.Initialize(ctx, dev, args.Name, "pool-init/"+identity, b.locker, owner, poolinit.Config{})
	closeErr := owner.Close()
	closeLeaseOnError = false
	if initErr != nil || closeErr != nil { return nil, errors.Join(initErr, closeErr) }
	return map[string]any{"pool": initResult, "device": args.Device, "state": "ready", "idempotent": false}, nil
}
func (b *catalogLifecycleBackend) PoolDestroy(context.Context, string) (any, error) { return b.unsupported("pool destroy") }
func (b *catalogLifecycleBackend) VolumeDelete(context.Context, string) (any, error) { return b.unsupported("volume delete") }
func (b *catalogLifecycleBackend) VolumeActivate(ctx context.Context,id,mode string) (any,error) {
	if b==nil||b.runtime==nil{return nil,errors.New("dynamic Volume Runtime manager is not configured")}
	if mode!="rw" { return nil,errors.New("dynamic RO activation requires a cluster-coordinated Reader slot allocator and is not enabled yet") }
	item,err:=b.findVolume(ctx,id);if err!=nil{return nil,err}
	dmName,err:=dmowner.BlockMeshDMName(id);if err!=nil{return nil,err}
	parent,err:=device.OpenBlockDevice(item.Device,false,false);if err!=nil{return nil,fmt.Errorf("open Pool LUN to resolve DataView extents: %w",err)}
	bootstrap,err:=poolspace.OpenBootstrap(parent);if err!=nil{_=parent.Close();return nil,err}
	var linearExtents []dmowner.LinearExtent
	for _,extent:=range bootstrap.Extents(){if extent.Kind==poolspace.Data{
		if extent.Start%512!=0||extent.Length%512!=0{_=parent.Close();return nil,errors.New("Pool Data extent is not 512-byte aligned")}
		linearExtents=append(linearExtents,dmowner.LinearExtent{StartSector:extent.Start/512,Sectors:extent.Length/512})
	}}
	_=parent.Close()
	dataDevice,err:=b.dataViews.Ensure(ctx,item.PoolUUID,item.Device,linearExtents)
	if err!=nil{return nil,fmt.Errorf("create/verify Pool DataView linear device: %w",err)}
	if _,err=b.tables.Ensure(ctx,dmowner.TableSpec{VolumeID:id,LogicalSizeBytes:item.Descriptor.LogicalSizeBytes,
		BackingDevice:dataDevice,DataStartSector:0,NodeID:b.nodeID,BootID:b.bootID});err!=nil{return nil,fmt.Errorf("create closed/fenced DM target: %w",err)}
	spec:=metastore.VolumeRuntimeSpec{VolumeID:id,Mode:metastore.VolumeRuntimeReadWrite,NodeID:b.nodeID,BootID:b.bootID,VolumeIndex:item.Index}
	if err=b.runtime.Activate(ctx,spec);err!=nil{return nil,fmt.Errorf("activate fenced BMPOOL03 RW Runtime: %w",err)}
	return map[string]any{"volume_id":id,"mode":"rw","dm_device":dmName,"device":"/dev/mapper/"+dmName},nil
}
func (b *catalogLifecycleBackend) VolumeDeactivate(ctx context.Context,id string) (any,error) {
	if b==nil||b.runtime==nil{return nil,errors.New("dynamic Volume Runtime manager is not configured")}
	if err:=b.runtime.Deactivate(ctx,id);err!=nil{return nil,err}
	dmName,err:=dmowner.BlockMeshDMName(id);if err!=nil{return nil,err}
	names,err:=b.tables.List(ctx);if err!=nil{return nil,err}
	for _,name:=range names {
		if name!=dmName{continue}
		if err=b.dmSetup.FenceCurrent(ctx,name);err!=nil{return nil,fmt.Errorf("fence orphaned DM target %s: %w",name,err)}
		if err=b.tables.RemoveName(ctx,name);err!=nil{return nil,err}
	}
	return map[string]any{"volume_id":id,"state":"inactive"},nil
}
