package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"blockmesh/internal/device"
	"blockmesh/internal/fence"
	"blockmesh/internal/metadata"
	"blockmesh/internal/format"
	"blockmesh/internal/volume"
)

type deviceReport struct {
	DevicePath string              `json:"device_path"`
	DeviceBytes int64               `json:"device_bytes"`
	LogicalSectorSize int           `json:"logical_sector_size"`
	ReadOnly bool                   `json:"read_only"`
	Superblock format.Superblock `json:"pool_superblock"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "format":
		err = runFormat(os.Args[2:])
	case "inspect":
		err = runInspect(os.Args[2:])
	case "volume-init":
		err = runVolumeInit(os.Args[2:])
	case "volume-inspect":
		err = runVolumeInspect(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `bm-meta — BlockMesh Pool metadata utility

Usage:
  bm-meta inspect --device /dev/mapper/<metadata-lun>
  bm-meta format  --device /dev/mapper/<metadata-lun> --data-start-sector <sector> --confirm-format
  bm-meta volume-init --device /dev/mapper/<metadata-lun> --index <slot> --volume-id <id> --owner-epoch <epoch> --confirm-init
  bm-meta volume-inspect --device /dev/mapper/<metadata-lun> --index <slot>
  bm-meta demo [demo flags] <demo command>

format is destructive: it writes BMPOOL02 Pool Superblocks at LBA 0/1 and
initializes Pool-level AG metadata. It does not initialize Volume Descriptors.
inspect is read-only and does not acquire a cluster lease.
volume-init is destructive to one empty Node Table slot and requires external
Pool quiesce/fencing; local O_EXCL is not a distributed lock.
volume-inspect is read-only.

The demo command uses JSON state files, MemoryLease and NoopFencer. It is
not production-safe.`)
}

func runFormat(args []string) error {
	fs:=flag.NewFlagSet("format",flag.ContinueOnError)
	path:=fs.String("device","","Linux block device or multipath device")
	dataStart:=fs.Uint64("data-start-sector",0,"data area start sector")
	confirm:=fs.Bool("confirm-format",false,"confirm destructive Pool format")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0{return errors.New("unexpected positional arguments")}
	if !*confirm{return errors.New("refusing destructive Pool format without --confirm-format")}
	if *path==""{return errors.New("--device is required")}
	dev,err:=device.OpenBlockDevice(*path,true,true);if err!=nil{return err};defer dev.Close()
	if uint64(dev.Size())<format.MetadataSize{return fmt.Errorf("device size %d is smaller than Pool metadata size %d",dev.Size(),format.MetadataSize)}
	if err=metadata.InitDevice(dev,*dataStart,true);err!=nil{return fmt.Errorf("format Pool metadata: %w",err)}
	store,err:=metadata.OpenStoreDevice(dev);if err!=nil{return err}
	return writeReport(dev,store,false)
}

func runInspect(args []string) error {
	fs:=flag.NewFlagSet("inspect",flag.ContinueOnError)
	path:=fs.String("device","","Linux block device or multipath device")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0{return errors.New("unexpected positional arguments")}
	if *path==""{return errors.New("--device is required")}
	dev,err:=device.OpenBlockDevice(*path,false,false);if err!=nil{return err};defer dev.Close()
	store,err:=metadata.OpenStoreDevice(dev);if err!=nil{return fmt.Errorf("open Pool metadata: %w",err)}
	return writeReport(dev,store,true)
}

type volumeDeviceReport struct {
	DevicePath string `json:"device_path"`
	VolumeIndex uint32 `json:"volume_index"`
	Descriptor format.VolumeDescriptor `json:"volume_descriptor"`
}

func runVolumeInit(args []string) error {
	fs := flag.NewFlagSet("volume-init", flag.ContinueOnError)
	path := fs.String("device", "", "Linux block device or multipath device")
	index := fs.Uint("index", 0, "Node Table Volume slot")
	volumeID := fs.String("volume-id", "", "unique Volume ID")
	ownerEpoch := fs.Uint64("owner-epoch", 0, "initial non-zero Owner Epoch")
	lifecyclePages := fs.Uint64("lifecycle-pages", 32, "reserved Lifecycle pages (2..32)")
	confirm := fs.Bool("confirm-init", false, "confirm Volume Descriptor initialization")
	if err := fs.Parse(args); err != nil { return err }
	if fs.NArg() != 0 { return errors.New("unexpected positional arguments") }
	if *path == "" || *volumeID == "" || *ownerEpoch == 0 { return errors.New("--device, --volume-id and non-zero --owner-epoch are required") }
	if !*confirm { return errors.New("refusing Volume initialization without --confirm-init") }
	if uint64(*index) >= format.MaxVolumeDescriptors { return errors.New("Volume index exceeds Node Table capacity") }
	dev, err := device.OpenBlockDevice(*path, true, true)
	if err != nil { return err }
	defer dev.Close()
	pool, err := metadata.OpenStoreDevice(dev)
	if err != nil { return fmt.Errorf("open Pool metadata: %w", err) }
	desc, err := metadata.InitVolume(dev, pool.SB, uint32(*index), *volumeID, *ownerEpoch, *lifecyclePages)
	if err != nil { return fmt.Errorf("initialize Volume Descriptor: %w", err) }
	return json.NewEncoder(os.Stdout).Encode(volumeDeviceReport{DevicePath: dev.Path(), VolumeIndex: uint32(*index), Descriptor: desc})
}

func runVolumeInspect(args []string) error {
	fs := flag.NewFlagSet("volume-inspect", flag.ContinueOnError)
	path := fs.String("device", "", "Linux block device or multipath device")
	index := fs.Uint("index", 0, "Node Table Volume slot")
	if err := fs.Parse(args); err != nil { return err }
	if fs.NArg() != 0 { return errors.New("unexpected positional arguments") }
	if *path == "" { return errors.New("--device is required") }
	if uint64(*index) >= format.MaxVolumeDescriptors { return errors.New("Volume index exceeds Node Table capacity") }
	dev, err := device.OpenBlockDevice(*path, false, false)
	if err != nil { return err }
	defer dev.Close()
	pool, err := metadata.OpenStoreDevice(dev)
	if err != nil { return fmt.Errorf("open Pool metadata: %w", err) }
	desc, err := metadata.ReadVolumeDescriptor(dev, pool.SB, uint32(*index))
	if err != nil { return err }
	return json.NewEncoder(os.Stdout).Encode(volumeDeviceReport{DevicePath: dev.Path(), VolumeIndex: uint32(*index), Descriptor: desc})
}

func writeReport(dev *device.BlockDevice,store *metadata.Store,readOnly bool) error {
	return json.NewEncoder(os.Stdout).Encode(deviceReport{
		DevicePath:dev.Path(),DeviceBytes:dev.Size(),LogicalSectorSize:dev.LogicalSectorSize(),
		ReadOnly:readOnly,Superblock:store.SB,
	})
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	walPath := fs.String("wal", "/tmp/blockmesh/vol-a.wal", "demo WAL file")
	state := fs.String("state", "/tmp/blockmesh/vol-a.state", "demo state snapshot")
	vol := fs.String("volume", "vol-a", "demo volume id")
	node := fs.String("node", "node-a", "demo node id")
	blocks := fs.Uint64("blocks", 1024, "demo physical 4MiB blocks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, err := metadata.OpenEngine(metadata.Config{
		VolumeID: *vol, NodeID: *node, StatePath: *state, WALPath: *walPath,
		TotalBlocks: *blocks, Lease: volume.NewMemoryLease(), Fencer: fence.NoopFencer{},
	})
	if err != nil {
		return err
	}
	defer e.Close()
	if fs.NArg() == 0 {
		fmt.Println("demo commands: activate-rw | put <logical> <physical> <length> | alloc <logical> <length> | lookup <logical> | discard <logical> <length> | descriptor")
		return nil
	}
	ctx := context.Background()
	args = fs.Args()
	switch args[0] {
	case "activate-rw":
		err = e.ActivateRW(ctx)
	case "put":
		var l, p, n uint64
		if len(args) != 4 { return errors.New("put logical physical length") }
		_, err = fmt.Sscan(args[1], &l)
		if err == nil { _, err = fmt.Sscan(args[2], &p) }
		if err == nil { _, err = fmt.Sscan(args[3], &n) }
		if err == nil { err = e.PutMapping(ctx, l, p, n) }
	case "alloc":
		var l, n uint64
		if len(args) != 3 { return errors.New("alloc logical length") }
		_, err = fmt.Sscan(args[1], &l)
		if err == nil { _, err = fmt.Sscan(args[2], &n) }
		if err == nil {
			var p uint64
			p, err = e.AllocateMapping(ctx, l, n)
			if err == nil { fmt.Println(p) }
		}
	case "lookup":
		var l uint64
		if len(args) != 2 { return errors.New("lookup logical") }
		_, err = fmt.Sscan(args[1], &l)
		if err == nil {
			var p uint64
			p, err = e.Lookup(l)
			if err == nil { fmt.Println(p) }
		}
	case "discard":
		var l, n uint64
		if len(args) != 3 { return errors.New("discard logical length") }
		_, err = fmt.Sscan(args[1], &l)
		if err == nil { _, err = fmt.Sscan(args[2], &n) }
		if err == nil { err = e.Discard(ctx, l, n) }
	case "descriptor":
		fmt.Printf("%+v\n", e.Descriptor())
	default:
		return fmt.Errorf("unknown demo command %q", args[0])
	}
	return err
}
