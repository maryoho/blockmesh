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
	fmt.Fprintln(os.Stderr, `bm-meta — BlockMesh fixed-LBA metadata device utility

Usage:
  bm-meta inspect --device /dev/mapper/<metadata-lun> --volume-id <id>
  bm-meta format  --device /dev/mapper/<metadata-lun> --volume-id <id> \
      --arena-start <page> --arena-pages <count> \
      --lifecycle-start <page> --lifecycle-pages <count> --confirm-format
  bm-meta demo [demo flags] <demo command>

format is destructive and requires --confirm-format. It opens the block
device with O_EXCL. inspect is read-only and does not acquire a cluster lease.

The demo command uses JSON state files, MemoryLease and NoopFencer. It is
isolated from the fixed-LBA block-device commands and is not production-safe.`)
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
