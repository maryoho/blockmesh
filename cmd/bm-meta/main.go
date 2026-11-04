package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"blockmesh/internal/fence"
	"blockmesh/internal/metadata"
	"blockmesh/internal/volume"
)

func main() {
	walPath := flag.String("wal", "/tmp/blockmesh/vol-a.wal", "WAL file (demo only)")
	state := flag.String("state", "/tmp/blockmesh/vol-a.state", "state snapshot (demo only)")
	vol := flag.String("volume", "vol-a", "volume id")
	node := flag.String("node", "node-a", "node id")
	blocks := flag.Uint64("blocks", 1024, "physical 4MiB blocks")
	flag.Parse()
	e, err := metadata.OpenEngine(metadata.Config{VolumeID: *vol, NodeID: *node, StatePath: *state, WALPath: *walPath, TotalBlocks: *blocks, Lease: volume.NewMemoryLease(), Fencer: fence.NoopFencer{}})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	if len(flag.Args()) == 0 {
		fmt.Println("commands: activate-rw | put <logical> <physical> <length> | alloc <logical> <length> | lookup <logical> | discard <logical> <length> | descriptor")
		return
	}
	ctx := context.Background()
	args := flag.Args()
	switch args[0] {
	case "activate-rw":
		err = e.ActivateRW(ctx)
	case "put":
		var l, p, n uint64
		if len(args) != 4 {
			log.Fatal("put logical physical length")
		}
		_, err = fmt.Sscan(args[1], &l)
		if err == nil {
			_, err = fmt.Sscan(args[2], &p)
		}
		if err == nil {
			_, err = fmt.Sscan(args[3], &n)
		}
		if err == nil {
			err = e.PutMapping(ctx, l, p, n)
		}
	case "alloc":
		var l, n uint64
		if len(args) != 3 {
			log.Fatal("alloc logical length")
		}
		_, err = fmt.Sscan(args[1], &l)
		if err == nil {
			_, err = fmt.Sscan(args[2], &n)
		}
		if err == nil {
			var p uint64
			p, err = e.AllocateMapping(ctx, l, n)
			if err == nil {
				fmt.Println(p)
			}
		}
	case "lookup":
		var l uint64
		if len(args) != 2 {
			log.Fatal("lookup logical")
		}
		_, err = fmt.Sscan(args[1], &l)
		if err == nil {
			var p uint64
			p, err = e.Lookup(l)
			if err == nil {
				fmt.Println(p)
			}
		}
	case "discard":
		var l, n uint64
		if len(args) != 3 {
			log.Fatal("discard logical length")
		}
		_, err = fmt.Sscan(args[1], &l)
		if err == nil {
			_, err = fmt.Sscan(args[2], &n)
		}
		if err == nil {
			err = e.Discard(ctx, l, n)
		}
	case "descriptor":
		fmt.Printf("%+v\n", e.Descriptor())
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}
