package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"blockmesh/internal/owneragent"
)

func main() {
	if err:=run(os.Args[1:]);err!=nil {fmt.Fprintln(os.Stderr,"bm-owner-san-agent:",err);os.Exit(1)}
}
func run(args []string) error {
	if len(args)==0 {return errors.New("usage: capture --volume-id ... | verify-pair --node-a file --node-b file")}
	switch args[0] {
	case "capture": return capture(args[1:])
	case "verify-pair": return verifyPair(args[1:])
	default: return fmt.Errorf("unknown command %q",args[0])
	}
}
func capture(args []string) error {
	fs:=flag.NewFlagSet("capture",flag.ContinueOnError)
	volume:=fs.String("volume-id","","BlockMesh Volume ID")
	node:=fs.String("node-id","","local configured NodeID")
	boot:=fs.String("boot-id","","local BootID (default read from proc)")
	mpath:=fs.String("multipath-map","","multipath map block device")
	paths:=fs.String("paths","","comma-separated exhaustive component block paths")
	dm:=fs.String("dm-device","","BlockMesh DM target name")
	unit:=fs.String("systemd-unit","","BlockMesh systemd unit")
	index:=fs.Uint("metadata-index",0,"Pool Volume index")
	snapshotBin:=fs.String("owner-snapshot-bin","bm-owner-crash-qualify","read-only metadata snapshot binary")
	timeout:=fs.Duration("timeout",10*time.Second,"per-command timeout")
	output:=fs.String("output","","new evidence JSON file; exclusive create")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0 || *output=="" {return errors.New("--output is required and positional arguments are not accepted")}
	pathList:=strings.Split(*paths,",")
	for i:=range pathList {pathList[i]=strings.TrimSpace(pathList[i])}
	b,err:=owneragent.Capture(context.Background(),owneragent.Config{
		VolumeID:*volume,NodeID:*node,BootID:*boot,MultipathMap:*mpath,Paths:pathList,
		DMDevice:*dm,SystemdUnit:*unit,MetadataIndex:*index,OwnerSnapshotBinary:*snapshotBin,Timeout:*timeout,
	},nil)
	if err!=nil{return err}
	f,err:=os.OpenFile(*output,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
	if err!=nil{return fmt.Errorf("create evidence bundle (existing files are never overwritten): %w",err)}
	enc:=json.NewEncoder(f);enc.SetIndent("","  ")
	writeErr:=enc.Encode(b);syncErr:=f.Sync();closeErr:=f.Close()
	if err=errors.Join(writeErr,syncErr,closeErr);err!=nil{return err}
	fmt.Fprintf(os.Stderr,"captured read-only SAN evidence for node %s (%s); bundle: %s\n",b.NodeID,b.BootID,*output)
	return nil
}
func verifyPair(args []string) error {
	fs:=flag.NewFlagSet("verify-pair",flag.ContinueOnError)
	aPath:=fs.String("node-a","","Node A evidence bundle")
	bPath:=fs.String("node-b","","Node B evidence bundle")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0 || *aPath=="" || *bPath=="" {return errors.New("--node-a and --node-b are required")}
	a,err:=load(*aPath);if err!=nil{return err}
	b,err:=load(*bPath);if err!=nil{return err}
	result:=owneragent.VerifyPair(a,b)
	enc:=json.NewEncoder(os.Stdout);enc.SetIndent("","  ")
	if err=enc.Encode(result);err!=nil{return err}
	if !result.Passed{return errors.New("two-node SAN evidence is incomplete or inconsistent")}
	fmt.Fprintln(os.Stderr,"PASS: both nodes observe the same SAN LUN and durable OwnerRecord")
	return nil
}
func load(path string)(owneragent.Bundle,error) {
	var b owneragent.Bundle
	raw,err:=os.ReadFile(path);if err!=nil{return b,err}
	if err=json.Unmarshal(raw,&b);err!=nil{return b,fmt.Errorf("decode %s: %w",path,err)}
	return b,nil
}
