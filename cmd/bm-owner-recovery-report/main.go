package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"blockmesh/internal/ownerqual"
)

type ownerSnapshot struct {
	VolumeID string `json:"volume_id"`
	SelectedOwner *struct {
		VolumeID string `json:"volume_id"`
		NodeID string `json:"node_id"`
		BootID string `json:"boot_id"`
		Epoch uint64 `json:"epoch"`
	} `json:"selected_owner"`
}

func main() {
	if err:=run(os.Args[1:]); err!=nil { fmt.Fprintln(os.Stderr,"bm-owner-recovery-report:",err); os.Exit(1) }
}
func run(args []string) error {
	fs:=flag.NewFlagSet("bm-owner-recovery-report",flag.ContinueOnError)
	input:=fs.String("evidence","","runtime evidence JSON")
	after:=fs.String("after-owner-snapshot","","post-recovery metadata snapshot produced by bm-owner-crash-qualify")
	output:=fs.String("output","","Markdown report path; exclusive create")
	if err:=fs.Parse(args); err!=nil{return err}
	if fs.NArg()!=0 || *input=="" || *after=="" { return errors.New("--evidence and --after-owner-snapshot are required; no positional arguments are accepted") }
	raw,err:=os.ReadFile(*input); if err!=nil{return err}
	var e ownerqual.Evidence
	if err=json.Unmarshal(raw,&e);err!=nil{return fmt.Errorf("decode runtime evidence: %w",err)}
	snapshotBytes,err:=os.ReadFile(*after);if err!=nil{return fmt.Errorf("read post-recovery metadata snapshot: %w",err)}
	var s ownerSnapshot
	if err=json.Unmarshal(snapshotBytes,&s);err!=nil{return fmt.Errorf("decode post-recovery metadata snapshot: %w",err)}
	if s.SelectedOwner==nil{return errors.New("post-recovery metadata snapshot has no selected durable OwnerRecord")}
	if e.VolumeID!=s.VolumeID || e.VolumeID!=s.SelectedOwner.VolumeID ||
		e.NewOwnerNodeID!=s.SelectedOwner.NodeID || e.NewOwnerBootID!=s.SelectedOwner.BootID ||
		e.OwnerEpoch!=s.SelectedOwner.Epoch {
		return errors.New("runtime evidence Volume/Node/Boot/Epoch does not match the durable post-recovery SAN metadata snapshot")
	}
	result,err:=ownerqual.Verify(e);if err!=nil{return err}
	report:=ownerqual.Markdown(e,result)
	if *output=="" { _,err=os.Stdout.WriteString(report) } else {
		var f *os.File
		f,err=os.OpenFile(*output,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
		if err!=nil{return fmt.Errorf("create report (existing files are never overwritten): %w",err)}
		_,writeErr:=f.WriteString(report);syncErr:=f.Sync();closeErr:=f.Close()
		err=errors.Join(writeErr,syncErr,closeErr)
	}
	if err!=nil{return err}
	if !result.Passed{return errors.New("qualification did not PASS; inspect FAIL / INCONCLUSIVE report")}
	fmt.Fprintln(os.Stderr,"PASS: full Owner recovery evidence validated")
	return nil
}
