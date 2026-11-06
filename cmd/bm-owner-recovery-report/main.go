package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"blockmesh/internal/ownerqual"
	"blockmesh/internal/runtimeevents"
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
	journalPath:=fs.String("runtime-events-journal","","P3.17 hash-chained JSONL journal")
	exportPath:=fs.String("runtime-events-export","","exclusive verified runtime event JSON export")
	output:=fs.String("output","","Markdown report path; exclusive create")
	if err:=fs.Parse(args); err!=nil{return err}
	if fs.NArg()!=0 || *input=="" || *after=="" || *journalPath=="" || *exportPath=="" {
		return errors.New("--evidence, --after-owner-snapshot, --runtime-events-journal and --runtime-events-export are required; no positional arguments are accepted")
	}
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
	records,err:=runtimeevents.Read(*journalPath)
	if err!=nil{return fmt.Errorf("read runtime event journal: %w",err)}
	eventExport,err:=runtimeevents.VerifyRecovery(records)
	if err!=nil{return fmt.Errorf("verify runtime event recovery timeline: %w",err)}
	if eventExport.VolumeID!=e.VolumeID || eventExport.NodeID!=e.NewOwnerNodeID ||
		eventExport.BootID!=e.NewOwnerBootID || eventExport.OwnerEpoch!=e.OwnerEpoch {
		return errors.New("runtime event Volume/Node/Boot/Epoch does not match runtime evidence and durable SAN snapshot")
	}
	hasPreempt,hasDrain:=false,false
	for _,record:=range records {
		switch record.Event.EventType {
		case "scsi_pr.preempt_and_abort": hasPreempt=true
		case "old_owner.metadata_io_drain": hasDrain=true
		}
	}
	if !hasPreempt || !hasDrain {
		return errors.New("P3.15 takeover report requires both old-owner PREEMPT AND ABORT and metadata I/O drain runtime events")
	}
	if err=runtimeevents.WriteExport(*exportPath,eventExport);err!=nil{return fmt.Errorf("write verified runtime event export: %w",err)}
	journalDigest,err:=fileSHA256(*journalPath);if err!=nil{return err}
	exportDigest,err:=fileSHA256(*exportPath);if err!=nil{return err}
	e.Artifacts=append(e.Artifacts,
		ownerqual.Artifact{Name:"runtime-events-journal",Path:*journalPath,SHA256:journalDigest},
		ownerqual.Artifact{Name:"runtime-events-export",Path:*exportPath,SHA256:exportDigest},
	)
	result,err:=ownerqual.Verify(e);if err!=nil{return err}
	report:=ownerqual.Markdown(e,result)+runtimeTimelineMarkdown(eventExport)
	if *output=="" { _,err=os.Stdout.WriteString(report) } else {
		var f *os.File
		f,err=os.OpenFile(*output,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
		if err!=nil{return fmt.Errorf("create report (existing files are never overwritten): %w",err)}
		_,writeErr:=f.WriteString(report);syncErr:=f.Sync();closeErr:=f.Close()
		err=errors.Join(writeErr,syncErr,closeErr)
	}
	if err!=nil{return err}
	if !result.Passed{return errors.New("qualification did not PASS; inspect FAIL / INCONCLUSIVE report")}
	fmt.Fprintf(os.Stderr,"PASS: full Owner recovery evidence and %d runtime events validated (operation %s)\n",len(eventExport.Records),eventExport.OperationID)
	return nil
}
func fileSHA256(path string)(string,error){
	b,err:=os.ReadFile(path);if err!=nil{return "",err}
	sum:=sha256.Sum256(b)
	return hex.EncodeToString(sum[:]),nil
}
func runtimeTimelineMarkdown(e runtimeevents.Export) string {
	var b strings.Builder
	fmt.Fprintf(&b,"\n## Runtime event timeline (P3.17)\n\n- **Operation ID:** %s\n- **Event schema:** %s\n- **Verified records:** %d\n- **Journal chain:** SHA-256 verified\n- **Identity:** %s / %s / %s\n- **OwnerEpoch:** %d\n\n| Seq | Observed at (UTC) | Event | Phase | Proof boundary |\n|---:|---|---|---|---|\n",
		e.OperationID,e.SchemaVersion,len(e.Records),e.VolumeID,e.NodeID,e.BootID,e.OwnerEpoch)
	for _,r:=range e.Records {
		ev:=r.Event
		fmt.Fprintf(&b,"| %d | %s | %s | %s | %s |\n",r.Sequence,ev.ObservedAt.UTC().Format("2006-01-02T15:04:05.000Z"),ev.EventType,ev.Phase,ev.Proof)
	}
	b.WriteString("\n> Runtime events prove that the instrumented caller observed the corresponding method return. They do not independently prove SAN hardware state, per-path SCSI PR results, host fencing, zero inflight I/O, or application data integrity; those remain covered by the separately hashed raw evidence artifacts.\n")
	return b.String()
}
