package runtimeevents

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Export struct {
	SchemaVersion string   `json:"schema_version"`
	ExportedAt    time.Time `json:"exported_at"`
	OperationID   string   `json:"operation_id"`
	VolumeID      string   `json:"volume_id"`
	NodeID        string   `json:"node_id"`
	BootID        string   `json:"boot_id"`
	OwnerEpoch    uint64   `json:"owner_epoch"`
	Records       []Record `json:"records"`
}

func VerifyRecovery(records []Record) (Export,error) {
	if len(records)==0{return Export{},errors.New("runtime event journal is empty")}
	first:=records[0].Event
	out:=Export{SchemaVersion:"blockmesh-runtime-event-export/v1",ExportedAt:time.Now().UTC(),OperationID:first.OperationID,VolumeID:first.VolumeID,NodeID:first.NodeID,BootID:first.BootID,Records:records}
	positions:=map[string]int{}
	prevHash:=""
	for i,r:=range records{
		e:=r.Event
		if r.Sequence!=uint64(i+1) || r.PreviousHash!=prevHash{return Export{},fmt.Errorf("event journal sequence/hash chain discontinuity at record %d",i+1)}
		wantHash,hashErr:=recordHash(r.Sequence,r.PreviousHash,e)
		if hashErr!=nil{return Export{},hashErr}
		if r.Hash!=wantHash{return Export{},fmt.Errorf("event journal hash mismatch at sequence %d",r.Sequence)}
		if e.SchemaVersion!=SchemaVersion || e.ObservedAt.IsZero() || e.Proof==""{return Export{},fmt.Errorf("event %d has invalid schema, timestamp or proof classification",r.Sequence)}
		prevHash=r.Hash
		if e.OperationID!=out.OperationID || e.VolumeID!=out.VolumeID || e.NodeID!=out.NodeID || e.BootID!=out.BootID{return Export{},fmt.Errorf("event %d changes operation or node identity",r.Sequence)}
		if e.Result!="success"{return Export{},fmt.Errorf("event %d (%s) is not successful",r.Sequence,e.EventType)}
		if _,ok:=positions[e.EventType];ok{return Export{},fmt.Errorf("duplicate event type %q",e.EventType)}
		positions[e.EventType]=i
	}
	required:=[]string{"sanlock.owner_lease_acquired","scsi_pr.registration_prepared","owner_record.durable","scsi_pr.owner_reservation_verified","dm.owner_gate.fenced","wal.recovery.completed","descriptor.owner_epoch.durable","dm.owner_gate.prepared","dm.owner_gate.enabled"}
	for _,name:=range required{if _,ok:=positions[name];!ok{return Export{},fmt.Errorf("required runtime event %q is missing",name)}}
	optional:=[]string{"scsi_pr.preempt_and_abort","old_owner.metadata_io_drain"}
	_,hasFence:=positions[optional[0]]
	_,hasDrain:=positions[optional[1]]
	if hasFence!=hasDrain{return Export{},errors.New("old Owner fencing and I/O drain events must either both exist or both be absent")}
	order:=[]string{"sanlock.owner_lease_acquired","scsi_pr.registration_prepared"}
	if hasFence{order=append(order,optional[0],optional[1])}
	order=append(order,"owner_record.durable","scsi_pr.owner_reservation_verified","dm.owner_gate.fenced","wal.recovery.completed","descriptor.owner_epoch.durable","dm.owner_gate.prepared","dm.owner_gate.enabled")
	last:=-1
	for _,name:=range order{idx,ok:=positions[name];if !ok{continue};if idx<=last{return Export{},fmt.Errorf("runtime event %q is out of required recovery order",name)};last=idx}
	epoch:=records[positions["owner_record.durable"]].Event.OwnerEpoch
	if epoch==0{return Export{},errors.New("durable OwnerRecord event has zero OwnerEpoch")}
	for _,name:=range []string{"scsi_pr.owner_reservation_verified","descriptor.owner_epoch.durable","dm.owner_gate.prepared","dm.owner_gate.enabled"}{
		if records[positions[name]].Event.OwnerEpoch!=epoch{return Export{},fmt.Errorf("event %q OwnerEpoch does not match durable OwnerRecord Epoch %d",name,epoch)}
	}
	out.OwnerEpoch=epoch
	return out,nil
}

func WriteExport(path string, e Export) error {
	if strings.TrimSpace(path)==""{return errors.New("export path is required")}
	b,err:=json.MarshalIndent(e,"","  ");if err!=nil{return err};b=append(b,'\n')
	f,err:=os.OpenFile(path,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if err!=nil{return fmt.Errorf("create export (existing files are never overwritten): %w",err)}
	_,werr:=f.Write(b);serr:=f.Sync();cerr:=f.Close()
	return errors.Join(werr,serr,cerr)
}
