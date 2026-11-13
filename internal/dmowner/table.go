package dmowner

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrDMTableConflict = errors.New("existing BlockMesh DM table does not match requested Volume identity")

type TableSpec struct {
	VolumeID string
	LogicalSizeBytes uint64
	BackingDevice string
	NodeID string
	BootID string
	ReadOnly bool
}

// TableManager creates/removes the DM target table. The kernel target's
// constructor starts fenced with both request gates closed; this manager never
// opens a request gate. Runtime admission remains the responsibility of
// DMRequestGate after SAN fencing and metadata recovery.
type TableManager struct {
	Runner CommandRunner
	Binary string
}

func (m TableManager) runner() (CommandRunner,string) {
	r:=m.Runner
	if r==nil { r=ExecRunner{} }
	b:=m.Binary
	if b=="" { b="dmsetup" }
	return r,b
}

func dmUUIDHex(value string) (string,error) {
	value=strings.ReplaceAll(strings.TrimSpace(value),"-","")
	if len(value)!=32 { return "",fmt.Errorf("identity %q must be a 128-bit UUID",value) }
	if _,err:=hex.DecodeString(value);err!=nil { return "",fmt.Errorf("identity %q is not hexadecimal: %w",value,err) }
	return strings.ToLower(value),nil
}

func BlockMeshDMName(volumeID string) (string,error) {
	id,err:=dmUUIDHex(volumeID)
	if err!=nil { return "",err }
	return "bm-"+id,nil
}

func (m TableManager) table(spec TableSpec) (name,line string,readOnly bool,err error) {
	if spec.LogicalSizeBytes==0 || spec.LogicalSizeBytes%512!=0 {
		err=errors.New("DM target logical size must be a positive multiple of 512 bytes");return
	}
	if strings.TrimSpace(spec.BackingDevice)=="" || strings.ContainsAny(spec.BackingDevice," \t\r\n") {
		err=errors.New("DM backing device path is empty or contains whitespace");return
	}
	volume,node,boot:="","",""
	if volume,err=dmUUIDHex(spec.VolumeID);err!=nil{return}
	if node,err=dmUUIDHex(spec.NodeID);err!=nil{return}
	if boot,err=dmUUIDHex(spec.BootID);err!=nil{return}
	if name,err=BlockMeshDMName(spec.VolumeID);err!=nil{return}
	sectors:=spec.LogicalSizeBytes/512
	line=fmt.Sprintf("0 %d blockmesh %s 0 %s %s %s",sectors,spec.BackingDevice,volume,node,boot)
	readOnly=spec.ReadOnly
	return
}

func (m TableManager) Ensure(ctx context.Context,spec TableSpec) (string,error) {
	if ctx==nil { return "",errors.New("DM table context is required") }
	name,want,readOnly,err:=m.table(spec)
	if err!=nil{return "",err}
	r,b:=m.runner()
	if out,lookupErr:=r.Run(ctx,b,"table",name);lookupErr==nil {
		if strings.TrimSpace(string(out))!=want { return "",fmt.Errorf("%w: name=%s actual=%q expected=%q",ErrDMTableConflict,name,strings.TrimSpace(string(out)),want) }
		if err=m.requireClosedFenced(ctx,r,b,name);err!=nil{return "",fmt.Errorf("existing DM table is not safely reusable: %w",err)}
		mode,modeErr:=r.Run(ctx,b,"info","-c","--noheadings","-o","readonly",name)
		if modeErr!=nil{return "",fmt.Errorf("read existing DM table mode: %w",modeErr)}
		actualRO:=strings.TrimSpace(string(mode))=="1"
		if actualRO!=readOnly{return "",fmt.Errorf("%w: read-only mode differs",ErrDMTableConflict)}
		return name,nil
	}
	args:=[]string{"create",name}
	if readOnly { args=append(args,"--readonly") }
	args=append(args,"--table",want)
	_,createErr:=r.Run(ctx,b,args...)
	// A lost response is ambiguous. Re-read the table and accept only an exact,
	// still-closed instance; never treat a mismatched or open table as success.
	out,lookupErr:=r.Run(ctx,b,"table",name)
	if lookupErr!=nil {
		if createErr!=nil{return "",fmt.Errorf("create DM table %s: %w",name,createErr)}
		return "",fmt.Errorf("DM table %s was not visible after successful create",name)
	}
	if strings.TrimSpace(string(out))!=want{return "",fmt.Errorf("%w after create: name=%s actual=%q expected=%q",ErrDMTableConflict,name,strings.TrimSpace(string(out)),want)}
	if err=m.requireClosedFenced(ctx,r,b,name);err!=nil{return "",fmt.Errorf("new DM target did not start fenced: %w",err)}
	mode,modeErr:=r.Run(ctx,b,"info","-c","--noheadings","-o","readonly",name)
	if modeErr!=nil{return "",fmt.Errorf("verify new DM table mode: %w",modeErr)}
	if (strings.TrimSpace(string(mode))=="1")!=readOnly{return "",fmt.Errorf("%w: read-only mode differs after create",ErrDMTableConflict)}
	return name,nil
}

func (m TableManager) requireClosedFenced(ctx context.Context,r CommandRunner,b,name string) error {
	out,err:=r.Run(ctx,b,"status",name)
	if err!=nil{return err}
	fields:=make(map[string]string)
	for _,field:=range strings.Fields(string(out)) {
		pair:=strings.SplitN(field,"=",2)
		if len(pair)==2 { fields[pair[0]]=pair[1] }
	}
	for key,want:=range map[string]string{"gate":"0","reader_gate":"0","fenced":"1","inflight":"0"} {
		if fields[key]!=want{return fmt.Errorf("DM target %s has %s=%q, expected %s",name,key,fields[key],want)}
	}
	if _,err:=strconv.ParseUint(fields["instance"],16,64);err!=nil{return fmt.Errorf("DM target %s has invalid instance ID",name)}
	return nil
}

// Remove is deliberately strict: callers must first stop Runtime admission and
// drain BIOs through DMRequestGate. It refuses to remove an open or in-flight
// target, preserving the lease-release ordering invariant.
func (m TableManager) Remove(ctx context.Context,volumeID string) error {
	if ctx==nil{return errors.New("DM table context is required")}
	name,err:=BlockMeshDMName(volumeID);if err!=nil{return err}
	r,b:=m.runner()
	if err=m.requireClosedFenced(ctx,r,b,name);err!=nil{return fmt.Errorf("refuse to remove non-fenced DM table %s: %w",name,err)}
	if _,err=r.Run(ctx,b,"remove",name);err!=nil{return fmt.Errorf("remove DM table %s: %w",name,err)}
	return nil
}
