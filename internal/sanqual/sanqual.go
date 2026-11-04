// Package sanqual provides read-only evidence collection and verification
// helpers for real shared-SAN qualification. It never changes PR state,
// sanlock state, device-mapper state, or host power state.
package sanqual

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type CommandResult struct {
	Binary string `json:"binary"`
	Args []string `json:"args"`
	Output string `json:"output"`
	Error string `json:"error,omitempty"`
	ExitCode int `json:"exit_code"`
	StartedAt time.Time `json:"started_at"`
	Duration time.Duration `json:"duration"`
}

type PathEvidence struct {
	Device string `json:"device"`
	WWID string `json:"wwid,omitempty"`
	ReadKeys CommandResult `json:"read_keys"`
	ReadReservation CommandResult `json:"read_reservation"`
	RegisteredKeys []string `json:"registered_keys,omitempty"`
}

type Evidence struct {
	SchemaVersion string `json:"schema_version"`
	CollectedAt time.Time `json:"collected_at"`
	Hostname string `json:"hostname"`
	VolumeID string `json:"volume_id,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	BootID string `json:"boot_id,omitempty"`
	MultipathMap string `json:"multipath_map,omitempty"`
	Paths []PathEvidence `json:"paths"`
	HostCommands []CommandResult `json:"host_commands"`
	Warnings []string `json:"warnings,omitempty"`
}

type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type ExecRunner struct{}
func (ExecRunner) Run(ctx context.Context, binary string, args ...string) ([]byte,error) {
	return exec.CommandContext(ctx,binary,args...).CombinedOutput()
}

type Collector struct {
	Runner Runner
	Timeout time.Duration
	ValidateDevice func(string) error
}

func (c Collector) defaults() (Runner,time.Duration,func(string)error) {
	r:=c.Runner;if r==nil{r=ExecRunner{}}
	t:=c.Timeout;if t<=0{t=10*time.Second}
	v:=c.ValidateDevice;if v==nil{v=validateBlockDevice}
	return r,t,v
}

func validateBlockDevice(path string) error {
	if !filepath.IsAbs(path){return fmt.Errorf("device path %q must be absolute",path)}
	info,err:=os.Stat(path);if err!=nil{return err}
	if info.Mode()&os.ModeDevice==0{return fmt.Errorf("%s is not a block device",path)}
	if info.Mode()&os.ModeCharDevice!=0{return fmt.Errorf("%s is a character device, not a block device",path)}
	return nil
}

func run(ctx context.Context,r Runner,timeout time.Duration,binary string,args ...string) CommandResult {
	start:=time.Now()
	callCtx,cancel:=context.WithTimeout(ctx,timeout);defer cancel()
	out,err:=r.Run(callCtx,binary,args...)
	result:=CommandResult{Binary:binary,Args:append([]string(nil),args...),Output:strings.TrimSpace(string(out)),StartedAt:start.UTC(),Duration:time.Since(start),ExitCode:0}
	if err!=nil{
		result.Error=err.Error();result.ExitCode=1
		var exitErr *exec.ExitError
		if errors.As(err,&exitErr){result.ExitCode=exitErr.ExitCode()}
		if errors.Is(callCtx.Err(),context.DeadlineExceeded){result.Error="command timed out: "+err.Error()}
	}
	return result
}

func (c Collector) Collect(ctx context.Context, device, multipathMap, volumeID,nodeID,bootID string, paths []string)(Evidence,error) {
	if ctx==nil{return Evidence{},errors.New("context is required")}
	if strings.TrimSpace(device)==""{return Evidence{},errors.New("multipath map device is required")}
	if len(paths)==0{return Evidence{},errors.New("explicit component paths are required; refusing to infer incomplete multipath coverage")}
	r,timeout,validate:=c.defaults()
	seen:=map[string]bool{}
	for _,p:=range paths{
		p=filepath.Clean(strings.TrimSpace(p))
		if p=="."||seen[p]{return Evidence{},fmt.Errorf("empty or duplicate SAN path %q",p)}
		seen[p]=true
		if err:=validate(p);err!=nil{return Evidence{},fmt.Errorf("validate SAN path %s: %w",p,err)}
	}
	host,_:=os.Hostname()
	e:=Evidence{SchemaVersion:"blockmesh-san-qualification/v1",CollectedAt:time.Now().UTC(),Hostname:host,
		VolumeID:volumeID,NodeID:nodeID,BootID:bootID,MultipathMap:device}
	// Every command below is read-only. Keep raw output so an operator can
	// independently inspect the evidence instead of trusting parsed fields.
	e.HostCommands=append(e.HostCommands,
		run(ctx,r,timeout,"multipath","-ll",device),
		run(ctx,r,timeout,"dmsetup","info","-c","--noheadings","-o","name,uuid,attr,open,segments,events",device),
		run(ctx,r,timeout,"dmsetup","status",device),
		run(ctx,r,timeout,"sanlock","client","status"),
		run(ctx,r,timeout,"sanlock","status"),
		run(ctx,r,timeout,"lsblk","-J","-O",device),
	)
	for _,cmd:=range e.HostCommands{if cmd.ExitCode!=0{e.Warnings=append(e.Warnings,cmd.Binary+" "+strings.Join(cmd.Args," ")+": "+cmd.Error)}}
	for _,p:=range paths{
		wwid:=run(ctx,r,timeout,"udevadm","info","--query=property","--name",p)
		keyResult:=run(ctx,r,timeout,"sg_persist","--in","--read-keys","--device="+p)
		reservation:=run(ctx,r,timeout,"sg_persist","--in","--read-reservation","--device="+p)
		pe:=PathEvidence{Device:p,WWID:extractWWID(wwid.Output),ReadKeys:keyResult,ReadReservation:reservation}
		if keyResult.ExitCode==0{pe.RegisteredKeys=parseKeys(keyResult.Output)}else{e.Warnings=append(e.Warnings,"cannot read PR keys on "+p+": "+keyResult.Error)}
		if reservation.ExitCode!=0{e.Warnings=append(e.Warnings,"cannot read PR reservation on "+p+": "+reservation.Error)}
		if wwid.ExitCode!=0{e.Warnings=append(e.Warnings,"cannot read WWID for "+p+": "+wwid.Error)}
		e.Paths=append(e.Paths,pe)
	}
	wwids:=map[string]bool{}
	for _,p:=range e.Paths{if p.WWID!=""{wwids[p.WWID]=true}}
	if len(wwids)>1{e.Warnings=append(e.Warnings,"component paths report different WWIDs; verify they belong to the same LUN")}
	if len(wwids)==0{e.Warnings=append(e.Warnings,"no component-path WWID could be established")}
	return e,nil
}

var (
	keyRE=regexp.MustCompile(`(?i)^\s*key\s*=\s*0x([0-9a-f]{1,16})\s*$`)
	plainKeyRE=regexp.MustCompile(`(?i)^\s*0x([0-9a-f]{1,16})\s*$`)
	wwidRE=regexp.MustCompile(`(?m)^ID_WWN=(\S+)\s*$`)
)
func parseKeys(output string)[]string{
	set:=map[string]bool{}
	for _,line:=range strings.Split(output,"\n"){
		m:=keyRE.FindStringSubmatch(line);if m==nil{m=plainKeyRE.FindStringSubmatch(line)}
		if m!=nil{set[strings.ToLower(strings.TrimLeft(m[1],"0"))]=true}
	}
	keys:=make([]string,0,len(set));for k:=range set{if k==""{k="0"};keys=append(keys,k)}
	sort.Strings(keys);return keys
}
func extractWWID(output string)string{m:=wwidRE.FindStringSubmatch(output);if m==nil{return ""};return m[1]}

type FenceVerification struct {
	TargetKey string `json:"target_key"`
	AbsentOnAllPaths bool `json:"absent_on_all_paths"`
	PresentPaths []string `json:"present_paths,omitempty"`
	UnknownPaths []string `json:"unknown_paths,omitempty"`
}

// VerifyTargetKeyAbsent only inspects an already-collected evidence document.
// It does not issue PREEMPT AND ABORT and cannot itself fence a node.
func VerifyTargetKeyAbsent(e Evidence,targetKey string)(FenceVerification,error){
	key:=strings.ToLower(strings.TrimPrefix(strings.TrimSpace(targetKey),"0x"))
	key=strings.TrimLeft(key,"0");if key==""{key="0"}
	if len(key)>16{return FenceVerification{},errors.New("target PR key exceeds 64 bits")}
	if _,err:=fmt.Sscanf(key,"%x",new(uint64));err!=nil{return FenceVerification{},fmt.Errorf("invalid target PR key %q",targetKey)}
	if len(e.Paths)==0{return FenceVerification{},errors.New("evidence has no SAN paths")}
	v:=FenceVerification{TargetKey:"0x"+key,AbsentOnAllPaths:true}
	for _,p:=range e.Paths{
		if p.ReadKeys.ExitCode!=0{v.UnknownPaths=append(v.UnknownPaths,p.Device);v.AbsentOnAllPaths=false;continue}
		for _,registered:=range p.RegisteredKeys{if registered==key{v.PresentPaths=append(v.PresentPaths,p.Device);v.AbsentOnAllPaths=false;break}}
	}
	return v,nil
}
