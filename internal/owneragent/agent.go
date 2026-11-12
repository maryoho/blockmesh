// Package owneragent captures read-only, node-local evidence for a real
// two-node SAN Owner recovery qualification. It never issues PR OUT or changes
// sanlock, DM, service, or host power state.
package owneragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"blockmesh/internal/sanqual"
)

const SchemaVersion = "blockmesh-owner-san-agent/v1"

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type ExecRunner struct{}
func (ExecRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

type Config struct {
	VolumeID string
	NodeID string
	BootID string
	MultipathMap string
	Paths []string
	DMDevice string
	SystemdUnit string
	MetadataIndex uint
	OwnerSnapshotBinary string
	Timeout time.Duration
	ValidateDevice func(string) error
}

type Probe struct {
	Name string `json:"name"`
	Result sanqual.CommandResult `json:"result"`
	OutputSHA256 string `json:"output_sha256"`
}

type OwnerSnapshot struct {
	Schema string `json:"schema"`
	VolumeID string `json:"volume_id"`
	VolumeIndex uint32 `json:"volume_index"`
	PoolUUID string `json:"pool_uuid"`
	SelectedOwner *struct {
		VolumeID string `json:"volume_id"`
		NodeID string `json:"node_id"`
		BootID string `json:"boot_id"`
		Epoch uint64 `json:"epoch"`
	} `json:"selected_owner"`
}

type Bundle struct {
	SchemaVersion string `json:"schema_version"`
	CapturedAt time.Time `json:"captured_at"`
	Hostname string `json:"hostname"`
	Kernel string `json:"kernel"`
	VolumeID string `json:"volume_id"`
	NodeID string `json:"node_id"`
	BootID string `json:"boot_id"`
	MultipathMap string `json:"multipath_map"`
	DMDevice string `json:"dm_device"`
	SystemdUnit string `json:"systemd_unit"`
	SAN sanqual.Evidence `json:"san"`
	OwnerSnapshot json.RawMessage `json:"owner_snapshot"`
	Owner OwnerSnapshot `json:"owner"`
	Probes []Probe `json:"probes"`
}

func Capture(ctx context.Context, cfg Config, runner CommandRunner) (Bundle, error) {
	var b Bundle
	if ctx == nil { return b, errors.New("context is required") }
	if runner == nil { runner = ExecRunner{} }
	if strings.TrimSpace(cfg.VolumeID)=="" || strings.TrimSpace(cfg.NodeID)=="" ||
		strings.TrimSpace(cfg.MultipathMap)=="" || strings.TrimSpace(cfg.DMDevice)=="" ||
		strings.TrimSpace(cfg.SystemdUnit)=="" || strings.TrimSpace(cfg.OwnerSnapshotBinary)=="" {
		return b, errors.New("volume, node, multipath map, DM device, systemd unit and metadata snapshot binary are required")
	}
	if len(cfg.Paths)==0 { return b, errors.New("explicit SAN component paths are required") }
	if uint64(cfg.MetadataIndex)>uint64(^uint32(0)) { return b, errors.New("metadata index exceeds uint32") }
	if cfg.BootID=="" {
		raw,err:=os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err!=nil{return b,fmt.Errorf("read local boot ID: %w",err)}
		cfg.BootID=strings.TrimSpace(string(raw))
	}
	if cfg.BootID=="" { return b,errors.New("local BootID is empty") }
	if cfg.Timeout<=0 { cfg.Timeout=10*time.Second }

	san,err:=(sanqual.Collector{Runner:runner,Timeout:cfg.Timeout,ValidateDevice:cfg.ValidateDevice}).Collect(ctx,cfg.MultipathMap,cfg.VolumeID,cfg.NodeID,cfg.BootID,cfg.Paths)
	if err!=nil{return b,fmt.Errorf("collect per-path SCSI PR and SAN evidence: %w",err)}

	snapshotResult:=runProbe(ctx,runner,cfg.Timeout,"owner-metadata-snapshot",
		cfg.OwnerSnapshotBinary,"snapshot","--device",cfg.MultipathMap,
		"--index",fmt.Sprint(cfg.MetadataIndex),"--volume-id",cfg.VolumeID)
	if snapshotResult.ExitCode!=0 { return b,fmt.Errorf("read-only Owner metadata snapshot failed: %s",snapshotResult.Error) }
	var owner OwnerSnapshot
	if err:=json.Unmarshal([]byte(snapshotResult.Output),&owner);err!=nil{return b,fmt.Errorf("decode Owner metadata snapshot: %w",err)}
	if owner.Schema!="blockmesh-owner-crash-evidence/v1" || owner.VolumeID!=cfg.VolumeID ||
		owner.VolumeIndex!=uint32(cfg.MetadataIndex) || owner.PoolUUID=="" {
		return b,errors.New("metadata snapshot schema, Volume, index or Pool UUID does not match requested target")
	}

	host,_:=os.Hostname()
	kernel,_:=os.ReadFile("/proc/sys/kernel/osrelease")
	b=Bundle{
		SchemaVersion:SchemaVersion,CapturedAt:time.Now().UTC(),Hostname:host,
		Kernel:strings.TrimSpace(string(kernel)),VolumeID:cfg.VolumeID,NodeID:cfg.NodeID,
		BootID:cfg.BootID,MultipathMap:cfg.MultipathMap,DMDevice:cfg.DMDevice,
		SystemdUnit:cfg.SystemdUnit,SAN:san,OwnerSnapshot:json.RawMessage(snapshotResult.Output),
		Owner:owner,
	}
	probes:=[]struct{name,bin string;args []string}{
		{"systemd-unit","systemctl",[]string{"show","--no-pager","--property=Id,LoadState,ActiveState,SubState,MainPID,ExecMainStartTimestamp,NRestarts",cfg.SystemdUnit}},
		{"dm-info","dmsetup",[]string{"info","-c","--noheadings","-o","name,uuid,attr,open,segments,events",cfg.DMDevice}},
		{"dm-status","dmsetup",[]string{"status",cfg.DMDevice}},
		{"dm-table","dmsetup",[]string{"table",cfg.DMDevice}},
		{"sanlock-client-status","sanlock",[]string{"client","status"}},
		{"sanlock-status","sanlock",[]string{"status"}},
		{"multipath-map","multipath",[]string{"-ll",cfg.MultipathMap}},
		{"device-size","blockdev",[]string{"--getsize64",cfg.MultipathMap}},
	}
	for _,p:=range probes {
		result,digest:=runProbeWithDigest(ctx,runner,cfg.Timeout,p.bin,p.args...)
		b.Probes=append(b.Probes,Probe{Name:p.name,Result:result,OutputSHA256:digest})
	}
	return b,nil
}

func runProbe(ctx context.Context, runner CommandRunner, timeout time.Duration, name string, binary string, args ...string) sanqual.CommandResult {
	result,_:=runProbeWithDigest(ctx,runner,timeout,binary,args...)
	_ = name
	return result
}

func runProbeWithDigest(ctx context.Context, runner CommandRunner, timeout time.Duration, binary string, args ...string) (sanqual.CommandResult,string) {
	start:=time.Now()
	callCtx,cancel:=context.WithTimeout(ctx,timeout);defer cancel()
	out,err:=runner.Run(callCtx,binary,args...)
	r:=sanqual.CommandResult{Binary:binary,Args:append([]string(nil),args...),Output:strings.TrimSpace(string(out)),StartedAt:start.UTC(),Duration:time.Since(start)}
	if err!=nil {
		r.ExitCode=1;r.Error=err.Error()
		var ee *exec.ExitError
		if errors.As(err,&ee){r.ExitCode=ee.ExitCode()}
		if errors.Is(callCtx.Err(),context.DeadlineExceeded){r.Error="command timed out: "+err.Error()}
	}
	return r,sha256Hex(out)
}

func sha256Hex(data []byte) string { sum:=sha256.Sum256(data);return hex.EncodeToString(sum[:]) }

type PairResult struct {
	Passed bool `json:"passed"`
	Checks []string `json:"checks"`
	Failures []string `json:"failures,omitempty"`
	WWID string `json:"wwid,omitempty"`
	VolumeID string `json:"volume_id,omitempty"`
	PoolUUID string `json:"pool_uuid,omitempty"`
}

func VerifyPair(a,b Bundle) PairResult {
	r:=PairResult{}
	fail:=func(s string){r.Failures=append(r.Failures,s)}
	pass:=func(s string){r.Checks=append(r.Checks,s)}
	if a.SchemaVersion!=SchemaVersion || b.SchemaVersion!=SchemaVersion { fail("unsupported or missing node evidence schema") }
	if a.CapturedAt.IsZero() || b.CapturedAt.IsZero() { fail("capture timestamps are missing") }
	if a.SAN.VolumeID!=a.VolumeID || b.SAN.VolumeID!=b.VolumeID ||
		a.SAN.NodeID!=a.NodeID || b.SAN.NodeID!=b.NodeID ||
		a.SAN.BootID!=a.BootID || b.SAN.BootID!=b.BootID {
		fail("SAN collector identity does not match the enclosing node bundle")
	}
	if a.VolumeID=="" || a.VolumeID!=b.VolumeID || a.Owner.VolumeID!=a.VolumeID || b.Owner.VolumeID!=b.VolumeID { fail("Node A/B Volume identity mismatch") }
	if a.Owner.PoolUUID=="" || a.Owner.PoolUUID!=b.Owner.PoolUUID || a.Owner.VolumeIndex!=b.Owner.VolumeIndex { fail("Node A/B Pool UUID or Volume index mismatch") }
	if a.NodeID=="" || b.NodeID=="" || a.NodeID==b.NodeID || a.BootID=="" || b.BootID=="" { fail("two distinct NodeID and valid BootID identities are required") }
	if !a.SAN.PathCoverageVerified || !b.SAN.PathCoverageVerified || len(a.SAN.Paths)==0 || len(b.SAN.Paths)==0 { fail("multipath component path coverage is not verified on both nodes") }
	wwidA,okA:=singleWWID(a.SAN.Paths);wwidB,okB:=singleWWID(b.SAN.Paths)
	if !okA || !okB || wwidA!=wwidB { fail("SAN component paths on both nodes do not resolve to the same single WWID") } else { r.WWID=wwidA;pass("both nodes see the same SAN WWID and every declared component path is covered") }
	if a.Owner.SelectedOwner==nil || b.Owner.SelectedOwner==nil { fail("durable OwnerRecord is absent on one or both nodes") } else if *a.Owner.SelectedOwner!=*b.Owner.SelectedOwner { fail("Node A/B do not observe the same durable OwnerRecord identity and Epoch") } else if a.Owner.SelectedOwner.Epoch==0 || a.Owner.SelectedOwner.VolumeID!=a.VolumeID { fail("durable OwnerRecord has invalid Epoch or Volume identity") } else { pass("both nodes observe the same durable SAN OwnerRecord identity and Epoch") }
	if a.Owner.PoolUUID!="" && a.Owner.PoolUUID==b.Owner.PoolUUID && a.VolumeID==b.VolumeID && a.Owner.VolumeIndex==b.Owner.VolumeIndex { r.PoolUUID=a.Owner.PoolUUID;r.VolumeID=a.VolumeID;pass("both nodes read the same Pool UUID, Volume ID and Volume index") }
	required:=[]string{"systemd-unit","dm-info","dm-status","dm-table","sanlock-client-status","sanlock-status","multipath-map","device-size"}
	for _,node:=range []struct{name string;b Bundle}{{"Node A",a},{"Node B",b}} {
		seen:=map[string]bool{}
		for _,p:=range node.b.Probes {
			if p.Name=="" || seen[p.Name] { fail(node.name+" has an empty or duplicate probe name: "+p.Name);continue }
			seen[p.Name]=true
			if len(p.OutputSHA256)!=64 { fail(node.name+" probe "+p.Name+" has no valid output digest") } else if _,err:=hex.DecodeString(p.OutputSHA256);err!=nil { fail(node.name+" probe "+p.Name+" has an invalid output digest") }
			if p.Result.ExitCode!=0 { fail(fmt.Sprintf("%s probe %s failed: %s",node.name,p.Name,p.Result.Error)) }
		}
		for _,name:=range required { if !seen[name] { fail(node.name+" is missing required probe "+name) } }
	}
	if len(r.Failures)==0 { r.Passed=true;pass("all required read-only host probes completed successfully on both nodes") }
	return r
}

func singleWWID(paths []sanqual.PathEvidence)(string,bool) {
	if len(paths)==0{return "",false}
	value:=""
	for _,p:=range paths { if p.WWID=="" {return "",false};if value==""{value=p.WWID}else if value!=p.WWID{return "",false} }
	return value,true
}
