package owneragent

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blockmesh/internal/sanqual"
)

type fakeRunner struct{}
func (fakeRunner) Run(_ context.Context, binary string, args ...string) ([]byte,error) {
	joined:=strings.Join(args," ")
	switch binary {
	case "multipath":
		return []byte("mpatha (3600508b400105e210000900000490000) dm-0 VENDOR,MODEL\n|- 1:0:0:1 sda 8:0 active ready running\n"),nil
	case "udevadm":
		return []byte("ID_WWN=3600508b400105e210000900000490000\n"),nil
	case "sg_persist":
		if strings.Contains(joined,"read-keys") { return []byte("PR generation=0x1\n  0x1234\n"),nil }
		return []byte("key = 0x1234\n scope = LU_SCOPE, type = 0x6\n"),nil
	case "bm-owner-crash-qualify":
		return []byte(`{"schema":"blockmesh-owner-crash-evidence/v1","volume_id":"vol-a","volume_index":3,"pool_uuid":"pool-a","selected_owner":{"volume_id":"vol-a","node_id":"node-a","boot_id":"boot-a","epoch":8}}`),nil
	default:
		return []byte("read-only probe ok\n"),nil
	}
}

func TestCaptureBuildsNodeEvidenceBundle(t *testing.T) {
	cfg:=Config{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",MultipathMap:"/dev/mapper/mpatha",
		Paths:[]string{"/dev/sda"},DMDevice:"bm-vol-a",SystemdUnit:"blockmesh.service",
		MetadataIndex:3,OwnerSnapshotBinary:"bm-owner-crash-qualify",Timeout:time.Second,
		ValidateDevice:func(string)error{return nil}}
	b,err:=Capture(context.Background(),cfg,fakeRunner{})
	if err!=nil {t.Fatal(err)}
	if b.SchemaVersion!=SchemaVersion || b.Owner.PoolUUID!="pool-a" || b.SAN.VolumeID!="vol-a" {
		t.Fatalf("unexpected bundle: %+v",b)
	}
	if !b.SAN.PathCoverageVerified || len(b.Probes)!=8 {t.Fatalf("incomplete capture: %+v",b)}
	for _,p:=range b.Probes { if p.OutputSHA256=="" {t.Fatalf("probe %s missing digest",p.Name)} }
}

func TestCaptureRejectsMissingComponentPathInventory(t *testing.T) {
	_,err:=Capture(context.Background(),Config{VolumeID:"v",NodeID:"n"},fakeRunner{})
	if err==nil {t.Fatal("missing inventory accepted")}
}

func validBundle(node,boot string) Bundle {
	owner:=OwnerSnapshot{VolumeID:"vol-a",VolumeIndex:3,PoolUUID:"pool-a",
		SelectedOwner:&struct{VolumeID string `json:"volume_id"`;NodeID string `json:"node_id"`;BootID string `json:"boot_id"`;Epoch uint64 `json:"epoch"`}{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:8}}
	probes:=make([]Probe,8)
	for i,name:=range []string{"systemd-unit","dm-info","dm-status","dm-table","sanlock-client-status","sanlock-status","multipath-map","device-size"} {
		probes[i]=Probe{Name:name,Result:sanqual.CommandResult{ExitCode:0},OutputSHA256:strings.Repeat("a",64)}
	}
	return Bundle{SchemaVersion:SchemaVersion,CapturedAt:time.Now().UTC(),VolumeID:"vol-a",NodeID:node,BootID:boot,Owner:owner,
		SAN:sanqual.Evidence{VolumeID:"vol-a",NodeID:node,BootID:boot,PathCoverageVerified:true,Paths:[]sanqual.PathEvidence{{Device:"/dev/sda",WWID:"3600508b400105e210000900000490000"}}},
		Probes:probes}
}

func TestVerifyPairRequiresSameWWIDAndOwnerRecord(t *testing.T) {
	a:=validBundle("node-a","boot-a")
	b:=validBundle("node-b","boot-b")
	r:=VerifyPair(a,b)
	if !r.Passed {t.Fatalf("valid pair rejected: %+v",r)}
	b.SAN.Paths[0].WWID="different"
	r=VerifyPair(a,b)
	if r.Passed {t.Fatal("different WWID accepted")}
}

func TestVerifyPairFailsClosedOnProbeError(t *testing.T) {
	a:=validBundle("node-a","boot-a")
	b:=validBundle("node-b","boot-b")
	b.Probes[0].Result.Error=errors.New("probe unavailable").Error()
	b.Probes[0].Result.ExitCode=1
	r:=VerifyPair(a,b)
	if r.Passed {t.Fatal("probe failure accepted")}
}

func TestTwoNodeCaptureScriptSyntax(t *testing.T) {
	script:=filepath.Join("..","..","scripts","owner-san-two-node-capture.sh")
	if out,err:=exec.Command("bash","-n",script).CombinedOutput();err!=nil {
		t.Fatalf("bash -n %s: %v\\n%s",script,err,out)
	}
}
