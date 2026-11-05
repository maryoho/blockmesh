package ownerqual

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testEvidence(t *testing.T) Evidence {
	t.Helper()
	dir:=t.TempDir()
	names:=append([]string(nil),requiredArtifacts...)
	artifacts:=make([]Artifact,0,len(names))
	for _,name:=range names {
		path:=filepath.Join(dir,name+".log")
		data:=[]byte("captured "+name+" evidence\n")
		if err:=os.WriteFile(path,data,0600);err!=nil{t.Fatal(err)}
		sum:=sha256.Sum256(data)
		artifacts=append(artifacts,Artifact{Name:name,Path:path,SHA256:hex.EncodeToString(sum[:])})
	}
	e:=Evidence{SchemaVersion:SchemaVersion,CapturedAt:time.Now().UTC(),VolumeID:"vol-a",NewOwnerNodeID:"node-b",NewOwnerBootID:"boot-b",OwnerEpoch:42,Artifacts:artifacts}
	e.Sanlock.Resource="pool:owner:vol-a";e.Sanlock.NewOwnerHeld=true;e.Sanlock.OldOwnerAbsent=true
	e.Fencing.PreemptAndAbort=true;e.Fencing.AllPathsVerified=true;e.Fencing.PathCount=2;e.Fencing.VerifiedPathCount=2;e.Fencing.OldOwnerWriteDenied=true
	e.OldDM.QueueClosed=true;e.OldDM.InflightIOs=0;e.OldDM.StaleEpochWriteRejected=true
	e.WAL.RecoveryCompleted=true;e.WAL.ReplayedTransactions=3;e.WAL.DescriptorPublished=true;e.WAL.DescriptorEpoch=42
	e.NewDM.Prepared=true;e.NewDM.Enabled=true;e.NewDM.QueueEnabled=true;e.NewDM.OwnerEpoch=42;e.NewDM.InstanceID=0x1234
	e.Integrity.Method="pattern+sha256";e.Integrity.BaselineSHA256=hex.EncodeToString(make([]byte,32));e.Integrity.RecoveredSHA256=e.Integrity.BaselineSHA256;e.Integrity.SequenceBefore=100;e.Integrity.SequenceAfter=100;e.Integrity.Verified=true
	return e
}

func TestVerifyFullRecoveryEvidencePasses(t *testing.T) {
	e:=testEvidence(t)
	r,err:=Verify(e)
	if err!=nil || !r.Passed {t.Fatalf("Verify()=%+v err=%v",r,err)}
	if len(r.Checks)<7 {t.Fatalf("missing checks: %+v",r.Checks)}
}

func TestVerifyFailsClosedOnIncompleteRecovery(t *testing.T) {
	e:=testEvidence(t)
	e.OldDM.InflightIOs=1
	e.WAL.DescriptorEpoch=41
	e.NewDM.OwnerEpoch=41
	e.Integrity.RecoveredSHA256="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	r,err:=Verify(e)
	if err!=nil {t.Fatal(err)}
	if r.Passed || len(r.Failures)<4 {t.Fatalf("incomplete recovery accepted: %+v",r)}
}

func TestVerifyRejectsArtifactDigestMismatch(t *testing.T) {
	e:=testEvidence(t)
	e.Artifacts[0].SHA256="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	r,err:=Verify(e)
	if err!=nil {t.Fatal(err)}
	if r.Passed {t.Fatal("tampered artifact accepted")}
}

func TestVerifyRequiresCompletePathCoverage(t *testing.T) {
	e:=testEvidence(t)
	e.Fencing.VerifiedPathCount=1
	r,err:=Verify(e)
	if err!=nil {t.Fatal(err)}
	if r.Passed {t.Fatal("partial SAN path coverage accepted")}
}

func TestVerifyRejectsEpochMismatch(t *testing.T) {
	e:=testEvidence(t)
	e.NewDM.OwnerEpoch++
	r,err:=Verify(e)
	if err!=nil {t.Fatal(err)}
	if r.Passed {t.Fatal("DM Epoch mismatch accepted")}
}
