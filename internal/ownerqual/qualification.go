// Package ownerqual validates operator-collected evidence for a complete
// two-node Owner recovery qualification. It does not perform SAN mutations.
package ownerqual

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const SchemaVersion = "blockmesh-owner-recovery-evidence/v1"

type Artifact struct {
	Name string `json:"name"`
	Path string `json:"path"`
	SHA256 string `json:"sha256"`
}

type Evidence struct {
	SchemaVersion string `json:"schema_version"`
	CapturedAt time.Time `json:"captured_at"`
	VolumeID string `json:"volume_id"`
	NewOwnerNodeID string `json:"new_owner_node_id"`
	NewOwnerBootID string `json:"new_owner_boot_id"`
	OwnerEpoch uint64 `json:"owner_epoch"`
	Sanlock struct {
		Resource string `json:"resource"`
		NewOwnerHeld bool `json:"new_owner_held"`
		OldOwnerAbsent bool `json:"old_owner_absent"`
	} `json:"sanlock"`
	Fencing struct {
		PreemptAndAbort bool `json:"preempt_and_abort"`
		AllPathsVerified bool `json:"all_paths_verified"`
		PathCount uint32 `json:"path_count"`
		VerifiedPathCount uint32 `json:"verified_path_count"`
		OldOwnerWriteDenied bool `json:"old_owner_write_denied"`
	} `json:"fencing"`
	OldDM struct {
		QueueClosed bool `json:"queue_closed"`
		InflightIOs uint64 `json:"inflight_ios"`
		StaleEpochWriteRejected bool `json:"stale_epoch_write_rejected"`
	} `json:"old_dm"`
	WAL struct {
		RecoveryCompleted bool `json:"recovery_completed"`
		ReplayedTransactions uint64 `json:"replayed_transactions"`
		DescriptorPublished bool `json:"descriptor_published"`
		DescriptorEpoch uint64 `json:"descriptor_epoch"`
		RecoveryError string `json:"recovery_error,omitempty"`
	} `json:"wal"`
	NewDM struct {
		Prepared bool `json:"prepared"`
		Enabled bool `json:"enabled"`
		QueueEnabled bool `json:"queue_enabled"`
		OwnerEpoch uint64 `json:"owner_epoch"`
		InstanceID uint64 `json:"instance_id"`
	} `json:"new_dm"`
	Integrity struct {
		Method string `json:"method"`
		BaselineSHA256 string `json:"baseline_sha256"`
		RecoveredSHA256 string `json:"recovered_sha256"`
		SequenceBefore uint64 `json:"sequence_before"`
		SequenceAfter uint64 `json:"sequence_after"`
		Verified bool `json:"verified"`
	} `json:"integrity"`
	Artifacts []Artifact `json:"artifacts"`
}

type Result struct {
	Passed bool
	Checks []string
	Failures []string
	Artifacts []Artifact
}

var requiredArtifacts = []string{
	"san-pr-after", "old-node-sanlock", "new-node-sanlock",
	"old-node-dm", "new-node-dm", "wal-recovery", "data-integrity",
}

func Verify(e Evidence) (Result, error) {
	r := Result{}
	fail := func(s string) { r.Failures = append(r.Failures, s) }
	pass := func(s string) { r.Checks = append(r.Checks, s) }
	if e.SchemaVersion != SchemaVersion { return r, fmt.Errorf("unsupported evidence schema %q", e.SchemaVersion) }
	if e.CapturedAt.IsZero() || strings.TrimSpace(e.VolumeID)=="" || strings.TrimSpace(e.NewOwnerNodeID)=="" ||
		strings.TrimSpace(e.NewOwnerBootID)=="" || e.OwnerEpoch==0 {
		return r, errors.New("captured_at, volume_id, new owner identity and nonzero owner_epoch are required")
	}
	if e.Sanlock.Resource=="" || !e.Sanlock.NewOwnerHeld || !e.Sanlock.OldOwnerAbsent {
		fail("exclusive sanlock Owner resource is not proven on the new node and absent on the old node")
	} else { pass("new Owner holds the exclusive sanlock resource; old Owner no longer holds it") }
	if !e.Fencing.PreemptAndAbort || !e.Fencing.AllPathsVerified || e.Fencing.PathCount==0 ||
		e.Fencing.VerifiedPathCount!=e.Fencing.PathCount || !e.Fencing.OldOwnerWriteDenied {
		fail("SCSI PREEMPT AND ABORT, complete path coverage, or old-initiator write exclusion is not proven")
	} else { pass(fmt.Sprintf("SCSI PREEMPT AND ABORT and old-initiator write exclusion verified on all %d paths",e.Fencing.PathCount)) }
	if !e.OldDM.QueueClosed || e.OldDM.InflightIOs!=0 || !e.OldDM.StaleEpochWriteRejected {
		fail("old Owner DM queue closure, zero inflight I/O, or stale-Epoch write rejection is not proven")
	} else { pass("old Owner DM queue is closed, inflight I/O is zero, and stale-Epoch writes are rejected") }
	if !e.WAL.RecoveryCompleted || e.WAL.RecoveryError!="" || !e.WAL.DescriptorPublished ||
		e.WAL.DescriptorEpoch!=e.OwnerEpoch {
		fail("WAL recovery or durable descriptor publication at the new OwnerEpoch is not proven")
	} else { pass(fmt.Sprintf("WAL recovery completed (%d committed transactions replayed); descriptor Epoch %d is published",e.WAL.ReplayedTransactions,e.WAL.DescriptorEpoch)) }
	if !e.NewDM.Prepared || !e.NewDM.Enabled || !e.NewDM.QueueEnabled ||
		e.NewDM.OwnerEpoch!=e.OwnerEpoch || e.NewDM.InstanceID==0 {
		fail("new Owner DM target is not enabled with the expected Epoch and nonzero table instance")
	} else { pass(fmt.Sprintf("new Owner DM target is enabled at Epoch %d (instance %x)",e.NewDM.OwnerEpoch,e.NewDM.InstanceID)) }
	if !e.Integrity.Verified || strings.TrimSpace(e.Integrity.Method)=="" ||
		!validSHA256(e.Integrity.BaselineSHA256) || !validSHA256(e.Integrity.RecoveredSHA256) ||
		e.Integrity.BaselineSHA256!=e.Integrity.RecoveredSHA256 ||
		e.Integrity.SequenceAfter<e.Integrity.SequenceBefore {
		fail("application data checksum/sequence continuity is not proven")
	} else { pass(fmt.Sprintf("data integrity verified using %s; sequence %d -> %d",e.Integrity.Method,e.Integrity.SequenceBefore,e.Integrity.SequenceAfter)) }
	seen:=map[string]bool{}
	for _,a:=range e.Artifacts {
		if a.Name=="" || a.Path=="" || seen[a.Name] { fail("evidence artifacts contain an empty or duplicate name/path"); continue }
		seen[a.Name]=true
		if !validSHA256(a.SHA256) { fail("artifact "+a.Name+" has an invalid SHA-256"); continue }
		b,err:=os.ReadFile(a.Path)
		if err!=nil { fail("artifact "+a.Name+" cannot be read: "+err.Error()); continue }
		sum:=sha256.Sum256(b)
		if hex.EncodeToString(sum[:])!=strings.ToLower(a.SHA256) { fail("artifact "+a.Name+" SHA-256 does not match its file"); continue }
		r.Artifacts=append(r.Artifacts,a)
	}
	for _,name:=range requiredArtifacts { if !seen[name] { fail("required raw evidence artifact is missing: "+name) } }
	if len(r.Failures)==0 { r.Passed=true; pass("all required raw evidence artifacts exist and their SHA-256 digests match") }
	return r,nil
}

func validSHA256(s string) bool {
	if len(s)!=64 { return false }
	_,err:=hex.DecodeString(s)
	return err==nil
}

func Markdown(e Evidence, r Result) string {
	status:="FAIL / INCONCLUSIVE"
	if r.Passed { status="PASS" }
	var b strings.Builder
	fmt.Fprintf(&b,"# BlockMesh Owner Recovery Qualification\n\n- **Result:** %s\n- **Captured:** %s\n- **Volume:** %s\n- **New Owner:** %s / %s\n- **OwnerEpoch:** %d\n\n",status,e.CapturedAt.UTC().Format(time.RFC3339),e.VolumeID,e.NewOwnerNodeID,e.NewOwnerBootID,e.OwnerEpoch)
	b.WriteString("## Verified checks\n\n")
	for _,v:=range r.Checks { fmt.Fprintf(&b,"- [x] %s\n",v) }
	if len(r.Failures)>0 {
		b.WriteString("\n## Failed or inconclusive checks\n\n")
		for _,v:=range r.Failures { fmt.Fprintf(&b,"- [ ] %s\n",v) }
	}
	b.WriteString("\n## Evidence artifacts\n\n| Artifact | Path | SHA-256 |\n|---|---|---|\n")
	for _,a:=range r.Artifacts { fmt.Fprintf(&b,"| %s | `%s` | `%s` |\n",a.Name,a.Path,a.SHA256) }
	b.WriteString("\n> This report is an evidence validator, not a fencing mechanism. A PASS is valid only for the captured test iteration, Volume, WWID/path inventory, node identities and kernel/array configuration. Preserve raw logs and the separate pre/post metadata snapshots.\n")
	return b.String()
}
