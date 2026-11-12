#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  dynamic-owner-crash-qualification.sh capture <bm-owner-crash-qualify> <bm-san-qualify> <device> <paths-csv> <volume-id> <index> <node-id> <boot-id> <prefix>
  dynamic-owner-crash-qualification.sh verify <bm-owner-crash-qualify> <bm-san-qualify> <before-prefix> <after-prefix> <old-pr-key> <new-pr-key> <pr-type> <new-node-id> <new-boot-id>

Capture writes:
  PREFIX.owner.json — read-only BMPOOL03 BMDVOL03 descriptor pair and SAN OwnerRecord
  PREFIX.san.json   — multipath and all-component-path SCSI PR evidence
  PREFIX.sha256     — checksums for both evidence files

Verify requires:
  - same Pool UUID, Volume ID and Volume index
  - monotonic Bootstrap View and BMPOOL03 Superblock generations
  - new SAN OwnerRecord identity and strictly increasing OwnerEpoch
  - highest-generation BMDVOL03 descriptor OwnerEpoch equals SAN OwnerRecord
  - old Owner PR key absent on every listed component path
  - new Owner reservation holder/type matches on every listed path

This script verifies evidence; it does not power off or fence a node.
A real crash test must use an independently controlled PDU/STONITH/hypervisor
power action and retain its audit evidence.
EOF
  exit 2
}

[[ $# -gt 0 ]] || usage
mode=$1; shift
case "$mode" in
capture)
  [[ $# -eq 9 ]] || usage
  meta_bin=$1; san_bin=$2; device=$3; paths=$4; volume=$5; index=$6
  node=$7; boot=$8; prefix=$9
  [[ -x "$meta_bin" && -x "$san_bin" ]] || { echo "qualification binaries must be executable" >&2; exit 2; }
  [[ -b "$device" ]] || { echo "parent metadata device must be a block device: $device" >&2; exit 2; }
  [[ "$index" =~ ^[0-9]+$ ]] || { echo "Volume index must be numeric" >&2; exit 2; }
  [[ "$paths" =~ ^/dev/[A-Za-z0-9_./-]+(,/dev/[A-Za-z0-9_./-]+)*$ ]] || { echo "component paths must be comma-separated /dev paths" >&2; exit 2; }
  [[ "$volume" =~ ^[A-Za-z0-9_.:-]+$ && "$node" =~ ^[A-Za-z0-9_.:-]+$ && "$boot" =~ ^[A-Za-z0-9_.:-]+$ ]] || { echo "unsafe Volume/Node/Boot identity" >&2; exit 2; }
  mkdir -p "$(dirname -- "$prefix")"
  [[ ! -e "$prefix.owner.json" && ! -e "$prefix.san.json" && ! -e "$prefix.sha256" ]] || { echo "refusing to overwrite existing evidence" >&2; exit 1; }
  "$meta_bin" snapshot-dynamic --device "$device" --index "$index" --volume-id "$volume" --output "$prefix.owner.json"
  "$san_bin" collect --device "$device" --paths "$paths" --volume-id "$volume" --node-id "$node" --boot-id "$boot" --output "$prefix.san.json"
  (set -o noclobber; sha256sum "$prefix.owner.json" "$prefix.san.json" > "$prefix.sha256")
  chmod 0600 "$prefix.owner.json" "$prefix.san.json" "$prefix.sha256"
  echo "CAPTURED BMPOOL03 EVIDENCE: $prefix"
  ;;
verify)
  [[ $# -eq 9 ]] || usage
  meta_bin=$1; san_bin=$2; before=$3; after=$4; old_key=$5; new_key=$6
  pr_type=$7; node=$8; boot=$9
  [[ -x "$meta_bin" && -x "$san_bin" ]] || { echo "qualification binaries must be executable" >&2; exit 2; }
  for file in "$before.owner.json" "$before.san.json" "$after.owner.json" "$after.san.json"; do
    [[ -s "$file" ]] || { echo "missing evidence file: $file" >&2; exit 2; }
  done
  echo "[1/3] BMPOOL03 descriptor and SAN OwnerRecord Epoch"
  "$meta_bin" verify-dynamic --before "$before.owner.json" --after "$after.owner.json" --node-id "$node" --boot-id "$boot"
  echo "[2/3] previous Owner PR key absent on every component path"
  "$san_bin" verify-fenced --evidence "$after.san.json" --target-key "$old_key"
  echo "[3/3] new Owner reservation holder/type verified on every component path"
  "$san_bin" verify-reservation --evidence "$after.san.json" --holder-key "$new_key" --type "$pr_type"
  echo "BMPOOL03 OWNER EPOCH + ALL-PATH SCSI PR EVIDENCE PASS"
  echo "Separate PDU/STONITH audit, DM inflight drain, data checksum and array power-loss evidence are still mandatory."
  ;;
*) usage ;;
esac
