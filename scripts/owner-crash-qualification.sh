#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  owner-crash-qualification.sh capture <bm-owner-crash-qualify> <bm-san-qualify> <device> <paths-csv> <volume-id> <index> <node-id> <boot-id> <prefix>
  owner-crash-qualification.sh verify <bm-owner-crash-qualify> <bm-san-qualify> <before-prefix> <after-prefix> <old-pr-key> <new-pr-key> <pr-type> <new-node-id> <new-boot-id>

capture writes:
  PREFIX.owner.json  — read-only BMDVOL02 + both SAN OwnerRecord pages
  PREFIX.san.json    — multipath, sanlock, DM and per-path SCSI PR evidence

verify checks:
  - same Pool UUID / Volume identity
  - OwnerRecord identity and strictly monotonic Epoch
  - highest-generation valid BMDVOL02 descriptor Epoch matches OwnerRecord
  - old PR key absent on every listed path
  - new reservation holder/type matches on every listed path

All output files are created exclusively and are never overwritten.
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
  [[ -b "$device" ]] || { echo "metadata device must be a block device: $device" >&2; exit 2; }
  [[ -n "$paths" && -n "$volume" && -n "$node" && -n "$boot" ]] || usage
  [[ ! -e "$prefix.owner.json" && ! -e "$prefix.san.json" && ! -e "$prefix.sha256" ]] || { echo "refusing to overwrite existing evidence" >&2; exit 1; }
  "$meta_bin" snapshot --device "$device" --index "$index" --volume-id "$volume" --output "$prefix.owner.json"
  "$san_bin" collect --device "$device" --paths "$paths" --volume-id "$volume" --node-id "$node" --boot-id "$boot" --output "$prefix.san.json"
  (set -o noclobber; sha256sum "$prefix.owner.json" "$prefix.san.json" > "$prefix.sha256")
  chmod 0600 "$prefix.sha256"
  echo "CAPTURED: $prefix.owner.json $prefix.san.json"
  ;;
verify)
  [[ $# -eq 9 ]] || usage
  meta_bin=$1; san_bin=$2; before=$3; after=$4; old_key=$5; new_key=$6
  pr_type=$7; node=$8; boot=$9
  [[ -x "$meta_bin" && -x "$san_bin" ]] || { echo "qualification binaries must be executable" >&2; exit 2; }
  for file in "$before.owner.json" "$before.san.json" "$after.owner.json" "$after.san.json"; do
    [[ -s "$file" ]] || { echo "missing evidence file: $file" >&2; exit 2; }
  done
  echo "[1/3] durable OwnerRecord and descriptor Epoch"
  "$meta_bin" verify --before "$before.owner.json" --after "$after.owner.json" --node-id "$node" --boot-id "$boot"
  echo "[2/3] previous Owner PR registration absent on every path"
  "$san_bin" verify-fenced --evidence "$after.san.json" --target-key "$old_key"
  echo "[3/3] new Owner reservation holder/type verified on every path"
  "$san_bin" verify-reservation --evidence "$after.san.json" --holder-key "$new_key" --type "$pr_type"
  echo "PASS: metadata Epoch and all-path PR evidence checks passed"
  echo "NOTE: this does not prove old-host power isolation, sanlock exclusivity, DM inflight drain, data integrity or array power-loss durability."
  ;;
*) usage ;;
esac
