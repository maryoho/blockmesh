#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 12 ]]; then
  echo "usage: $0 <bm-owner-crash-qualify> <bm-san-qualify> <bm-owner-recovery-report> <before-prefix> <after-prefix> <old-pr-key> <new-pr-key> <pr-type> <new-node-id> <new-boot-id> <runtime-evidence.json> <report.md>" >&2
  exit 2
fi
meta_bin=$1
san_bin=$2
report_bin=$3
before=$4
after=$5
old_key=$6
new_key=$7
pr_type=$8
node=$9
boot=${10}
runtime_evidence=${11}
report=${12}

for bin in "$meta_bin" "$san_bin" "$report_bin"; do
  [[ -x "$bin" ]] || { echo "qualification binary is not executable: $bin" >&2; exit 2; }
done
[[ -s "$runtime_evidence" ]] || { echo "runtime evidence JSON is missing or empty" >&2; exit 2; }
[[ ! -e "$report" ]] || { echo "refusing to overwrite report: $report" >&2; exit 1; }

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
"$script_dir/owner-crash-qualification.sh" verify \
  "$meta_bin" "$san_bin" "$before" "$after" \
  "$old_key" "$new_key" "$pr_type" "$node" "$boot"

mkdir -p "$(dirname -- "$report")"
"$report_bin" --evidence "$runtime_evidence" \
  --after-owner-snapshot "$after.owner.json" --output "$report"
echo "OWNER RECOVERY ACCEPTANCE PASS: $report"
