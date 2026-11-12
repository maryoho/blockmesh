#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 14 ]]; then
  echo "usage: $0 <node-a-ssh> <node-b-ssh> <remote-agent-absolute-path> <remote-snapshot-absolute-path> <volume-id> <node-a-id> <node-b-id> <multipath-map> <component-paths-csv> <dm-device> <systemd-unit> <metadata-index> <local-agent> <evidence-dir>" >&2
  exit 2
fi
host_a=$1; host_b=$2; remote_agent=$3; remote_snapshot=$4; volume=$5
node_a=$6; node_b=$7; mpath=$8; paths=$9; dm=${10}; unit=${11}
index=${12}; local_agent=${13}; outdir=${14}

safe() { [[ $1 =~ $2 ]]; }
safe "$host_a" '^[A-Za-z0-9_.@:-]+$' || { echo "unsafe Node A SSH destination" >&2; exit 2; }
safe "$host_b" '^[A-Za-z0-9_.@:-]+$' || { echo "unsafe Node B SSH destination" >&2; exit 2; }
safe "$remote_agent" '^/[A-Za-z0-9_./-]+$' || { echo "remote agent path must be absolute and shell-safe" >&2; exit 2; }
safe "$remote_snapshot" '^/[A-Za-z0-9_./-]+$' || { echo "remote snapshot binary path must be absolute and shell-safe" >&2; exit 2; }
safe "$volume" '^[A-Za-z0-9_.:-]+$' || { echo "unsafe Volume ID" >&2; exit 2; }
safe "$node_a" '^[A-Za-z0-9_.:-]+$' || { echo "unsafe Node A ID" >&2; exit 2; }
safe "$node_b" '^[A-Za-z0-9_.:-]+$' || { echo "unsafe Node B ID" >&2; exit 2; }
safe "$mpath" '^/dev/[A-Za-z0-9_./-]+$' || { echo "unsafe multipath map path" >&2; exit 2; }
safe "$paths" '^/dev/[A-Za-z0-9_./-]+(,/dev/[A-Za-z0-9_./-]+)*$' || { echo "component paths must be comma-separated /dev paths" >&2; exit 2; }
safe "$dm" '^[A-Za-z0-9_.+-]+$' || { echo "unsafe DM device name" >&2; exit 2; }
safe "$unit" '^[A-Za-z0-9_.@:-]+$' || { echo "unsafe systemd unit" >&2; exit 2; }
safe "$index" '^[0-9]+$' || { echo "metadata index must be numeric" >&2; exit 2; }
[[ -x "$local_agent" ]] || { echo "local agent is not executable: $local_agent" >&2; exit 2; }
command -v ssh >/dev/null
command -v scp >/dev/null
mkdir -p "$outdir"
local_a="$outdir/node-a.json"
local_b="$outdir/node-b.json"
[[ ! -e "$local_a" && ! -e "$local_b" ]] || { echo "refusing to overwrite existing evidence bundles in $outdir" >&2; exit 1; }

capture_node() {
  local host=$1 node=$2 local_file=$3 remote_file="/var/tmp/blockmesh-owner-san-${volume}-${node}-$$.json"
  local cmd="$remote_agent capture --volume-id $volume --node-id $node --multipath-map $mpath --paths $paths --dm-device $dm --systemd-unit $unit --metadata-index $index --owner-snapshot-bin $remote_snapshot --output $remote_file"
  echo "Capturing read-only SAN evidence on $node ($host)"
  ssh "$host" "$cmd"
  scp -- "$host:$remote_file" "$local_file"
  chmod 0600 "$local_file"
  echo "Evidence copied to $local_file (remote copy retained at $remote_file)"
}

capture_node "$host_a" "$node_a" "$local_a"
capture_node "$host_b" "$node_b" "$local_b"
"$local_agent" verify-pair --node-a "$local_a" --node-b "$local_b"
echo "TWO-NODE SAN BASELINE CAPTURE PASS: $outdir"
