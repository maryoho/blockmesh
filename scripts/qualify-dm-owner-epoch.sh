#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/dm-blockmesh.ko" >&2
  exit 2
fi
module=$(readlink -f "$1")
[[ -f "$module" ]] || { echo "module not found: $module" >&2; exit 2; }
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 2; }
for cmd in dmsetup losetup truncate dd grep awk; do
  command -v "$cmd" >/dev/null || { echo "missing command: $cmd" >&2; exit 2; }
done

loaded_by_script=0
if ! dmsetup targets | grep -q '^blockmesh '; then
  insmod "$module"
  loaded_by_script=1
fi

tmp=$(mktemp)
name="bm-owner-epoch-$$"
loop=""
cleanup() {
  set +e
  dmsetup remove "$name" >/dev/null 2>&1
  [[ -n "$loop" ]] && losetup -d "$loop" >/dev/null 2>&1
  rm -f "$tmp"
  if [[ $loaded_by_script -eq 1 ]]; then rmmod dm-blockmesh >/dev/null 2>&1; fi
}
trap cleanup EXIT

truncate -s 64M "$tmp"
loop=$(losetup --find --show "$tmp")
dmsetup create "$name" --table "0 131072 blockmesh $loop 0"
dev="/dev/mapper/$name"

expect_write_rejected() {
  if dd if=/dev/zero of="$dev" bs=4096 count=1 conv=notrunc status=none 2>/dev/null; then
    echo "ERROR: write unexpectedly succeeded (expected closed/fenced gate)" >&2
    exit 1
  fi
}
target_instance() {
  local status
  status=$(dmsetup status "$name")
  awk '{for (i=1;i<=NF;i++) if ($i ~ /^instance=/) {sub(/^instance=/,"",$i); print $i; exit}}' <<<"$status"
}
expect_message_rejected() {
  local message=$1
  if dmsetup message "$name" 0 "$message" >/dev/null 2>&1; then
    echo "ERROR: stale/invalid DM message unexpectedly succeeded: $message" >&2
    exit 1
  fi
}

echo "[1/7] initial target must reject writes"
expect_write_rejected
dd if="$dev" of=/dev/null bs=4096 count=1 status=none

echo "[2/7] prepare/enable current epoch"
instance=$(target_instance)
[[ -n "$instance" ]] || { echo "missing target instance ID" >&2; exit 1; }
dmsetup message "$name" 0 "owner_prepare 41 $instance"
dmsetup message "$name" 0 "owner_enable 41 $instance"
dd if=/dev/zero of="$dev" bs=4096 count=1 conv=notrunc status=none

echo "[3/7] fence drains and closes current epoch"
dmsetup message "$name" 0 "owner_fence 41 $instance"
expect_write_rejected
expect_message_rejected "owner_enable 41 $instance"

echo "[4/7] stale prepare/fence/enable cannot alter newer epoch"
dmsetup message "$name" 0 "owner_prepare 42 $instance"
dmsetup message "$name" 0 "owner_enable 42 $instance"
expect_message_rejected "owner_fence 41 $instance"
expect_message_rejected "owner_enable 41 $instance"
expect_message_rejected "owner_prepare 41 $instance"
dd if=/dev/zero of="$dev" bs=4096 count=1 conv=notrunc status=none
status=$(dmsetup status "$name")
echo "$status" | grep -q 'epoch=42 gate=1 fenced=0 inflight=0'

echo "[5/7] table reload must start closed and forget prior epoch"
dmsetup suspend "$name"
dmsetup reload "$name" --table "0 131072 blockmesh $loop 0"
dmsetup resume "$name"
expect_write_rejected
expect_message_rejected "owner_enable 42 $instance"
old_instance=$instance
instance=$(target_instance)
[[ -n "$instance" && "$instance" != "$old_instance" ]] || { echo "table reload did not rotate target instance ID" >&2; exit 1; }
expect_message_rejected "owner_enable 42 $old_instance"
expect_message_rejected "owner_fence 42 $old_instance"
expect_message_rejected "owner_prepare 43 $old_instance"
dmsetup message "$name" 0 "owner_prepare 43 $instance"
dmsetup message "$name" 0 "owner_enable 43 $instance"

echo "[6/7] fence while writes are being submitted"
if command -v fio >/dev/null; then
  fio --name=bm-fence-race --filename="$dev" --rw=write --bs=4k --iodepth=32 \
    --ioengine=libaio --direct=1 --time_based=1 --runtime=2 --group_reporting \
    >/tmp/bm-fence-race-$$.log 2>&1 &
  fio_pid=$!
  sleep 0.2
  dmsetup message "$name" 0 "owner_fence 43 $instance"
  wait "$fio_pid" || true
  rm -f /tmp/bm-fence-race-$$.log
else
  echo "fio not installed; skipped concurrent fio stress (install fio to enable)"
  dmsetup message "$name" 0 "owner_fence 43"
fi
status=$(dmsetup status "$name")
echo "$status" | grep -q 'gate=0 fenced=1 inflight=0'

echo "[7/7] a strictly newer epoch is required to reopen"
expect_write_rejected
expect_message_rejected "owner_enable 43 $instance"
dmsetup message "$name" 0 "owner_prepare 44 $instance"
dmsetup message "$name" 0 "owner_enable 44 $instance"
dd if=/dev/zero of="$dev" bs=4096 count=1 conv=notrunc status=none
status=$(dmsetup status "$name")
echo "$status" | grep -q 'epoch=44 gate=1 fenced=0 inflight=0'

echo "PASS: DM OwnerEpoch admission, fencing, reload and drain checks"
