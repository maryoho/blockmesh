#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <systemd-unit> <test-volume-id> I_UNDERSTAND_THIS_KILLS_BLOCKMESH_ON_A_TEST_NODE" >&2
  exit 2
fi
unit=$1
volume=$2
confirm=$3
[[ "$confirm" == "I_UNDERSTAND_THIS_KILLS_BLOCKMESH_ON_A_TEST_NODE" ]] || { echo "explicit crash confirmation missing" >&2; exit 2; }
[[ -n "$volume" && "${BLOCKMESH_TEST_LUN:-}" == "$volume" ]] || {
  echo "set BLOCKMESH_TEST_LUN to the exact disposable test Volume ID ($volume)" >&2
  exit 2
}
command -v systemctl >/dev/null
exec_start=$(systemctl show "$unit" --property=ExecStart --value)
pid=$(systemctl show "$unit" --property=MainPID --value)
[[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 ]] || { echo "unit has no valid MainPID: $pid" >&2; exit 1; }
[[ "$exec_start" == *blockmesh* ]] || { echo "refusing: ExecStart does not identify a BlockMesh service" >&2; exit 1; }
[[ -r "/proc/$pid/cmdline" && -e "/proc/$pid/exe" ]] || { echo "MainPID disappeared before injection" >&2; exit 1; }
cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline")
exe=$(readlink -f "/proc/$pid/exe")
[[ "$cmdline" == *blockmesh* || "$exe" == *blockmesh* ]] || {
  echo "refusing: MainPID executable/cmdline is not recognizably BlockMesh" >&2
  exit 1
}
echo "FAULT INJECTION: SIGKILL main process pid=$pid unit=$unit volume=$volume exe=$exe"
systemctl kill --signal=SIGKILL --kill-whom=main "$unit"
for _ in $(seq 1 50); do
  current=$(systemctl show "$unit" --property=MainPID --value || true)
  [[ "$current" == "0" || "$current" != "$pid" ]] && {
    echo "injected process crash confirmed; collect post-crash evidence before restarting the unit"
    exit 0
  }
  sleep 0.1
done
echo "SIGKILL was sent but MainPID did not transition; inspect unit manually" >&2
exit 1
