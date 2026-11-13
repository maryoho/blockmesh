package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const lifecycleUsage = `BlockMesh CLI v1.0.0

Usage: bmctl [global-options] <group> <command> [args]

Global options (must appear before the command):
  -h, --help              Show this help
  --socket <path>         bm-metad control socket (default /run/blockmesh/control.sock)

Pool commands:
  pool create <name> <device> [--yes]
  pool list
  pool info <name>
  pool destroy <name>

Volume commands:
  volume create <pool> <name> <size>
  volume list
  volume info <uuid>
  volume delete <uuid>
  volume activate <uuid> <rw|ro>
  volume deactivate <uuid>

Size suffixes: K=KiB, M=MiB, G=GiB, T=TiB
`

type lifecycleRequest struct {
	Version int             `json:"version"`
	ID      string          `json:"id"`
	Action  string          `json:"action"`
	Args    json.RawMessage `json:"args"`
}
type lifecycleResponse struct {
	Version int             `json:"version"`
	ID      string          `json:"id"`
	OK      bool            `json:"ok"`
	Error   string          `json:"error,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}
type lifecycleArgs struct {
	Name string `json:"name,omitempty"`
	Device string `json:"device,omitempty"`
	Pool string `json:"pool,omitempty"`
	SizeBytes uint64 `json:"size_bytes,omitempty"`
	UUID string `json:"uuid,omitempty"`
	Mode string `json:"mode,omitempty"`
	CreateToken string `json:"create_token,omitempty"`
	Confirmed bool `json:"confirmed,omitempty"`
}

func parseVolumeSize(s string) (uint64, error) {
	if s == "" { return 0, errors.New("volume size is required") }
	unit := s[len(s)-1]
	multiplier := uint64(1)
	switch unit {
	case 'K','k': multiplier = 1 << 10
	case 'M','m': multiplier = 1 << 20
	case 'G','g': multiplier = 1 << 30
	case 'T','t': multiplier = 1 << 40
	default: return 0, fmt.Errorf("invalid size %q: use K, M, G or T suffix", s)
	}
	n, err := strconv.ParseUint(s[:len(s)-1], 10, 64)
	if err != nil || n == 0 { return 0, fmt.Errorf("invalid positive volume size %q", s) }
	if n > ^uint64(0)/multiplier { return 0, errors.New("volume size overflows uint64") }
	return n * multiplier, nil
}

func runCLI(argv []string) error {
	socket := "/run/blockmesh/control.sock"
	if len(argv) > 0 && (argv[0] == "-h" || argv[0] == "--help") {
		fmt.Fprint(os.Stdout, lifecycleUsage); return nil
	}
	if len(argv) > 1 && argv[0] == "--socket" {
		socket = argv[1]; argv = argv[2:]
	}
	if len(argv) == 0 { fmt.Fprint(os.Stdout, lifecycleUsage); return nil }
	// Keep the existing low-level recovery/diagnostic commands available for
	// developers. Normal Pool/Volume lifecycle operations use bm-metad RPC.
	if argv[0] == "pool" && len(argv) > 1 {
		switch argv[1] {
		case "format","data-init","dynamic-format","dynamic-status","dynamic-recover","rescan","metadata-expand","metadata-expand-status","data-expand":
			return runPool(argv[1:])
		}
	}
	action, args, err := parseLifecycleCommand(argv)
	if err != nil { return err }
	return callLifecycle(socket, action, args)
}

func parseLifecycleCommand(argv []string) (string, lifecycleArgs, error) {
	var a lifecycleArgs
	if len(argv) < 2 { return "", a, errors.New("expected a command; run bmctl --help") }
	group, command := argv[0], argv[1]
	rest := argv[2:]
	var err error
	switch group + " " + command {
	case "pool create":
		if len(rest) != 2 { return "", a, errors.New("usage: bmctl pool create <name> <device>") }
		a.Name, a.Device = rest[0], rest[1]
	case "pool list":
		if len(rest) != 0 { return "", a, errors.New("usage: bmctl pool list") }
	case "pool info", "pool destroy":
		if len(rest) != 1 { return "", a, fmt.Errorf("usage: bmctl %s %s <name>", group, command) }
		a.Name = rest[0]
	case "volume create":
		if len(rest) != 3 { return "", a, errors.New("usage: bmctl volume create <pool> <name> <size>") }
		a.Pool, a.Name = rest[0], rest[1]
		a.SizeBytes, err = parseVolumeSize(rest[2]); if err != nil { return "", a, err }
	case "volume list":
		if len(rest) != 0 { return "", a, errors.New("usage: bmctl volume list") }
	case "volume info", "volume delete", "volume deactivate":
		if len(rest) != 1 { return "", a, fmt.Errorf("usage: bmctl volume %s <uuid>", command) }
		a.UUID = rest[0]
	case "volume activate":
		if len(rest) != 2 { return "", a, errors.New("usage: bmctl volume activate <uuid> <rw|ro>") }
		if rest[1] != "rw" && rest[1] != "ro" { return "", a, errors.New("activation mode must be rw or ro") }
		a.UUID, a.Mode = rest[0], rest[1]
	default:
		return "", a, fmt.Errorf("unknown command %q; run bmctl --help", strings.Join(argv, " "))
	}
	return group+command, a, nil
}

func callLifecycle(socket, action string, args lifecycleArgs) error {
	if action == "volumecreate" && args.CreateToken == "" {
		// A deterministic token makes a repeated CLI invocation after a lost
		// response replay the original descriptor instead of allocating again.
		identity := fmt.Sprintf("blockmesh/volume-create/v1/%s/%s/%d", args.Pool, args.Name, args.SizeBytes)
		sum := sha256.Sum256([]byte(identity))
		args.CreateToken = hex.EncodeToString(sum[:16])
	}
	payload, err := json.Marshal(args); if err != nil { return err }
	id := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	req := lifecycleRequest{Version: 1, ID: id, Action: action, Args: payload}
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil { return fmt.Errorf("connect bm-metad at %s: %w (is bm-metad running?)", socket, err) }
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2*time.Minute))
	if err = json.NewEncoder(conn).Encode(req); err != nil { return fmt.Errorf("send lifecycle request: %w", err) }
	var resp lifecycleResponse
	if err = json.NewDecoder(io.LimitReader(conn, 16<<20)).Decode(&resp); err != nil { return fmt.Errorf("read bm-metad response: %w", err) }
	if resp.Version != 1 || resp.ID != id { return errors.New("bm-metad returned a mismatched protocol version or request ID") }
	if !resp.OK { if resp.Error == "" { resp.Error = "unspecified daemon error" }; return errors.New(resp.Error) }
	if len(resp.Result) != 0 && !bytes.Equal(resp.Result, []byte("null")) {
		var pretty bytes.Buffer
		if json.Indent(&pretty, resp.Result, "", "  ") == nil { fmt.Fprintln(os.Stdout, pretty.String()); return nil }
	}
	fmt.Fprintln(os.Stdout, "OK")
	return nil
}

