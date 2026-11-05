package fence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var ErrOwnerReservationMismatch = errors.New("SCSI PR Owner reservation does not match the expected holder and type")

// OwnerReservation establishes the new Owner's reservation and verifies it
// through every configured multipath component path before RW admission.
type OwnerReservation interface {
	EstablishOwnerReservation(context.Context, string, string) error
}

// SCSIPROwnerReservation registers the local path-scoped keys, issues RESERVE
// through the first path, then verifies the same reservation through every
// path. It intentionally does not unregister keys after an ambiguous RESERVE
// outcome: a command error may mean the target committed the operation.
type SCSIPROwnerReservation struct {
	Binary string
	Paths []PRPath
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

func (r *SCSIPROwnerReservation) EstablishOwnerReservation(ctx context.Context, nodeID, bootID string) error {
	if r == nil { return errors.New("SCSI PR Owner reservation manager is required") }
	if ctx == nil { return errors.New("context is required") }
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" {
		return errors.New("Owner node and boot identities are required")
	}
	if err := validatePRPaths(r.Paths); err != nil { return err }
	prType := r.ReservationType
	if prType == 0 { prType = 6 }
	// All Registrants types do not have one unique reservation-holder key in
	// PRIN READ RESERVATION. This verifier requires a singular holder key.
	if prType != 1 && prType != 3 && prType != 5 && prType != 6 {
		return fmt.Errorf("Owner reservation type %d cannot be verified with a singular holder key; supported types are 1, 3, 5 and 6", prType)
	}
	binary := r.Binary
	if binary == "" { binary = "sg_persist" }
	runner := r.Runner
	if runner == nil { runner = execRunner{} }
	timeout := r.Timeout
	if timeout <= 0 { timeout = 30 * time.Second }
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	registrar := &SCSIPRRegistrar{Binary: binary, Paths: r.Paths, Timeout: timeout, Runner: runner}
	if err := registrar.RegisterReader(ctx, nodeID, bootID); err != nil {
		return fmt.Errorf("register new Owner PR keys on all paths: %w", err)
	}
	holder := r.Paths[0].LocalKey
	output, reserveErr := runner.Run(ctx, binary, "--out", "--reserve",
		fmt.Sprintf("--param-rk=%x", holder), fmt.Sprintf("--prout-type=%d", prType),
		"--device="+r.Paths[0].Device)

	// Always verify after RESERVE, including when the command reports an
	// error. The target may have committed the reservation before a response
	// was lost. A fully matching all-path observation is authoritative.
	verifyErr := verifyLiveReservation(ctx, runner, binary, r.Paths, holder, prType)
	if verifyErr == nil { return nil }
	if reserveErr != nil {
		return errors.Join(fmt.Errorf("SCSI PR RESERVE through %s failed: %w: %s", r.Paths[0].Device, reserveErr, strings.TrimSpace(string(output))),
			fmt.Errorf("all-path reservation verification after ambiguous RESERVE: %w", verifyErr))
	}
	return fmt.Errorf("SCSI PR RESERVE completed but all-path verification failed: %w", verifyErr)
}

func verifyLiveReservation(ctx context.Context, runner CommandRunner, binary string, paths []PRPath, holder uint64, prType uint8) error {
	for _, path := range paths {
		output, err := runner.Run(ctx, binary, "--in", "--read-reservation", "--device="+path.Device)
		if err != nil {
			return fmt.Errorf("read reservation on %s: %w: %s", path.Device, err, strings.TrimSpace(string(output)))
		}
		gotKey, gotType, parsed := parseLiveReservation(string(output))
		if !parsed {
			return fmt.Errorf("reservation response on %s is absent, ambiguous or unsupported: %s", path.Device, strings.TrimSpace(string(output)))
		}
		if gotKey != holder || gotType != prType {
			return fmt.Errorf("%w on %s: holder=%x type=%d, want holder=%x type=%d",
				ErrOwnerReservationMismatch, path.Device, gotKey, gotType, holder, prType)
		}
	}
	return nil
}

func parseLiveReservation(output string) (uint64, uint8, bool) {
	lower := strings.ToLower(output)
	if strings.Contains(lower, "no reservation held") || strings.Contains(lower, "no reservation") ||
		strings.Contains(lower, "reservation not held") { return 0, 0, false }

	var key uint64
	lowerOutput := strings.ToLower(output)
	if strings.Count(lowerOutput, "key") != 1 { return 0, 0, false }
	for _, line := range strings.Split(output, "\n") {
		l := strings.ToLower(line)
		idx := strings.Index(l, "key")
		if idx < 0 { continue }
		rest := strings.TrimSpace(line[idx+len("key"):])
		rest = strings.TrimLeft(rest, " :=\t")
		fields := strings.Fields(rest)
		if len(fields) == 0 { return 0, 0, false }
		value := strings.Trim(fields[0], ",;")
		if !strings.HasPrefix(strings.ToLower(value), "0x") { return 0, 0, false }
		n, err := strconv.ParseUint(value[2:], 16, 64)
		if err != nil { return 0, 0, false }
		key = n
	}
	if key == 0 { return 0, 0, false }

	var prType uint8
	typeCount := 0
	for _, line := range strings.Split(output, "\n") {
		l := strings.ToLower(line)
		idx := strings.Index(l, "type")
		if idx < 0 { continue }
		value := strings.TrimSpace(line[idx+len("type"):])
		value = strings.TrimLeft(value, " :=\t")
		value = strings.TrimSpace(strings.TrimRight(value, ",;"))
		switch strings.ToLower(value) {
		case "write exclusive": prType = 1
		case "exclusive access": prType = 3
		case "write exclusive, registrants only": prType = 5
		case "exclusive access, registrants only": prType = 6
		default:
			fields := strings.Fields(value)
			if len(fields) > 0 {
				numeric := strings.Trim(fields[0], ",;")
				if strings.HasPrefix(strings.ToLower(numeric), "0x") {
					n, err := strconv.ParseUint(numeric[2:], 16, 8)
					if err == nil { prType = uint8(n) }
				} else {
					n, err := strconv.ParseUint(numeric, 10, 8)
					if err == nil { prType = uint8(n) }
				}
			}
		}
		if prType != 0 { typeCount++ }
	}
	if typeCount != 1 || (prType != 1 && prType != 3 && prType != 5 && prType != 6) { return 0, 0, false }
	return key, prType, true
}

var _ OwnerReservation = (*SCSIPROwnerReservation)(nil)
