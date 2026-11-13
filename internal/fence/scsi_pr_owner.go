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
	// Prepare registers local path-scoped keys before previous-owner fencing.
	PrepareOwnerReservation(context.Context, string, string) error
	// Establish issues RESERVE and verifies holder/type on every component path.
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

// PrepareOwnerReservation registers the new Owner's path-scoped keys before
// the previous Owner is fenced. PREEMPT AND ABORT must use a key already
// registered by the new initiator on each component path.
func (r *SCSIPROwnerReservation) PrepareOwnerReservation(ctx context.Context, nodeID, bootID string) error {
	if r == nil { return errors.New("SCSI PR Owner reservation manager is required") }
	if ctx == nil { return errors.New("context is required") }
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" { return errors.New("Owner node and boot identities are required") }
	if err := validatePRPaths(r.Paths); err != nil { return err }
	binary := r.Binary; if binary == "" { binary = "sg_persist" }
	runner := r.Runner; if runner == nil { runner = execRunner{} }
	timeout := r.Timeout; if timeout <= 0 { timeout = 30 * time.Second }
	registrar := &SCSIPRRegistrar{Binary: binary, Paths: r.Paths, Timeout: timeout, Runner: runner}
	if err := registrar.RegisterReader(ctx, nodeID, bootID); err != nil { return fmt.Errorf("register new Owner PR keys on all paths: %w", err) }
	return nil
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

	holder := r.Paths[0].LocalKey
	for _, path := range r.Paths {
		present, err := (&SCSIPRFencer{}).keyPresent(ctx, runner, binary, path.Device, path.LocalKey)
		if err != nil { return fmt.Errorf("preflight prepared Owner PR key on %s: %w", path.Device, err) }
		if !present { return fmt.Errorf("Owner PR key %x was not prepared on %s before RESERVE", path.LocalKey, path.Device) }
	}
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


func (r *SCSIPROwnerReservation) prCommandConfig() (string, CommandRunner) {
	binary := r.Binary
	if binary == "" { binary = "sg_persist" }
	runner := r.Runner
	if runner == nil { runner = execRunner{} }
	return binary, runner
}

func readReservationState(ctx context.Context, runner CommandRunner, binary, path string) (uint64, uint8, bool, error) {
	out, err := runner.Run(ctx, binary, "--in", "--read-reservation", "--device="+path)
	if err != nil { return 0, 0, false, fmt.Errorf("read SCSI PR reservation on %s: %w: %s", path, err, strings.TrimSpace(string(out))) }
	lower := strings.ToLower(string(out))
	if strings.Contains(lower, "no reservation") || strings.Contains(lower, "reservation not held") || strings.Contains(lower, "not reserved") {
		return 0, 0, false, nil
	}
	key, typ, ok := parseLiveReservation(string(out))
	if !ok { return 0, 0, false, fmt.Errorf("ambiguous SCSI PR reservation response on %s: %s", path, strings.TrimSpace(string(out))) }
	return key, typ, true, nil
}

// VerifyNoForeignRegistrations is used only for first-time Pool bootstrap. It
// permits registrations belonging to this configured initiator, but rejects
// any other initiator because the bootstrap format has no previous-owner
// record that could safely identify and drain an interrupted foreign writer.
func (r *SCSIPROwnerReservation) VerifyNoForeignRegistrations(ctx context.Context) error {
	if r == nil || ctx == nil { return errors.New("SCSI PR reservation manager and context are required") }
	if err := validatePRPaths(r.Paths); err != nil { return err }
	binary, runner := r.prCommandConfig()
	allowed := make(map[uint64]struct{}, len(r.Paths))
	for _, path := range r.Paths { allowed[path.LocalKey] = struct{}{} }
	var holder uint64
	var holderType uint8
	heldCount, absentCount := 0, 0
	for _, path := range r.Paths {
		out, err := runner.Run(ctx, binary, "--in", "--read-keys", "--device="+path.Device)
		if err != nil { return fmt.Errorf("read SCSI PR registrations on %s: %w: %s", path.Device, err, strings.TrimSpace(string(out))) }
		for key := range parsePRKeys(out) {
			if _, ok := allowed[key]; !ok { return fmt.Errorf("Pool bootstrap refuses foreign SCSI PR registration %x on %s", key, path.Device) }
		}
		key, typ, held, err := readReservationState(ctx, runner, binary, path.Device)
		if err != nil { return err }
		if !held { absentCount++; continue }
		heldCount++
		if holder == 0 { holder, holderType = key, typ } else if holder != key || holderType != typ {
			return errors.New("SCSI PR reservation differs across configured Pool paths")
		}
	}
	if heldCount != 0 && absentCount != 0 { return errors.New("SCSI PR reservation is present on only some Pool paths") }
	if heldCount != 0 {
		if holder != r.Paths[0].LocalKey { return fmt.Errorf("Pool bootstrap reservation is held by foreign key %x", holder) }
		typ := r.ReservationType
		if typ == 0 { typ = 6 }
		if holderType != typ { return fmt.Errorf("Pool bootstrap reservation type %d differs from configured type %d", holderType, typ) }
	}
	return nil
}

// VerifyOwnerReservation rechecks the reservation on every component path.
// Pool bootstrap calls this from OwnerLease.Valid before each metadata phase.
func (r *SCSIPROwnerReservation) VerifyOwnerReservation(ctx context.Context, nodeID, bootID string) error {
	if r == nil || ctx == nil || strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" {
		return errors.New("SCSI PR reservation and initiator identity are required")
	}
	if err := validatePRPaths(r.Paths); err != nil { return err }
	binary, runner := r.prCommandConfig()
	typ := r.ReservationType
	if typ == 0 { typ = 6 }
	return verifyLiveReservation(ctx, runner, binary, r.Paths, r.Paths[0].LocalKey, typ)
}

// ReleaseOwnerReservation releases only this initiator's verified reservation,
// verifies that every path reports no holder, and then unregisters local keys.
func (r *SCSIPROwnerReservation) ReleaseOwnerReservation(ctx context.Context, nodeID, bootID string) error {
	if r == nil || ctx == nil { return errors.New("SCSI PR reservation manager and context are required") }
	if err := r.VerifyNoForeignRegistrations(ctx); err != nil { return err }
	binary, runner := r.prCommandConfig()
	typ := r.ReservationType
	if typ == 0 { typ = 6 }
	holder := r.Paths[0].LocalKey
	key, actualType, held, err := readReservationState(ctx, runner, binary, r.Paths[0].Device)
	if err != nil { return err }
	if held {
		if key != holder || actualType != typ { return ErrOwnerReservationMismatch }
		out, releaseErr := runner.Run(ctx, binary, "--out", "--release", fmt.Sprintf("--param-rk=%x", holder),
			fmt.Sprintf("--prout-type=%d", typ), "--device="+r.Paths[0].Device)
		if releaseErr != nil { return fmt.Errorf("release Pool bootstrap SCSI PR reservation: %w: %s", releaseErr, strings.TrimSpace(string(out))) }
	}
	for _, path := range r.Paths {
		_, _, stillHeld, verifyErr := readReservationState(ctx, runner, binary, path.Device)
		if verifyErr != nil { return verifyErr }
		if stillHeld { return fmt.Errorf("Pool bootstrap SCSI PR reservation remains on %s", path.Device) }
	}
	registrar := &SCSIPRRegistrar{Binary: binary, Paths: r.Paths, Runner: runner}
	if err = registrar.UnregisterReader(ctx, nodeID, bootID); err != nil { return fmt.Errorf("unregister Pool bootstrap PR keys: %w", err) }
	return nil
}

var _ OwnerReservation = (*SCSIPROwnerReservation)(nil)
