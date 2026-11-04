// Package sanqual collects read-only evidence for shared-SAN qualification.
// It never changes PR, sanlock, device-mapper, or host power state.
package sanqual

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CommandResult struct {
	Binary string `json:"binary"`
	Args []string `json:"args"`
	Output string `json:"output"`
	Error string `json:"error,omitempty"`
	ExitCode int `json:"exit_code"`
	StartedAt time.Time `json:"started_at"`
	Duration time.Duration `json:"duration"`
}

type ReservationEvidence struct {
	Present bool `json:"present"`
	HolderKey string `json:"holder_key,omitempty"`
	Scope string `json:"scope,omitempty"`
	Type uint8 `json:"type,omitempty"`
	TypeName string `json:"type_name,omitempty"`
	AllRegistrants bool `json:"all_registrants"`
	Parsed bool `json:"parsed"`
}

type PathEvidence struct {
	Device string `json:"device"`
	WWID string `json:"wwid,omitempty"`
	ReadKeys CommandResult `json:"read_keys"`
	ReadReservation CommandResult `json:"read_reservation"`
	RegisteredKeys []string `json:"registered_keys,omitempty"`
	KeysParsed bool `json:"keys_parsed"`
	Reservation ReservationEvidence `json:"reservation"`
}

type Evidence struct {
	SchemaVersion string `json:"schema_version"`
	CollectedAt time.Time `json:"collected_at"`
	Hostname string `json:"hostname"`
	VolumeID string `json:"volume_id,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	BootID string `json:"boot_id,omitempty"`
	MultipathMap string `json:"multipath_map"`
	Paths []PathEvidence `json:"paths"`
	DiscoveredPaths []string `json:"discovered_paths,omitempty"`
	PathCoverageVerified bool `json:"path_coverage_verified"`
	HostCommands []CommandResult `json:"host_commands"`
	Warnings []string `json:"warnings,omitempty"`
}

type Runner interface { Run(context.Context, string, ...string) ([]byte, error) }
type ExecRunner struct{}
func (ExecRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

type Collector struct {
	Runner Runner
	Timeout time.Duration
	ValidateDevice func(string) error
}

func (c Collector) defaults() (Runner, time.Duration, func(string) error) {
	r := c.Runner
	if r == nil { r = ExecRunner{} }
	timeout := c.Timeout
	if timeout <= 0 { timeout = 10 * time.Second }
	validate := c.ValidateDevice
	if validate == nil { validate = validateBlockDevice }
	return r, timeout, validate
}

func validateBlockDevice(path string) error {
	if !filepath.IsAbs(path) { return fmt.Errorf("device path %q must be absolute", path) }
	info, err := os.Stat(path)
	if err != nil { return err }
	if info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		return fmt.Errorf("%s is not a block device", path)
	}
	return nil
}

func run(ctx context.Context, r Runner, timeout time.Duration, binary string, args ...string) CommandResult {
	started := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	output, err := r.Run(callCtx, binary, args...)
	result := CommandResult{
		Binary: binary, Args: append([]string(nil), args...), Output: strings.TrimSpace(string(output)),
		StartedAt: started.UTC(), Duration: time.Since(started),
	}
	if err != nil {
		result.ExitCode = 1
		result.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) { result.ExitCode = exitErr.ExitCode() }
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) { result.Error = "command timed out: " + err.Error() }
	}
	return result
}

func (c Collector) Collect(ctx context.Context, multipathMap, volumeID, nodeID, bootID string, paths []string) (Evidence, error) {
	if ctx == nil { return Evidence{}, errors.New("context is required") }
	if strings.TrimSpace(multipathMap) == "" { return Evidence{}, errors.New("multipath map device is required") }
	if len(paths) == 0 { return Evidence{}, errors.New("explicit component paths are required; refusing to infer incomplete multipath coverage") }
	runner, timeout, validate := c.defaults()
	if err := validate(filepath.Clean(strings.TrimSpace(multipathMap))); err != nil { return Evidence{}, fmt.Errorf("validate multipath map %s: %w", multipathMap, err) }
	cleanPaths := make([]string, 0, len(paths))
	seen := make(map[string]bool, len(paths))
	for _, raw := range paths {
		path := filepath.Clean(strings.TrimSpace(raw))
		if path == "." || seen[path] { return Evidence{}, fmt.Errorf("empty or duplicate SAN path %q", raw) }
		if err := validate(path); err != nil { return Evidence{}, fmt.Errorf("validate SAN path %s: %w", path, err) }
		seen[path] = true
		cleanPaths = append(cleanPaths, path)
	}
	hostname, _ := os.Hostname()
	e := Evidence{
		SchemaVersion: "blockmesh-san-qualification/v1", CollectedAt: time.Now().UTC(), Hostname: hostname,
		VolumeID: volumeID, NodeID: nodeID, BootID: bootID, MultipathMap: multipathMap,
	}
	// The command list is intentionally read-only. Preserve raw output for audit.
	e.HostCommands = []CommandResult{
		run(ctx, runner, timeout, "multipath", "-ll", multipathMap),
		run(ctx, runner, timeout, "dmsetup", "info", "-c", "--noheadings", "-o", "name,uuid,attr,open,segments,events", multipathMap),
		run(ctx, runner, timeout, "dmsetup", "status", multipathMap),
		run(ctx, runner, timeout, "sanlock", "client", "status"),
		run(ctx, runner, timeout, "sanlock", "status"),
		run(ctx, runner, timeout, "lsblk", "-J", "-O", multipathMap),
	}
	for _, cmd := range e.HostCommands {
		if cmd.ExitCode != 0 { e.Warnings = append(e.Warnings, cmd.Binary+" "+strings.Join(cmd.Args, " ")+": "+cmd.Error) }
	}
	if e.HostCommands[0].ExitCode == 0 {
		e.DiscoveredPaths = parseMultipathPaths(e.HostCommands[0].Output)
		expected := make(map[string]bool, len(cleanPaths))
		found := make(map[string]bool, len(e.DiscoveredPaths))
		for _, path := range cleanPaths { expected[filepath.Base(path)] = true }
		for _, path := range e.DiscoveredPaths { found[path] = true }
		e.PathCoverageVerified = len(found) > 0 && len(found) == len(expected)
		for path := range expected {
			if !found[path] { e.Warnings = append(e.Warnings, "multipath output does not list supplied path "+path); e.PathCoverageVerified = false }
		}
		for path := range found {
			if !expected[path] { e.Warnings = append(e.Warnings, "multipath contains uninspected path "+path); e.PathCoverageVerified = false }
		}
	} else {
		e.Warnings = append(e.Warnings, "multipath component-path coverage unavailable")
	}
	for _, path := range cleanPaths {
		wwid := run(ctx, runner, timeout, "udevadm", "info", "--query=property", "--name", path)
		keys := run(ctx, runner, timeout, "sg_persist", "--in", "--read-keys", "--device="+path)
		reservation := run(ctx, runner, timeout, "sg_persist", "--in", "--read-reservation", "--device="+path)
		item := PathEvidence{Device: path, WWID: extractWWID(wwid.Output), ReadKeys: keys, ReadReservation: reservation}
		if keys.ExitCode == 0 { item.RegisteredKeys, item.KeysParsed = parseKeys(keys.Output) } else { e.Warnings = append(e.Warnings, "cannot read PR keys on "+path+": "+keys.Error) }
		if reservation.ExitCode == 0 {
			item.Reservation = parseReservation(reservation.Output)
			if !item.Reservation.Parsed { e.Warnings = append(e.Warnings, "cannot parse PR reservation on "+path) }
		} else { e.Warnings = append(e.Warnings, "cannot read PR reservation on "+path+": "+reservation.Error) }
		if item.WWID == "" { e.Warnings = append(e.Warnings, "cannot establish WWID for "+path) }
		e.Paths = append(e.Paths, item)
	}
	wwids := make(map[string]bool)
	for _, path := range e.Paths { if path.WWID != "" { wwids[path.WWID] = true } }
	if len(wwids) != 1 {
		e.PathCoverageVerified = false
		e.Warnings = append(e.Warnings, "component paths do not all resolve to one known WWID")
	}
	return e, nil
}

var (
	keyLineRE = regexp.MustCompile("(?i)^\\s*key\\s*=\\s*0x([0-9a-f]{1,16})\\s*$")
	plainKeyRE = regexp.MustCompile("(?i)^\\s*0x([0-9a-f]{1,16})\\s*$")
	wwidRE = regexp.MustCompile("(?m)^ID_WWN(?:_WITH_EXTENSION)?=(\\S+)\\s*$")
	majorMinorRE = regexp.MustCompile("^\\d+:\\d+$")
	reservationKeyRE = regexp.MustCompile("(?i)^\\s*(?:key|reservation\\s+holder)\\s*[:=]\\s*0x([0-9a-f]{1,16})\\s*$")
	reservationTypeRE = regexp.MustCompile("(?i)\\btype\\s*:\\s*([^,\\r\\n]+(?:,\\s*(?:registrants only|all registrants))?)")
	reservationTypeCodeRE = regexp.MustCompile("(?i)\\btype\\s*=\\s*(0x[0-9a-f]+|[0-9]+)")
	reservationScopeRE = regexp.MustCompile("(?i)\\bscope\\s*:\\s*([^,\\r\\n]+)")
)

func normalizeKey(value string) string {
	value = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "0x")
	value = strings.TrimLeft(value, "0")
	if value == "" { return "0" }
	return value
}

func parseKeys(output string) ([]string, bool) {
	set := make(map[string]bool)
	parseable := strings.Contains(strings.ToLower(output), "pr generation") || strings.Contains(strings.ToLower(output), "registered reservation keys") || strings.Contains(strings.ToLower(output), "no registered")
	for _, line := range strings.Split(output, "\n") {
		match := keyLineRE.FindStringSubmatch(line)
		if match == nil { match = plainKeyRE.FindStringSubmatch(line) }
		if match != nil { parseable = true; set[normalizeKey(match[1])] = true }
	}
	keys := make([]string, 0, len(set))
	for key := range set { keys = append(keys, key) }
	sort.Strings(keys)
	return keys, parseable
}

func extractWWID(output string) string {
	match := wwidRE.FindStringSubmatch(output)
	if match == nil { return "" }
	return match[1]
}

func parseMultipathPaths(output string) []string {
	set := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		for i := 1; i+1 < len(fields); i++ {
			if majorMinorRE.MatchString(fields[i+1]) {
				name := strings.TrimLeft(fields[i], "|+-")
				if name != "" && name != "-" && name != "+" { set[name] = true }
			}
		}
	}
	paths := make([]string, 0, len(set))
	for path := range set { paths = append(paths, path) }
	sort.Strings(paths)
	return paths
}

type FenceVerification struct {
	TargetKey string `json:"target_key"`
	AbsentOnAllPaths bool `json:"absent_on_all_paths"`
	PresentPaths []string `json:"present_paths,omitempty"`
	UnknownPaths []string `json:"unknown_paths,omitempty"`
}

// VerifyTargetKeyAbsent checks collected evidence only. It does not fence.
func VerifyTargetKeyAbsent(e Evidence, targetKey string) (FenceVerification, error) {
	key := normalizeKey(targetKey)
	if len(key) > 16 { return FenceVerification{}, errors.New("target PR key exceeds 64 bits") }
	if _, err := strconv.ParseUint(key, 16, 64); err != nil { return FenceVerification{}, fmt.Errorf("invalid target PR key %q", targetKey) }
	if len(e.Paths) == 0 { return FenceVerification{}, errors.New("evidence has no SAN paths") }
	if !e.PathCoverageVerified { return FenceVerification{}, errors.New("multipath component-path coverage and common WWID are not verified; refusing a fencing conclusion") }
	result := FenceVerification{TargetKey: "0x"+key, AbsentOnAllPaths: true}
	for _, path := range e.Paths {
		if path.ReadKeys.ExitCode != 0 || !path.KeysParsed {
			result.UnknownPaths = append(result.UnknownPaths, path.Device)
			result.AbsentOnAllPaths = false
			continue
		}
		for _, registered := range path.RegisteredKeys {
			if normalizeKey(registered) == key {
				result.PresentPaths = append(result.PresentPaths, path.Device)
				result.AbsentOnAllPaths = false
				break
			}
		}
	}
	return result, nil
}

func parseReservation(output string) ReservationEvidence {
	lower := strings.ToLower(output)
	if strings.Contains(lower, "no reservation held") || strings.Contains(lower, "no reservation") || strings.Contains(lower, "reservation not held") {
		return ReservationEvidence{Present: false, Parsed: true}
	}
	result := ReservationEvidence{}
	keys := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		if match := reservationKeyRE.FindStringSubmatch(line); match != nil {
			keys[normalizeKey(match[1])] = true
		}
	}
	if len(keys) == 1 {
		for key := range keys { result.HolderKey = key }
	} else if len(keys) > 1 {
		return result
	}
	if match := reservationScopeRE.FindStringSubmatch(output); match != nil {
		result.Scope = strings.TrimSpace(match[1])
	}
	if match := reservationTypeCodeRE.FindStringSubmatch(output); match != nil {
		value := strings.TrimSpace(match[1])
		var n uint64
		var err error
		if strings.HasPrefix(strings.ToLower(value), "0x") { n, err = strconv.ParseUint(value[2:], 16, 8) } else { n, err = strconv.ParseUint(value, 10, 8) }
		if err == nil && validPRType(uint8(n)) { result.Type = uint8(n); result.TypeName = prTypeName(uint8(n)) }
	} else if match := reservationTypeRE.FindStringSubmatch(output); match != nil {
		name := strings.ToLower(strings.TrimSpace(match[1]))
		name = strings.Join(strings.Fields(name), " ")
		typeMap := map[string]uint8{
			"write exclusive": 1, "exclusive access": 3,
			"write exclusive, registrants only": 5, "exclusive access, registrants only": 6,
			"write exclusive, all registrants": 7, "exclusive access, all registrants": 8,
		}
		result.Type = typeMap[name]
		if result.Type != 0 { result.TypeName = prTypeName(result.Type) }
	}
	result.AllRegistrants = result.Type == 7 || result.Type == 8
	if result.HolderKey == "" || result.Type == 0 { return result }
	result.Present = true
	result.Parsed = true
	return result
}

func validPRType(prType uint8) bool {
	switch prType { case 1, 3, 5, 6, 7, 8: return true }
	return false
}

func prTypeName(prType uint8) string {
	switch prType {
	case 1: return "Write Exclusive"
	case 3: return "Exclusive Access"
	case 5: return "Write Exclusive, Registrants Only"
	case 6: return "Exclusive Access, Registrants Only"
	case 7: return "Write Exclusive, All Registrants"
	case 8: return "Exclusive Access, All Registrants"
	default: return ""
	}
}

type ReservationVerification struct {
	ExpectedHolderKey string `json:"expected_holder_key"`
	ExpectedType uint8 `json:"expected_type"`
	Verified bool `json:"verified"`
	MismatchPaths []string `json:"mismatch_paths,omitempty"`
	UnknownPaths []string `json:"unknown_paths,omitempty"`
}

// VerifyReservation checks the active reservation identity and type on every
// component path. It only evaluates captured evidence and never changes PR state.
func VerifyReservation(e Evidence, expectedHolderKey string, expectedType uint8) (ReservationVerification, error) {
	key := normalizeKey(expectedHolderKey)
	if len(key) > 16 { return ReservationVerification{}, errors.New("expected PR holder key exceeds 64 bits") }
	if _, err := strconv.ParseUint(key, 16, 64); err != nil { return ReservationVerification{}, fmt.Errorf("invalid expected PR holder key %q", expectedHolderKey) }
	if !validPRType(expectedType) { return ReservationVerification{}, fmt.Errorf("invalid expected SCSI PR type %d", expectedType) }
	if len(e.Paths) == 0 { return ReservationVerification{}, errors.New("evidence has no SAN paths") }
	if !e.PathCoverageVerified { return ReservationVerification{}, errors.New("multipath component-path coverage and common WWID are not verified; refusing a reservation conclusion") }
	result := ReservationVerification{ExpectedHolderKey: "0x"+key, ExpectedType: expectedType, Verified: true}
	for _, path := range e.Paths {
		if path.ReadReservation.ExitCode != 0 || !path.Reservation.Parsed {
			result.UnknownPaths = append(result.UnknownPaths, path.Device)
			result.Verified = false
			continue
		}
		if !path.Reservation.Present || normalizeKey(path.Reservation.HolderKey) != key || path.Reservation.Type != expectedType {
			result.MismatchPaths = append(result.MismatchPaths, path.Device)
			result.Verified = false
		}
	}
	return result, nil
}
