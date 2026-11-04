package fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

var (
	keyLinePattern = regexp.MustCompile(`(?i)^\s*key\s*=\s*0x([0-9a-f]{1,16})\s*package fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

)
	plainKeyLinePattern = regexp.MustCompile(`(?i)^\s*0x([0-9a-f]{1,16})\s*package fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

)
)

func (f *SCSIPRFencer) FenceNode(ctx context.Context, nodeID, bootID string) error {
	if f == nil || f.Keys == nil || len(f.Paths) == 0 {
		return errors.New("SCSI PR fencer requires paths and a registration-key resolver")
	}
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" {
		return errors.New("node ID and boot ID are required for fencing")
	}
	binary := f.Binary
	if binary == "" { binary = "sg_persist" }
	runner := f.Runner
	if runner == nil { runner = execRunner{} }
	prType := f.ReservationType
	if prType == 0 { prType = 6 }
	if prType != 1 && prType != 3 && prType != 5 && prType != 6 && prType != 7 && prType != 8 {
		return fmt.Errorf("invalid SCSI persistent reservation type %d", prType)
	}
	timeout := f.Timeout
	if timeout <= 0 { timeout = 30 * time.Second }
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	targetKeys, err := f.Keys.KeysForNode(ctx, nodeID, bootID)
	if err != nil { return fmt.Errorf("resolve PR keys for %s/%s: %w", nodeID, bootID, err) }
	if len(targetKeys) == 0 { return ErrPRKeyUnknown }
	unique := make(map[uint64]struct{}, len(targetKeys))
	for _, key := range targetKeys {
		if key == 0 { return errors.New("zero SCSI PR registration key is invalid") }
		if _, exists := unique[key]; exists { return errors.New("duplicate SCSI PR registration key") }
		unique[key] = struct{}{}
	}

	// Each local path may represent a different local initiator registration.
	// Try the matching local key on each path, but never treat a failed command
	// as success; the final per-path PRIN verification is authoritative.
	for _, path := range f.Paths {
		if strings.TrimSpace(path.Device) == "" || path.LocalKey == 0 {
			return errors.New("every SCSI PR path needs a device and nonzero local registration key")
		}
		for targetKey := range unique {
			present, readErr := f.keyPresent(ctx, runner, binary, path.Device, targetKey)
			if readErr != nil { return fmt.Errorf("read PR keys on %s: %w", path.Device, readErr) }
			if !present { continue }
			args := []string{
				"--out", "--preempt-abort",
				fmt.Sprintf("--param-rk=%x", path.LocalKey),
				fmt.Sprintf("--param-sark=%x", targetKey),
				fmt.Sprintf("--prout-type=%d", prType),
				"--device=" + path.Device,
			}
			output, runErr := runner.Run(ctx, binary, args...)
			if runErr != nil {
				return fmt.Errorf("PREEMPT AND ABORT target key %x through %s: %w: %s", targetKey, path.Device, runErr, strings.TrimSpace(string(output)))
			}
		}
	}

	for _, path := range f.Paths {
		output, readErr := runner.Run(ctx, binary, "--in", "--read-keys", "--device="+path.Device)
		if readErr != nil {
			return fmt.Errorf("verify PR registrations on %s: %w: %s", path.Device, readErr, strings.TrimSpace(string(output)))
		}
		keys := parsePRKeys(output)
		for targetKey := range unique {
			if _, found := keys[targetKey]; found {
				return fmt.Errorf("%w: key %x on %s", ErrPRTargetStillRegistered, targetKey, path.Device)
			}
		}
	}
	return nil
}

func (f *SCSIPRFencer) keyPresent(ctx context.Context, runner CommandRunner, binary, device string, key uint64) (bool, error) {
	output, err := runner.Run(ctx, binary, "--in", "--read-keys", "--device="+device)
	if err != nil { return false, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output))) }
	_, found := parsePRKeys(output)[key]
	return found, nil
}

func parsePRKeys(output []byte) map[uint64]struct{} {
	keys := make(map[uint64]struct{})
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		match := keyLinePattern.FindStringSubmatch(line)
		if match == nil {
			match = plainKeyLinePattern.FindStringSubmatch(line)
		}
		if match == nil { continue }
		var key uint64
		for _, c := range strings.ToLower(match[1]) {
			key <<= 4
			switch {
			case c >= '0' && c <= '9': key |= uint64(c-'0')
			case c >= 'a' && c <= 'f': key |= uint64(c-'a'+10)
			}
		}
		keys[key] = struct{}{}
	}
	return keys
}ackage fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

var (
	keyLinePattern = regexp.MustCompile(`(?i)^\s*key\s*=\s*0x([0-9a-f]{1,16})\s*package fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

)
	plainKeyLinePattern = regexp.MustCompile(`(?i)^\s*0x([0-9a-f]{1,16})\s*package fence

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	ErrPRKeyUnknown = errors.New("no persistent reservation key configured for target reader")
	ErrPRTargetStillRegistered = errors.New("target registration key remains on SAN device")
)

type PRPath struct {
	// Device is a stable multipath device or a single path to the same LU.
	// Use one entry per independently reachable local initiator path.
	Device string
	// LocalKey must be registered by the fencing host on this LU through
	// the initiator represented by Device.
	LocalKey uint64
}

type RegistrationKeyResolver interface {
	KeysForNode(context.Context, string, string) ([]uint64, error)
}

type RegistrationKeyFunc func(context.Context, string, string) ([]uint64, error)
func (f RegistrationKeyFunc) KeysForNode(ctx context.Context, nodeID, bootID string) ([]uint64, error) {
	return f(ctx, nodeID, bootID)
}

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}
func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

// SCSIPRFencer revokes every registration key associated with one node boot
// instance using SCSI PERSISTENT RESERVE OUT PREEMPT AND ABORT. It then reads
// keys on every configured local path and fails closed if any target key is
// still visible. ReservationType must match the active reservation type;
// type 6 (Exclusive Access - Registrants Only) is the usual choice when all
// permitted readers and the writer register with the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

)
)

func (f *SCSIPRFencer) FenceNode(ctx context.Context, nodeID, bootID string) error {
	if f == nil || f.Keys == nil || len(f.Paths) == 0 {
		return errors.New("SCSI PR fencer requires paths and a registration-key resolver")
	}
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" {
		return errors.New("node ID and boot ID are required for fencing")
	}
	binary := f.Binary
	if binary == "" { binary = "sg_persist" }
	runner := f.Runner
	if runner == nil { runner = execRunner{} }
	prType := f.ReservationType
	if prType == 0 { prType = 6 }
	if prType != 1 && prType != 3 && prType != 5 && prType != 6 && prType != 7 && prType != 8 {
		return fmt.Errorf("invalid SCSI persistent reservation type %d", prType)
	}
	timeout := f.Timeout
	if timeout <= 0 { timeout = 30 * time.Second }
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	targetKeys, err := f.Keys.KeysForNode(ctx, nodeID, bootID)
	if err != nil { return fmt.Errorf("resolve PR keys for %s/%s: %w", nodeID, bootID, err) }
	if len(targetKeys) == 0 { return ErrPRKeyUnknown }
	unique := make(map[uint64]struct{}, len(targetKeys))
	for _, key := range targetKeys {
		if key == 0 { return errors.New("zero SCSI PR registration key is invalid") }
		if _, exists := unique[key]; exists { return errors.New("duplicate SCSI PR registration key") }
		unique[key] = struct{}{}
	}

	// Each local path may represent a different local initiator registration.
	// Try the matching local key on each path, but never treat a failed command
	// as success; the final per-path PRIN verification is authoritative.
	for _, path := range f.Paths {
		if strings.TrimSpace(path.Device) == "" || path.LocalKey == 0 {
			return errors.New("every SCSI PR path needs a device and nonzero local registration key")
		}
		for targetKey := range unique {
			present, readErr := f.keyPresent(ctx, runner, binary, path.Device, targetKey)
			if readErr != nil { return fmt.Errorf("read PR keys on %s: %w", path.Device, readErr) }
			if !present { continue }
			args := []string{
				"--out", "--preempt-abort",
				fmt.Sprintf("--param-rk=%x", path.LocalKey),
				fmt.Sprintf("--param-sark=%x", targetKey),
				fmt.Sprintf("--prout-type=%d", prType),
				"--device=" + path.Device,
			}
			output, runErr := runner.Run(ctx, binary, args...)
			if runErr != nil {
				return fmt.Errorf("PREEMPT AND ABORT target key %x through %s: %w: %s", targetKey, path.Device, runErr, strings.TrimSpace(string(output)))
			}
		}
	}

	for _, path := range f.Paths {
		output, readErr := runner.Run(ctx, binary, "--in", "--read-keys", "--device="+path.Device)
		if readErr != nil {
			return fmt.Errorf("verify PR registrations on %s: %w: %s", path.Device, readErr, strings.TrimSpace(string(output)))
		}
		keys := parsePRKeys(output)
		for targetKey := range unique {
			if _, found := keys[targetKey]; found {
				return fmt.Errorf("%w: key %x on %s", ErrPRTargetStillRegistered, targetKey, path.Device)
			}
		}
	}
	return nil
}

func (f *SCSIPRFencer) keyPresent(ctx context.Context, runner CommandRunner, binary, device string, key uint64) (bool, error) {
	output, err := runner.Run(ctx, binary, "--in", "--read-keys", "--device="+device)
	if err != nil { return false, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output))) }
	_, found := parsePRKeys(output)[key]
	return found, nil
}

func parsePRKeys(output []byte) map[uint64]struct{} {
	keys := make(map[uint64]struct{})
	for _, match := range keyPattern.FindAllSubmatch(output, -1) {
		var key uint64
		for _, c := range strings.TrimPrefix(strings.ToLower(string(match[1])), "0x") {
			key <<= 4
			switch {
			case c >= '0' && c <= '9': key |= uint64(c-'0')
			case c >= 'a' && c <= 'f': key |= uint64(c-'a'+10)
			}
		}
		keys[key] = struct{}{}
	}
	return keys
}

var _ Fencer = (*SCSIPRFencer)(nil)
