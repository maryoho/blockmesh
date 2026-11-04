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
	Device string
	LocalKey uint64
}


func validatePRPaths(paths []PRPath) error {
	if len(paths)==0 { return errors.New("at least one SCSI PR path is required") }
	devices:=make(map[string]struct{},len(paths))
	keys:=make(map[uint64]struct{},len(paths))
	for _,path:=range paths {
		if strings.TrimSpace(path.Device)=="" || path.LocalKey==0 { return errors.New("every SCSI PR path needs a device and nonzero local key") }
		if _,ok:=devices[path.Device];ok { return fmt.Errorf("duplicate SCSI PR device path %s",path.Device) }
		if _,ok:=keys[path.LocalKey];ok { return fmt.Errorf("local PR keys must be unique per initiator path: %x",path.LocalKey) }
		devices[path.Device]=struct{}{}
		keys[path.LocalKey]=struct{}{}
	}
	return nil
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

// SCSIPRFencer uses PREEMPT AND ABORT for every key belonging to the exact
// node boot instance, then verifies that none remain on any configured path.
// The reservation type must match the active reservation on the LU.
type SCSIPRFencer struct {
	Binary string
	Paths []PRPath
	Keys RegistrationKeyResolver
	ReservationType uint8
	Timeout time.Duration
	Runner CommandRunner
}

var (
	keyLinePattern = regexp.MustCompile(`(?i)^\s*key\s*=\s*0x([0-9a-f]{1,16})\s*$`)
	plainKeyLinePattern = regexp.MustCompile(`(?i)^\s*0x([0-9a-f]{1,16})\s*$`)
)


type ReaderRegistrar interface {
	RegisterReader(context.Context, string, string) error
	UnregisterReader(context.Context, string, string) error
}

type SCSIPRRegistrar struct {
	Binary string
	Paths []PRPath
	Timeout time.Duration
	Runner CommandRunner
}


func (r *SCSIPRRegistrar) UnregisterReader(ctx context.Context, nodeID, bootID string) error {
	if r == nil { return errors.New("SCSI PR registrar requires local paths") }
	if err:=validatePRPaths(r.Paths);err!=nil{return err}
	binary:=r.Binary
	if binary=="" { binary="sg_persist" }
	runner:=r.Runner
	if runner==nil { runner=execRunner{} }
	timeout:=r.Timeout
	if timeout<=0 { timeout=30*time.Second }
	ctx,cancel:=context.WithTimeout(ctx,timeout)
	defer cancel()
	for _,path:=range r.Paths {
		if strings.TrimSpace(path.Device)=="" || path.LocalKey==0 { return errors.New("every PR registration path needs a device and nonzero local key") }
		present,err:=(&SCSIPRFencer{}).keyPresent(ctx,runner,binary,path.Device,path.LocalKey)
		if err!=nil { return fmt.Errorf("read local PR keys on %s: %w",path.Device,err) }
		if !present { continue }
		output,runErr:=runner.Run(ctx,binary,"--out","--register",
			fmt.Sprintf("--param-rk=%x",path.LocalKey),"--param-sark=0","--device="+path.Device)
		if runErr!=nil { return fmt.Errorf("unregister local PR key for %s/%s on %s: %w: %s",nodeID,bootID,path.Device,runErr,strings.TrimSpace(string(output))) }
	}
	for _,path:=range r.Paths {
		present,err:=(&SCSIPRFencer{}).keyPresent(ctx,runner,binary,path.Device,path.LocalKey)
		if err!=nil { return fmt.Errorf("verify local PR key removal on %s: %w",path.Device,err) }
		if present { return fmt.Errorf("local PR key %x remains registered on %s",path.LocalKey,path.Device) }
	}
	return nil
}

func (r *SCSIPRRegistrar) RegisterReader(ctx context.Context, nodeID, bootID string) error {
	if r == nil || len(r.Paths) == 0 { return errors.New("SCSI PR registrar requires local paths") }
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(bootID) == "" { return errors.New("reader identity is required") }
	binary:=r.Binary
	if binary=="" { binary="sg_persist" }
	runner:=r.Runner
	if runner==nil { runner=execRunner{} }
	timeout:=r.Timeout
	if timeout<=0 { timeout=30*time.Second }
	ctx,cancel:=context.WithTimeout(ctx,timeout)
	defer cancel()
	for _,path:=range r.Paths {
		if strings.TrimSpace(path.Device)=="" || path.LocalKey==0 { return errors.New("every PR registration path needs a device and nonzero local key") }
		present,err:=(&SCSIPRFencer{}).keyPresent(ctx,runner,binary,path.Device,path.LocalKey)
		if err!=nil { return fmt.Errorf("read local PR keys on %s: %w",path.Device,err) }
		if !present {
			output,runErr:=runner.Run(ctx,binary,"--out","--register","--param-rk=0",
				fmt.Sprintf("--param-sark=%x",path.LocalKey),"--param-aptpl","--device="+path.Device)
			if runErr!=nil { return fmt.Errorf("register local PR key for %s/%s on %s: %w: %s",nodeID,bootID,path.Device,runErr,strings.TrimSpace(string(output))) }
		}
	}
	for _,path:=range r.Paths {
		present,err:=(&SCSIPRFencer{}).keyPresent(ctx,runner,binary,path.Device,path.LocalKey)
		if err!=nil { return fmt.Errorf("verify local PR key on %s: %w",path.Device,err) }
		if !present { return fmt.Errorf("local PR key %x is not registered on %s",path.LocalKey,path.Device) }
	}
	return nil
}

func (f *SCSIPRFencer) FenceNode(ctx context.Context, nodeID, bootID string) error {
	if f == nil || f.Keys == nil {
		return errors.New("SCSI PR fencer requires paths and a registration-key resolver")
	}
	if err:=validatePRPaths(f.Paths);err!=nil{return err}
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
	for _, path := range f.Paths {
		for targetKey := range unique {
			if targetKey == path.LocalKey { return fmt.Errorf("refusing to preempt local registration key %x",targetKey) }
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
		if match == nil { match = plainKeyLinePattern.FindStringSubmatch(line) }
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
}

var _ Fencer = (*SCSIPRFencer)(nil)
