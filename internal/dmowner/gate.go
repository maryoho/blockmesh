package dmowner

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type Gate interface {
	// FenceCurrent closes and drains whichever target instance is currently
	// loaded. It is used after SAN fencing but before WAL recovery.
	FenceCurrent(context.Context, string) error
	// Prepare returns the per-table instance ID captured from DM status.
	Prepare(context.Context, string, uint64) (uint64, error)
	Enable(context.Context, string, uint64, uint64) error
	Fence(context.Context, string, uint64, uint64) error
	Drain(context.Context, string, uint64, uint64) error
}

type CommandRunner interface { Run(context.Context, string, ...string) ([]byte, error) }
type ExecRunner struct{}
func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name == "" { return nil, errors.New("command name is required") }
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil { return out, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out))) }
	return out, nil
}
type DMSetupGate struct { Runner CommandRunner; Binary string }

func (g DMSetupGate) runner() (CommandRunner, string) {
	r := g.Runner
	if r == nil { r = ExecRunner{} }
	b := g.Binary
	if b == "" { b = "dmsetup" }
	return r, b
}
func validDevice(ctx context.Context, device string) error {
	if ctx == nil { return errors.New("context is required") }
	if strings.TrimSpace(device) == "" || strings.ContainsAny(device, "\r\n") { return errors.New("valid DM device name is required") }
	return nil
}
func validEpoch(epoch uint64) error {
	if epoch == 0 { return errors.New("OwnerEpoch must be nonzero") }
	return nil
}
func (g DMSetupGate) status(ctx context.Context, device string) (uint64, uint64, error) {
	if err := validDevice(ctx, device); err != nil { return 0, 0, err }
	r, b := g.runner()
	out, err := r.Run(ctx, b, "status", device)
	if err != nil { return 0, 0, err }
	var epoch, instance uint64
	var epochFound, instanceFound bool
	for _, field := range strings.Fields(string(out)) {
		switch {
		case strings.HasPrefix(field, "instance="):
			value := strings.TrimPrefix(field, "instance=")
			parsed, parseErr := strconv.ParseUint(value, 16, 64)
			if parseErr != nil || parsed == 0 { return 0, 0, fmt.Errorf("invalid DM target instance ID %q", value) }
			if instanceFound && parsed != instance { return 0, 0, errors.New("DM status contains multiple target instance IDs") }
			instance, instanceFound = parsed, true
		case strings.HasPrefix(field, "epoch="):
			value := strings.TrimPrefix(field, "epoch=")
			parsed, parseErr := strconv.ParseUint(value, 10, 64)
			if parseErr != nil { return 0, 0, fmt.Errorf("invalid DM target epoch %q", value) }
			if epochFound && parsed != epoch { return 0, 0, errors.New("DM status contains multiple target epochs") }
			epoch, epochFound = parsed, true
		}
	}
	if !instanceFound || !epochFound { return 0, 0, errors.New("DM status did not expose target instance and epoch") }
	return epoch, instance, nil
}
func (g DMSetupGate) message(ctx context.Context, device, command string, epoch, instance uint64) error {
	if err := validDevice(ctx, device); err != nil { return err }
	if err := validEpoch(epoch); err != nil { return err }
	if instance == 0 { return errors.New("DM target instance ID must be nonzero") }
	r, b := g.runner()
	message := fmt.Sprintf("%s %d %x", command, epoch, instance)
	_, err := r.Run(ctx, b, "message", device, "0", message)
	return err
}
func (g DMSetupGate) FenceCurrent(ctx context.Context, device string) error {
	epoch, instance, err := g.status(ctx, device)
	if err != nil { return err }
	if epoch == 0 { return nil }
	return g.message(ctx, device, "owner_fence", epoch, instance)
}
func (g DMSetupGate) Prepare(ctx context.Context, device string, epoch uint64) (uint64, error) {
	if err := validEpoch(epoch); err != nil { return 0, err }
	_, instance, err := g.status(ctx, device)
	if err != nil { return 0, err }
	if err = g.message(ctx, device, "owner_prepare", epoch, instance); err != nil { return 0, err }
	return instance, nil
}
func (g DMSetupGate) Enable(ctx context.Context, device string, epoch, instance uint64) error {
	return g.message(ctx, device, "owner_enable", epoch, instance)
}
func (g DMSetupGate) Fence(ctx context.Context, device string, epoch, instance uint64) error {
	return g.message(ctx, device, "owner_fence", epoch, instance)
}
func (g DMSetupGate) Drain(ctx context.Context, device string, epoch, instance uint64) error {
	return g.Fence(ctx, device, epoch, instance)
}
var _ Gate = DMSetupGate{}
