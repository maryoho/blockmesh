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
	Prepare(context.Context, string, uint64) error
	Enable(context.Context, string, uint64) error
	Fence(context.Context, string, uint64) error
	Drain(context.Context, string, uint64) error
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
func (g DMSetupGate) command(ctx context.Context, device, message string) error {
	if ctx == nil { return errors.New("context is required") }
	if strings.TrimSpace(device) == "" || strings.ContainsAny(device, "\r\n") { return errors.New("valid DM device name is required") }
	runner := g.Runner
	if runner == nil { runner = ExecRunner{} }
	binary := g.Binary
	if binary == "" { binary = "dmsetup" }
	_, err := runner.Run(ctx, binary, "message", device, "0", message)
	return err
}
func validEpoch(epoch uint64) error { if epoch == 0 { return errors.New("OwnerEpoch must be nonzero") }; return nil }
func (g DMSetupGate) Prepare(ctx context.Context, device string, epoch uint64) error {
	if err := validEpoch(epoch); err != nil { return err }
	return g.command(ctx, device, "owner_prepare "+strconv.FormatUint(epoch, 10))
}
func (g DMSetupGate) Enable(ctx context.Context, device string, epoch uint64) error {
	if err := validEpoch(epoch); err != nil { return err }
	return g.command(ctx, device, "owner_enable "+strconv.FormatUint(epoch, 10))
}
func (g DMSetupGate) Fence(ctx context.Context, device string, epoch uint64) error {
	if err := validEpoch(epoch); err != nil { return err }
	return g.command(ctx, device, "owner_fence "+strconv.FormatUint(epoch, 10))
}
// Drain is epoch-scoped too: an old runtime must not close a newer Owner.
func (g DMSetupGate) Drain(ctx context.Context, device string, epoch uint64) error {
	return g.Fence(ctx, device, epoch)
}
var _ Gate = DMSetupGate{}
