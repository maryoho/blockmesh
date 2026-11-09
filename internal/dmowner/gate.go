package dmowner

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type ReaderGate interface {
	PrepareReader(context.Context, string, uint64, uint64) (uint64, error)
	EnableReader(context.Context, string, uint64, uint64, uint64) error
	FenceReader(context.Context, string, uint64, uint64) error
}

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
type statusSnapshot struct {
	ownerEpoch uint64
	readerEpoch uint64
	rootGeneration uint64
	instance uint64
	readOnly bool
	fenced bool
}

func (g DMSetupGate) statusSnapshot(ctx context.Context, device string) (statusSnapshot, error) {
	var s statusSnapshot
	if err := validDevice(ctx, device); err != nil { return s, err }
	r, b := g.runner()
	out, err := r.Run(ctx, b, "status", device)
	if err != nil { return s, err }
	var instanceFound, ownerFound, readerFound, rootFound, readOnlyFound bool
	for _, field := range strings.Fields(string(out)) {
		switch {
		case strings.HasPrefix(field, "instance="):
			v, e := strconv.ParseUint(strings.TrimPrefix(field, "instance="), 16, 64)
			if e != nil || v == 0 { return s, errors.New("invalid DM target instance ID") }
			s.instance, instanceFound = v, true
		case strings.HasPrefix(field, "epoch="):
			v, e := strconv.ParseUint(strings.TrimPrefix(field, "epoch="), 10, 64)
			if e != nil { return s, errors.New("invalid DM OwnerEpoch") }
			s.ownerEpoch, ownerFound = v, true
		case strings.HasPrefix(field, "reader_epoch="):
			v, e := strconv.ParseUint(strings.TrimPrefix(field, "reader_epoch="), 10, 64)
			if e != nil { return s, errors.New("invalid DM ReaderEpoch") }
			s.readerEpoch, readerFound = v, true
		case strings.HasPrefix(field, "root_generation="):
			v, e := strconv.ParseUint(strings.TrimPrefix(field, "root_generation="), 10, 64)
			if e != nil { return s, errors.New("invalid DM RootGeneration") }
			s.rootGeneration, rootFound = v, true
		case strings.HasPrefix(field, "readonly="):
			v := strings.TrimPrefix(field, "readonly=")
			if v != "0" && v != "1" { return s, errors.New("invalid DM read-only state") }
			s.readOnly, readOnlyFound = v == "1", true
		case strings.HasPrefix(field, "fenced="):
			v := strings.TrimPrefix(field, "fenced=")
			if v != "0" && v != "1" { return s, errors.New("invalid DM fenced state") }
			s.fenced = v == "1"
		}
	}
	if !instanceFound || !ownerFound { return s, errors.New("DM status lacks target instance or OwnerEpoch") }
	if s.readOnly && (!readerFound || !rootFound || s.readerEpoch == 0 || s.rootGeneration == 0) {
		return s, errors.New("DM read-only status lacks ReaderEpoch or RootGeneration")
	}
	if readOnlyFound == false { s.readOnly = false }
	return s, nil
}

func (g DMSetupGate) readerMessage(ctx context.Context, device, command string, epoch, root, instance uint64, includeRoot bool) error {
	if err := validDevice(ctx, device); err != nil { return err }
	if epoch == 0 || instance == 0 || (includeRoot && root == 0) { return errors.New("ReaderEpoch, RootGeneration and target instance must be nonzero") }
	r, b := g.runner()
	message := fmt.Sprintf("%s %d", command, epoch)
	if includeRoot { message += fmt.Sprintf(" %d", root) }
	message += fmt.Sprintf(" %x", instance)
	_, err := r.Run(ctx, b, "message", device, "0", message)
	return err
}

func (g DMSetupGate) PrepareReader(ctx context.Context, device string, epoch, root uint64) (uint64, error) {
	if epoch == 0 || root == 0 { return 0, errors.New("ReaderEpoch and RootGeneration must be nonzero") }
	s, err := g.statusSnapshot(ctx, device)
	if err != nil { return 0, err }
	if s.readOnly && epoch <= s.readerEpoch { return 0, errors.New("ReaderEpoch must exceed the current DM ReaderEpoch") }
	if !s.readOnly && s.ownerEpoch != 0 && !s.fenced {
		// A live local RW Owner cannot be converted into a Reader in place.
		return 0, errors.New("DM target still has an OwnerEpoch; fence/deactivate RW before RO")
	}
	if err = g.readerMessage(ctx, device, "reader_prepare", epoch, root, s.instance, true); err != nil { return 0, err }
	return s.instance, nil
}

func (g DMSetupGate) EnableReader(ctx context.Context, device string, epoch, root, instance uint64) error {
	return g.readerMessage(ctx, device, "reader_enable", epoch, root, instance, true)
}

func (g DMSetupGate) FenceReader(ctx context.Context, device string, epoch, instance uint64) error {
	return g.readerMessage(ctx, device, "reader_fence", epoch, 0, instance, false)
}

func (g DMSetupGate) FenceCurrent(ctx context.Context, device string) error {
	s, err := g.statusSnapshot(ctx, device)
	if err != nil { return err }
	if s.readOnly {
		return g.FenceReader(ctx, device, s.readerEpoch, s.instance)
	}
	if s.ownerEpoch == 0 { return nil }
	return g.message(ctx, device, "owner_fence", s.ownerEpoch, s.instance)
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
var _ ReaderGate = DMSetupGate{}
