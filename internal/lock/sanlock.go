package lock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SanlockLocker uses sanlock's client command to hold a shared-storage
// resource lease for the lifetime of a helper process. The sanlock daemon
// acquires the resource before exec'ing the helper; the helper creates a
// readiness marker and then remains alive until Close terminates it.
//
// ResourceFor must return a sanlock RESOURCE string in the form
// "lockspace:resource_name:path:offset". The local host must already have
// joined that lockspace with sanlock client add_lockspace.
type SanlockLocker struct {
	Binary string
	ReadyDir string
	ResourceFor func(string) (string, error)
	StartupTimeout time.Duration
	StopTimeout time.Duration
}

type sanlockLease struct {
	cmd *exec.Cmd
	wait chan error
	marker string
	resource string
}

func (l *SanlockLocker) defaults() {
	if l.Binary == "" { l.Binary = "sanlock" }
	if l.ReadyDir == "" { l.ReadyDir = os.TempDir() }
	if l.StartupTimeout <= 0 { l.StartupTimeout = 30 * time.Second }
	if l.StopTimeout <= 0 { l.StopTimeout = 5 * time.Second }
}

func (l *SanlockLocker) Acquire(ctx context.Context, resource string) (Lease, error) {
	if l == nil || l.ResourceFor == nil { return nil, errors.New("sanlock resource resolver is required") }
	l.defaults()
	if strings.TrimSpace(resource) == "" { return nil, errors.New("empty lock resource") }
	spec, err := l.ResourceFor(resource)
	if err != nil { return nil, err }
	if strings.TrimSpace(spec) == "" || strings.ContainsAny(spec, "\r\n") {
		return nil, errors.New("invalid sanlock RESOURCE specification")
	}
	if err = os.MkdirAll(l.ReadyDir, 0700); err != nil { return nil, err }
	markerFile, err := os.CreateTemp(l.ReadyDir, "blockmesh-sanlock-*.ready")
	if err != nil { return nil, err }
	marker := markerFile.Name()
	if err = markerFile.Close(); err != nil { _ = os.Remove(marker); return nil, err }
	if err = os.Remove(marker); err != nil { return nil, err }

	script := "umask 077; : > " + shellQuote(marker) + "; exec sleep 2147483647"
	cmd := exec.Command(l.Binary, "client", "command", "-r", spec, "-c", "/bin/sh", "-c", script)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil { return nil, fmt.Errorf("start sanlock client: %w", err) }
	lease := &sanlockLease{cmd: cmd, wait: make(chan error, 1), marker: marker, resource: resource}
	go func() { lease.wait <- cmd.Wait() }()

	deadline := time.NewTimer(l.StartupTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, statErr := os.Stat(marker); statErr == nil {
			return lease, nil
		}
		select {
		case waitErr := <-lease.wait:
			_ = os.Remove(marker)
			return nil, fmt.Errorf("sanlock failed to acquire %q: %w", resource, waitErr)
		case <-ctx.Done():
			_ = lease.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			_ = lease.Close()
			return nil, fmt.Errorf("timed out waiting for sanlock resource %q", resource)
		case <-ticker.C:
		}
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func (l *SanlockLocker) Lock(ctx context.Context, resource string) (func() error, error) {
	lease, err := l.Acquire(ctx, resource)
	if err != nil { return nil, err }
	return lease.Close, nil
}

func (l *sanlockLease) Resource() string { return l.resource }
func (l *sanlockLease) Valid() bool {
	select {
	case <-l.wait:
		return false
	default:
		return true
	}
}
func (l *sanlockLease) Close() error {
	if l == nil || l.cmd == nil || l.cmd.Process == nil { return nil }
	select {
	case err := <-l.wait:
		_ = os.Remove(l.marker)
		if err != nil { return err }
		return nil
	default:
	}
	_ = l.cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-l.wait:
		_ = os.Remove(l.marker)
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) { return nil }
			return err
		}
		return nil
	case <-timer.C:
		_ = l.cmd.Process.Kill()
		err := <-l.wait
		_ = os.Remove(l.marker)
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) { return nil }
		}
		return err
	}
}

// SanlockResource formats the resource descriptor expected by sanlock.
// Names may not contain ':' because sanlock uses it as a field separator.
func SanlockResource(lockspace, name, path string, offset uint64) (string, error) {
	for _, value := range []string{lockspace, name, path} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, ":\r\n") {
			return "", errors.New("invalid sanlock resource component")
		}
	}
	return strings.Join([]string{lockspace, name, filepath.Clean(path), strconv.FormatUint(offset, 10)}, ":"), nil
}

var _ Locker = (*SanlockLocker)(nil)
