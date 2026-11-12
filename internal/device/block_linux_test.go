//go:build linux

package device

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenBlockDeviceRejectsRegularFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-block-device")
	if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := OpenBlockDevice(path, true, true)
	if !errors.Is(err, ErrNotBlockDevice) {
		t.Fatalf("expected ErrNotBlockDevice, got %v", err)
	}
}

func TestOpenBlockDeviceRejectsMissingPath(t *testing.T) {
	_, err := OpenBlockDevice(filepath.Join(t.TempDir(), "missing"), false, false)
	if err == nil {
		t.Fatal("expected stat error")
	}
}
