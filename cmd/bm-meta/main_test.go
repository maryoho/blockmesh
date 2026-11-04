package main

import (
	"strings"
	"testing"
)

func TestFormatRequiresExplicitConfirmation(t *testing.T) {
	err := runFormat([]string{
		"--device", "/dev/does-not-exist",
		"--volume-id", "volume-a",
		"--arena-pages", "100",
		"--lifecycle-start", "102",
		"--lifecycle-pages", "8",
	})
	if err == nil || !strings.Contains(err.Error(), "--confirm-format") {
		t.Fatalf("expected explicit format confirmation error, got %v", err)
	}
}

func TestFormatRequiresPersistentLifecycleRegion(t *testing.T) {
	err := runFormat([]string{
		"--device", "/dev/does-not-exist",
		"--volume-id", "volume-a",
		"--arena-pages", "100",
		"--confirm-format",
	})
	if err == nil || !strings.Contains(err.Error(), "--lifecycle-start") {
		t.Fatalf("expected lifecycle geometry error, got %v", err)
	}
}

func TestInspectRequiresDeviceAndVolumeIdentity(t *testing.T) {
	err := runInspect(nil)
	if err == nil || !strings.Contains(err.Error(), "--device and --volume-id") {
		t.Fatalf("expected inspect argument error, got %v", err)
	}
}
