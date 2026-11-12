package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCobraRootHelp(t *testing.T) {
	root := newRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute help: %v", err)
	}
	for _, want := range []string{"pool create", "volume create", "--socket"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("help output missing %q: %s", want, out.String())
		}
	}
}

func TestCobraRejectsInvalidActivationMode(t *testing.T) {
	root := newRootCommand()
	root.SetArgs([]string{"volume", "activate", "vol-1", "readwrite"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "activation mode must be rw or ro") {
		t.Fatalf("expected activation mode validation error, got %v", err)
	}
}

func TestCobraAcceptsSocketAfterSubcommand(t *testing.T) {
	root := newRootCommand()
	root.SetArgs([]string{"volume", "list", "--socket", "/tmp/blockmesh.sock"})
	// The command reaches the RPC dial path; the expected error proves Cobra
	// parsed the persistent flag regardless of its position.
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "/tmp/blockmesh.sock") {
		t.Fatalf("expected socket dial error, got %v", err)
	}
}
