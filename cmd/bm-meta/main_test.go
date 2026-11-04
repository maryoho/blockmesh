package main

import (
	"strings"
	"testing"
)

func TestFormatRequiresExplicitConfirmation(t *testing.T) {
	err:=runFormat([]string{"--device","/dev/does-not-exist"})
	if err==nil || !strings.Contains(err.Error(),"--confirm-format") { t.Fatalf("expected explicit Pool format confirmation error, got %v",err) }
}

func TestFormatRequiresDevice(t *testing.T) {
	err:=runFormat([]string{"--confirm-format"})
	if err==nil || !strings.Contains(err.Error(),"--device") { t.Fatalf("expected device argument error, got %v",err) }
}

func TestInspectRequiresDevice(t *testing.T) {
	err:=runInspect(nil)
	if err==nil || !strings.Contains(err.Error(),"--device") { t.Fatalf("expected inspect device argument error, got %v",err) }
}
