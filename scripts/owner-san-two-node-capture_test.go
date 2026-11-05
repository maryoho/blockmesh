package scripts_test

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOwnerSANTwoNodeCaptureScriptSyntax(t *testing.T) {
	script:=filepath.Join("owner-san-two-node-capture.sh")
	if out,err:=exec.Command("bash","-n",script).CombinedOutput();err!=nil {
		t.Fatalf("bash -n %s: %v\n%s",script,err,out)
	}
}
