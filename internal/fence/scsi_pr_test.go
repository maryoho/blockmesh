package fence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

type fakePRRunner struct {
	keys map[string]map[uint64]bool
	failDevice string
}

func (r *fakePRRunner) Run(_ context.Context, _ string, args ...string) ([]byte, error) {
	device := ""
	var target uint64
	var reservation uint64
	preempt := false
	register := false
	for _, arg := range args {
		if strings.HasPrefix(arg, "--device=") { device = strings.TrimPrefix(arg, "--device=") }
		if strings.HasPrefix(arg, "--param-sark=") {
			target, _ = strconv.ParseUint(strings.TrimPrefix(arg, "--param-sark="), 16, 64)
		}
		if strings.HasPrefix(arg, "--param-rk=") {
			reservation, _ = strconv.ParseUint(strings.TrimPrefix(arg, "--param-rk="), 16, 64)
		}
		if arg == "--preempt-abort" { preempt = true }
		if arg == "--register" { register = true }
	}
	if device == r.failDevice { return []byte("path failure"), errors.New("injected path error") }
	if preempt {
		delete(r.keys[device], target)
		return []byte("preempted"), nil
	}
	if register {
		if target == 0 { delete(r.keys[device], reservation) } else { r.keys[device][target] = true }
		return []byte("registered"), nil
	}
	var output strings.Builder
	for key := range r.keys[device] { if r.keys[device][key] { fmt.Fprintf(&output, "Key=0x%x\n", key) } }
	return []byte(output.String()), nil
}

func TestSCSIPRFencerPreemptsAndVerifiesEveryPath(t *testing.T) {
	runner := &fakePRRunner{keys: map[string]map[uint64]bool{
		"/dev/mapper/mpatha": {0x11: true, 0x22: true},
		"/dev/mapper/mpathb": {0x11: true, 0x22: true},
	}}
	f := &SCSIPRFencer{
		Paths: []PRPath{{Device:"/dev/mapper/mpatha", LocalKey:0xaa}, {Device:"/dev/mapper/mpathb", LocalKey:0xbb}},
		Keys: RegistrationKeyFunc(func(context.Context,string,string)([]uint64,error){ return []uint64{0x22},nil }),
		Runner: runner,
	}
	if err := f.FenceNode(context.Background(),"node-b","boot-b"); err != nil { t.Fatal(err) }
	for device, keys := range runner.keys {
		if keys[0x22] { t.Fatalf("target key remains on %s", device) }
		if !keys[0x11] { t.Fatalf("unrelated key was removed from %s", device) }
	}
}

func TestSCSIPRFencerFailsClosedOnPathVerificationError(t *testing.T) {
	runner := &fakePRRunner{keys: map[string]map[uint64]bool{
		"/dev/a": {0x22:true},
		"/dev/b": {0x22:true},
	}, failDevice:"/dev/b"}
	f := &SCSIPRFencer{
		Paths: []PRPath{{Device:"/dev/a",LocalKey:0xaa},{Device:"/dev/b",LocalKey:0xbb}},
		Keys: RegistrationKeyFunc(func(context.Context,string,string)([]uint64,error){return []uint64{0x22},nil}),
		Runner:runner,
	}
	if err:=f.FenceNode(context.Background(),"node-b","boot-b");err==nil {t.Fatal("expected path error")}
}

func TestSCSIPRFencerRejectsUnknownTargetKey(t *testing.T) {
	f:=&SCSIPRFencer{Paths:[]PRPath{{Device:"/dev/a",LocalKey:1}},Keys:RegistrationKeyFunc(func(context.Context,string,string)([]uint64,error){return nil,nil}),Runner:&fakePRRunner{keys:map[string]map[uint64]bool{"/dev/a":{}}}}
	if err:=f.FenceNode(context.Background(),"node-b","boot-b");!errors.Is(err,ErrPRKeyUnknown){t.Fatalf("got %v",err)}
}

func TestParsePRKeysDoesNotTreatGenerationAsRegistration(t *testing.T) {
	keys:=parsePRKeys([]byte("PR generation=0x22\nKey=0x11\n0x33\n"))
	if _,ok:=keys[0x22];ok {t.Fatal("PR generation parsed as registration")}
	if _,ok:=keys[0x11];!ok {t.Fatal("Key= line not parsed")}
	if _,ok:=keys[0x33];!ok {t.Fatal("plain key line not parsed")}
}


func TestSCSIPRRegistrarRegistersAndUnregistersAllLocalPaths(t *testing.T) {
	runner:=&fakePRRunner{keys:map[string]map[uint64]bool{
		"/dev/a":{},
		"/dev/b":{},
	}}
	registrar:=&SCSIPRRegistrar{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner}
	if err:=registrar.RegisterReader(context.Background(),"node-a","boot-a");err!=nil{t.Fatal(err)}
	if !runner.keys["/dev/a"][0xa1]||!runner.keys["/dev/b"][0xb2]{t.Fatal("local keys were not registered")}
	if err:=registrar.UnregisterReader(context.Background(),"node-a","boot-a");err!=nil{t.Fatal(err)}
	if runner.keys["/dev/a"][0xa1]||runner.keys["/dev/b"][0xb2]{t.Fatal("local keys were not unregistered")}
}
