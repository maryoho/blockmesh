package fence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

type ownerReservationRunner struct {
	keys map[string]map[uint64]bool
	holder uint64
	prType uint8
	failReadReservation string
	reserveErr bool
}

func (r *ownerReservationRunner) Run(_ context.Context, _ string, args ...string) ([]byte,error) {
	device := ""
	var rk, sark uint64
	var typ uint64
	register, reserve, release, readKeys, readReservation := false,false,false,false,false
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg,"--device="): device=strings.TrimPrefix(arg,"--device=")
		case strings.HasPrefix(arg,"--param-rk="): rk,_=strconv.ParseUint(strings.TrimPrefix(arg,"--param-rk="),16,64)
		case strings.HasPrefix(arg,"--param-sark="): sark,_=strconv.ParseUint(strings.TrimPrefix(arg,"--param-sark="),16,64)
		case strings.HasPrefix(arg,"--prout-type="): typ,_=strconv.ParseUint(strings.TrimPrefix(arg,"--prout-type="),10,8)
		case arg=="--register": register=true
		case arg=="--reserve": reserve=true
		case arg=="--release": release=true
		case arg=="--read-keys": readKeys=true
		case arg=="--read-reservation": readReservation=true
		}
	}
	if r.keys[device]==nil { r.keys[device]=make(map[uint64]bool) }
	switch {
	case readKeys:
		var b strings.Builder
		b.WriteString("PR generation=0x1\n")
		for key, present := range r.keys[device] { if present { fmt.Fprintf(&b,"Key=0x%x\n",key) } }
		return []byte(b.String()),nil
	case register:
		if sark==0 { delete(r.keys[device],rk) } else { r.keys[device][sark]=true }
		return []byte("registered"),nil
	case reserve:
		r.holder=rk; r.prType=uint8(typ)
		if r.reserveErr { return []byte("response lost after target commit"),errors.New("injected transport timeout") }
		return []byte("reserved"),nil
	case release:
		if r.holder==rk { r.holder=0; r.prType=0 }
		return []byte("released"),nil
	case readReservation:
		if device==r.failReadReservation { return []byte("path unavailable"),errors.New("injected read error") }
		if r.holder==0 { return []byte("No reservation held"),nil }
		name:=map[uint8]string{1:"Write Exclusive",3:"Exclusive Access",5:"Write Exclusive, registrants only",6:"Exclusive Access, registrants only"}[r.prType]
		return []byte(fmt.Sprintf("PR reservation:\n  Key=0x%x Scope: LU_SCOPE, Type: %s\n",r.holder,name)),nil
	default:
		return nil,fmt.Errorf("unexpected command: %v",args)
	}
}

func TestSCSIPROwnerReservationRegistersReservesAndVerifiesEveryPath(t *testing.T) {
	runner:=&ownerReservationRunner{keys:map[string]map[uint64]bool{}}
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner,ReservationType:6}
	if err:=manager.PrepareOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.EstablishOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if !runner.keys["/dev/a"][0xa1] || !runner.keys["/dev/b"][0xb2] { t.Fatal("not all path-scoped keys registered") }
	if runner.holder!=0xa1 || runner.prType!=6 { t.Fatalf("reservation holder=%x type=%d",runner.holder,runner.prType) }
}

func TestSCSIPROwnerReservationAcceptsAmbiguousReserveWhenAllPathsVerify(t *testing.T) {
	runner:=&ownerReservationRunner{keys:map[string]map[uint64]bool{},reserveErr:true}
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner,ReservationType:6}
	if err:=manager.PrepareOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.EstablishOwnerReservation(context.Background(),"node-a","boot-a");err!=nil {
		t.Fatalf("durably committed reservation was not recognized after lost response: %v",err)
	}
}

func TestSCSIPROwnerReservationFailsClosedOnAnyPathReadFailure(t *testing.T) {
	runner:=&ownerReservationRunner{keys:map[string]map[uint64]bool{},failReadReservation:"/dev/b"}
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner,ReservationType:6}
	if err:=manager.PrepareOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.EstablishOwnerReservation(context.Background(),"node-a","boot-a");err==nil { t.Fatal("reservation accepted with unreadable component path") }
	// Do not unregister after an uncertain reservation: the next Owner must
	// fence this identity using its durable OwnerRecord.
	if !runner.keys["/dev/a"][0xa1] || !runner.keys["/dev/b"][0xb2] { t.Fatal("ambiguous reservation failure unexpectedly removed PR registrations") }
}

func TestSCSIPROwnerReservationRejectsAllRegistrantsTypes(t *testing.T) {
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1}},Runner:&ownerReservationRunner{keys:map[string]map[uint64]bool{}},ReservationType:8}
	if err:=manager.EstablishOwnerReservation(context.Background(),"node-a","boot-a");err==nil { t.Fatal("All Registrants reservation accepted despite singular holder verification") }
}

func TestParseLiveReservation(t *testing.T) {
	key,typ,ok:=parseLiveReservation("PR reservation:\n  Key=0x0123456789abcdef Scope: LU_SCOPE, Type: Exclusive Access, registrants only\n")
	if !ok || key!=0x0123456789abcdef || typ!=6 { t.Fatalf("parsed key=%x type=%d ok=%v",key,typ,ok) }
	for _,output:=range []string{
		"No reservation held",
		"PR reservation:\n Key=0x1 Scope: LU_SCOPE, Type: vendor specific\n",
		"PR reservation:\n Key=0x1 Key=0x2 Scope: LU_SCOPE, Type: Exclusive Access\n",
	} {
		if _,_,ok:=parseLiveReservation(output);ok { t.Fatalf("accepted ambiguous reservation output %q",output) }
	}
}


func TestPoolBootstrapReservationRejectsForeignRegistrations(t *testing.T) {
	runner:=&ownerReservationRunner{keys:map[string]map[uint64]bool{
		"/dev/a":{0xa1:true,0xdead:true},
		"/dev/b":{0xb2:true},
	}}
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner,ReservationType:6}
	if err:=manager.VerifyNoForeignRegistrations(context.Background());err==nil||!strings.Contains(err.Error(),"foreign") {
		t.Fatalf("expected foreign registration rejection, got %v",err)
	}
}

func TestPoolBootstrapReservationCanResumeAndReleaseSameInitiator(t *testing.T) {
	runner:=&ownerReservationRunner{keys:map[string]map[uint64]bool{}}
	manager:=&SCSIPROwnerReservation{Paths:[]PRPath{{Device:"/dev/a",LocalKey:0xa1},{Device:"/dev/b",LocalKey:0xb2}},Runner:runner,ReservationType:6}
	if err:=manager.VerifyNoForeignRegistrations(context.Background());err!=nil { t.Fatal(err) }
	if err:=manager.PrepareOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.EstablishOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.VerifyNoForeignRegistrations(context.Background());err!=nil { t.Fatalf("same initiator should be resumable: %v",err) }
	if err:=manager.VerifyOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if err:=manager.ReleaseOwnerReservation(context.Background(),"node-a","boot-a");err!=nil { t.Fatal(err) }
	if runner.holder!=0 || runner.keys["/dev/a"][0xa1] || runner.keys["/dev/b"][0xb2] { t.Fatal("Pool bootstrap PR reservation or registrations were not released") }
}
