package dmowner

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type tableRunner struct {
	table string
	status string
	readOnly string
	failCreate bool
	calls [][]string
}
func (r *tableRunner) Run(_ context.Context,_ string,args ...string)([]byte,error){
	r.calls=append(r.calls,append([]string(nil),args...))
	switch args[0] {
	case "table":
		if r.table=="" { return nil,errors.New("not found") }
		return []byte(r.table),nil
	case "status":
		return []byte(r.status),nil
	case "info":
		return []byte(r.readOnly),nil
	case "create":
		if r.failCreate{return nil,errors.New("response lost")}
		for i:=range args { if args[i]=="--table"&&i+1<len(args){r.table=args[i+1]} }
		r.status="0 8192 blockmesh instance=0000000000000001 epoch=0 gate=0 fenced=1 inflight=0 reader_gate=0"
		for _,arg:=range args { if arg=="--readonly" { r.readOnly="1" } }
		return nil,nil
	case "remove":
		r.table=""
		return nil,nil
	default:return nil,errors.New("unexpected command")
	}
}

func validTableSpec() TableSpec {
	return TableSpec{VolumeID:"01234567-89ab-cdef-0123-456789abcdef",LogicalSizeBytes:4<<20,
		BackingDevice:"/dev/mapper/mpatha",NodeID:"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		BootID:"11111111-2222-3333-4444-555555555555"}
}

func TestTableManagerCreatesClosedFencedTarget(t *testing.T) {
	r:=&tableRunner{}
	m:=TableManager{Runner:r}
	name,err:=m.Ensure(context.Background(),validTableSpec())
	if err!=nil{t.Fatal(err)}
	if name!="bm-0123456789abcdef0123456789abcdef"{t.Fatalf("unexpected name %s",name)}
	if len(r.calls)==0||!reflect.DeepEqual(r.calls[0][:2],[]string{"table",name}){t.Fatalf("unexpected initial lookup: %#v",r.calls)}
	create:=r.calls[1]
	if !reflect.DeepEqual(create[:2],[]string{"create",name})||create[len(create)-2]!="--table"{t.Fatalf("unexpected create args: %#v",create)}
	if !strings.Contains(create[len(create)-1],"blockmesh /dev/mapper/mpatha 0 "){t.Fatalf("unexpected DM table: %s",create[len(create)-1])}
}

func TestTableManagerReadOnlyAndAmbiguousCreate(t *testing.T) {
	r:=&tableRunner{failCreate:true}
	m:=TableManager{Runner:r}
	spec:=validTableSpec();spec.ReadOnly=true
	// A lost create response without a visible table must fail.
	if _,err:=m.Ensure(context.Background(),spec);err==nil{t.Fatal("ambiguous create without visible target succeeded")}
	r.failCreate=false
	if _,err:=m.Ensure(context.Background(),spec);err!=nil{t.Fatal(err)}
	if r.readOnly!="1"{t.Fatalf("DM table is not read-only: %q",r.readOnly)}
}

func TestTableManagerRemoveRequiresFencedDrainedTarget(t *testing.T) {
	r:=&tableRunner{table:"table",status:"instance=0000000000000001 gate=1 fenced=0 inflight=2 reader_gate=0"}
	m:=TableManager{Runner:r}
	if err:=m.Remove(context.Background(),validTableSpec().VolumeID);err==nil{t.Fatal("removed open/inflight DM target")}
	r.status="instance=0000000000000001 gate=0 fenced=1 inflight=0 reader_gate=0"
	if err:=m.Remove(context.Background(),validTableSpec().VolumeID);err!=nil{t.Fatal(err)}
	if r.table!=""{t.Fatal("DM table remains after remove")}
}

func TestDMTableHashesStableNodeAndBootIdentities(t *testing.T) {
	m:=TableManager{}
	spec:=validTableSpec();spec.NodeID="node-a";spec.BootID="boot-a"
	_,line,_,err:=m.table(spec);if err!=nil{t.Fatal(err)}
	if !strings.Contains(line,"blockmesh /dev/mapper/mpatha 0 "){t.Fatalf("invalid DM table: %s",line)}
	_,again,_,err:=m.table(spec);if err!=nil||again!=line{t.Fatalf("identity mapping is not stable: %v",err)}
	spec.LogicalSizeBytes=513
	if _,_,_,err=m.table(spec);err==nil{t.Fatal("unaligned logical size accepted")}
}
