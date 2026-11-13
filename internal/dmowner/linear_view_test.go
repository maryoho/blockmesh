package dmowner

import (
	"context"
	"errors"
	"testing"
)

type linearRunner struct { table string; calls [][]string }
func(r *linearRunner)Run(_ context.Context,_ string,args ...string)([]byte,error){
	r.calls=append(r.calls,append([]string(nil),args...))
	switch args[0] {
	case "table": if r.table==""{return nil,errors.New("not found")};return []byte(r.table),nil
	case "create":
		for i:=range args{if args[i]=="--table"&&i+1<len(args){r.table=args[i+1]}}
		return nil,nil
	default:return nil,errors.New("unexpected command")
	}
}
func TestLinearViewManagerMapsFragmentedDataExtents(t *testing.T){
	r:=&linearRunner{}
	m:=LinearViewManager{Runner:r}
	name,err:=m.Ensure(context.Background(),"0123456789abcdef0123456789abcdef","/dev/mapper/mpatha",
		[]LinearExtent{{StartSector:4096,Sectors:8192},{StartSector:32768,Sectors:4096}})
	if err!=nil{t.Fatal(err)}
	if name!="/dev/mapper/bmd-0123456789abcdef0123456789abcdef"{t.Fatalf("unexpected DataView name %s",name)}
	want:="0 8192 linear /dev/mapper/mpatha 4096\n8192 4096 linear /dev/mapper/mpatha 32768"
	if r.table!=want{t.Fatalf("linear table mismatch:\n%s\nwant:\n%s",r.table,want)}
	if _,err=m.Ensure(context.Background(),"0123456789abcdef0123456789abcdef","/dev/mapper/mpatha",
		[]LinearExtent{{StartSector:4096,Sectors:8192},{StartSector:32768,Sectors:4096}});err!=nil{t.Fatalf("idempotent Ensure: %v",err)}
}
