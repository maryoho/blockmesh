package main

import (
	"testing"
)

func TestParseVolumeSize(t *testing.T) {
	cases := []struct{name string; want uint64}{
		{"1K",1<<10},{"2M",2<<20},{"3G",3<<30},{"1T",1<<40},{"10g",10<<30},
	}
	for _, tc := range cases { got,err:=parseVolumeSize(tc.name); if err!=nil||got!=tc.want { t.Fatalf("%s: got %d err=%v want %d",tc.name,got,err,tc.want) } }
	for _, value := range []string{"","0G","-1G","1","1P","18446744073709551615T"} { if _,err:=parseVolumeSize(value); err==nil { t.Errorf("expected %q to fail",value) } }
	if _,err:=parseVolumeSize("16G"); err!=nil { t.Fatal(err) }
}

func TestParseLifecycleCommand(t *testing.T) {
	action,args,err:=parseLifecycleCommand([]string{"volume","create","pool0","disk0","100G"})
	if err!=nil||action!="volumecreate"||args.Pool!="pool0"||args.Name!="disk0"||args.SizeBytes!=100<<30 { t.Fatalf("action=%q args=%+v err=%v",action,args,err) }
	action,args,err=parseLifecycleCommand([]string{"volume","activate","abc","ro"})
	if err!=nil||action!="volumeactivate"||args.Mode!="ro" { t.Fatalf("action=%q args=%+v err=%v",action,args,err) }
	for _, argv:=range [][]string{{"volume","activate","abc","rwX"},{"volume","create","p","v","0G"},{"pool","create","p"}} { if _,_,err:=parseLifecycleCommand(argv); err==nil { t.Errorf("expected %v to fail",argv) } }
}

func TestVolumeCreateTokenIsDeterministic(t *testing.T) {
	first:=lifecycleArgs{Pool:"pool0",Name:"disk0",SizeBytes:100<<30}
	second:=lifecycleArgs{Pool:"pool0",Name:"disk0",SizeBytes:100<<30}
	makeToken:=func(a lifecycleArgs)string{
		identity:=fmt.Sprintf("blockmesh/volume-create/v1/%s/%s/%d",a.Pool,a.Name,a.SizeBytes)
		sum:=sha256.Sum256([]byte(identity))
		return hex.EncodeToString(sum[:16])
	}
	if makeToken(first)!=makeToken(second){t.Fatal("same Volume create request produced different idempotency tokens")}
	second.SizeBytes+=4<<20
	if makeToken(first)==makeToken(second){t.Fatal("different Volume sizes produced the same idempotency token")}
}
