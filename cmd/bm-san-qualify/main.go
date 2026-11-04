package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"blockmesh/internal/sanqual"
)

func main(){
	if err:=run(os.Args[1:]);err!=nil{fmt.Fprintln(os.Stderr,"bm-san-qualify:",err);os.Exit(1)}
}
func run(args []string)error{
	if len(args)==0{return errors.New("usage: bm-san-qualify collect|verify-fenced")}
	switch args[0]{
	case "collect":return collect(args[1:])
	case "verify-fenced":return verifyFenced(args[1:])
	default:return fmt.Errorf("unknown command %q (expected collect or verify-fenced)",args[0])
	}
}
func collect(args []string)error{
	fs:=flag.NewFlagSet("collect",flag.ContinueOnError)
	device:=fs.String("device","","multipath map, e.g. /dev/mapper/mpatha")
	pathsText:=fs.String("paths","","comma-separated component block paths; every path must be listed")
	volume:=fs.String("volume-id","","BlockMesh Volume ID")
	node:=fs.String("node-id","","local node identity")
	boot:=fs.String("boot-id","","local boot identity")
	out:=fs.String("output","","evidence JSON path (default stdout)")
	timeout:=fs.Duration("timeout",10*time.Second,"per-command timeout")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0{return errors.New("unexpected positional arguments")}
	if strings.TrimSpace(*device)==""||strings.TrimSpace(*pathsText)==""{return errors.New("--device and explicit --paths are required")}
	paths:=strings.Split(*pathsText,",")
	e,err:=(sanqual.Collector{Timeout:*timeout}).Collect(context.Background(),*device,*volume,*node,*boot,paths)
	if err!=nil{return err}
	var dst *os.File
	if *out==""{dst=os.Stdout}else{
		dst,err=os.OpenFile(*out,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
		if err!=nil{return fmt.Errorf("create evidence file (existing files are never overwritten): %w",err)}
		defer dst.Close()
	}
	enc:=json.NewEncoder(dst);enc.SetIndent("","  ")
	if err=enc.Encode(e);err!=nil{return err}
	if *out!=""{fmt.Printf("evidence written to %s\n",*out)}
	return nil
}
func verifyFenced(args []string)error{
	fs:=flag.NewFlagSet("verify-fenced",flag.ContinueOnError)
	input:=fs.String("evidence","","evidence JSON file created by collect")
	key:=fs.String("target-key","","old Owner PR registration key (hex)")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0{return errors.New("unexpected positional arguments")}
	if *input==""||*key==""{return errors.New("--evidence and --target-key are required")}
	raw,err:=os.ReadFile(*input);if err!=nil{return err}
	var e sanqual.Evidence
	if err=json.Unmarshal(raw,&e);err!=nil{return fmt.Errorf("decode evidence: %w",err)}
	result,err:=sanqual.VerifyTargetKeyAbsent(e,*key);if err!=nil{return err}
	if err=json.NewEncoder(os.Stdout).Encode(result);err!=nil{return err}
	if !result.AbsentOnAllPaths{return errors.New("target PR key is present or unreadable on one or more paths; fencing is NOT verified")}
	fmt.Fprintln(os.Stderr,"PR registration absence verified on every listed path; this alone is not proof of old-node I/O drain or RW safety")
	return nil
}
