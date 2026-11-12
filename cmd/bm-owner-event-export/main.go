package main

import (
	"flag"
	"fmt"
	"os"

	"blockmesh/internal/runtimeevents"
)

func main(){if err:=run(os.Args[1:]);err!=nil{fmt.Fprintln(os.Stderr,"bm-owner-event-export:",err);os.Exit(1)}}
func run(args []string)error{
	fs:=flag.NewFlagSet("bm-owner-event-export",flag.ContinueOnError)
	input:=fs.String("input","","hash-chained runtime event journal")
	output:=fs.String("output","","exclusive JSON export path")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0 || *input=="" || *output==""{return fmt.Errorf("--input and --output are required; no positional arguments are accepted")}
	records,err:=runtimeevents.Read(*input);if err!=nil{return err}
	export,err:=runtimeevents.VerifyRecovery(records);if err!=nil{return err}
	if err=runtimeevents.WriteExport(*output,export);err!=nil{return err}
	fmt.Fprintf(os.Stderr,"PASS: exported %d verified runtime events for Volume %s OwnerEpoch %d\n",len(records),export.VolumeID,export.OwnerEpoch)
	return nil
}
