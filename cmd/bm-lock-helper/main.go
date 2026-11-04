package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	parentPID := flag.Int("parent-pid", 0, "expected BlockMesh parent PID")
	readyFile := flag.String("ready-file", "", "file created after sanlock lease acquisition")
	flag.Parse()
	if *parentPID <= 1 || *readyFile == "" {
		fmt.Fprintln(os.Stderr, "parent-pid and ready-file are required")
		os.Exit(2)
	}
	// PR_SET_PDEATHSIG: if the BlockMesh process exits, the kernel terminates
	// this sanlock command helper, causing sanlock to release its resource.
	_, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, 1, uintptr(syscall.SIGTERM), 0)
	if errno != 0 {
		fmt.Fprintln(os.Stderr, "prctl(PR_SET_PDEATHSIG):", errno)
		os.Exit(1)
	}
	// Close the race where the parent exits immediately before prctl().
	if os.Getppid() != *parentPID {
		os.Exit(1)
	}
	f, err := os.OpenFile(*readyFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create ready marker:", err)
		os.Exit(1)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		fmt.Fprintln(os.Stderr, "sync ready marker:", err)
		os.Exit(1)
	}
	if err = f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close ready marker:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	_ = os.Remove(*readyFile)
	_ = strconv.Itoa(*parentPID) // keep the parsed identity explicit in diagnostics/builds
}
