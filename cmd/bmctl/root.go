package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const defaultControlSocket = "/run/blockmesh/control.sock"

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "bmctl",
		Short:         "BlockMesh storage lifecycle management",
		Long:          "Manage BlockMesh Pools and Volumes. Lifecycle operations are executed by bm-metad.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("socket", defaultControlSocket, "bm-metad control socket")
	root.AddCommand(newPoolCommand(), newVolumeCommand())
	return root
}

func newPoolCommand() *cobra.Command {
	pool := &cobra.Command{Use: "pool", Short: "Manage storage Pools"}
	pool.AddCommand(
		newLifecycleCommand("pool", "create", "name device", 2),
		newLifecycleCommand("pool", "list", "", 0),
		newLifecycleCommand("pool", "info", "name", 1),
		newLifecycleCommand("pool", "destroy", "name", 1),
	)
	for _, name := range []string{"format", "data-init", "dynamic-format", "dynamic-status", "dynamic-recover", "rescan", "metadata-expand", "metadata-expand-status", "data-expand"} {
		commandName := name
		pool.AddCommand(&cobra.Command{
			Use:                commandName + " [flags]",
			Short:              "Low-level Pool maintenance (advanced)",
			DisableFlagParsing: true,
			RunE: func(_ *cobra.Command, args []string) error {
				return runPool(append([]string{commandName}, args...))
			},
		})
	}
	return pool
}

func newVolumeCommand() *cobra.Command {
	volume := &cobra.Command{Use: "volume", Short: "Manage virtual Volumes"}
	volume.AddCommand(
		newLifecycleCommand("volume", "create", "pool name size", 3),
		newLifecycleCommand("volume", "list", "", 0),
		newLifecycleCommand("volume", "info", "uuid", 1),
		newLifecycleCommand("volume", "delete", "uuid", 1),
		newLifecycleCommand("volume", "activate", "uuid <rw|ro>", 2),
		newLifecycleCommand("volume", "deactivate", "uuid", 1),
	)
	return volume
}

func newLifecycleCommand(group, name, argsUsage string, argCount int) *cobra.Command {
	use := name
	if argsUsage != "" {
		use += " " + argsUsage
	}
	return &cobra.Command{
		Use:   use,
		Short: fmt.Sprintf("%s %s", strings.Title(group), name),
		Args:  cobra.ExactArgs(argCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			socket, err := cmd.Root().PersistentFlags().GetString("socket")
			if err != nil {
				return err
			}
			action, requestArgs, err := parseLifecycleCommand(append([]string{group, name}, args...))
			if err != nil {
				return err
			}
			return callLifecycle(socket, action, requestArgs)
		},
	}
}

func executeCLI(args []string) error {
	root := newRootCommand()
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		return err
	}
	return nil
}

func printCLIError(err error) {
	fmt.Fprintln(os.Stderr, "bmctl:", err)
}
