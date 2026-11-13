package main

import (
	"bufio"
	"errors"
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
		Long:          "Manage BlockMesh Pools and Volumes. Lifecycle operations are executed by bm-metad.\n\nCommands:\n  pool create <name> <device> [--yes]\n  pool list\n  pool info <name>\n  pool destroy <name>\n  volume create <pool> <name> <size>\n  volume list\n  volume info <uuid>\n  volume delete <uuid>\n  volume activate <uuid> <rw|ro>\n  volume deactivate <uuid>",
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
		newPoolCreateCommand(),
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


func newPoolCreateCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use: "create name device",
		Short: "Create and initialize a Pool on a blank SAN device",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !yes {
				info, err := os.Stdin.Stat()
				if err != nil { return fmt.Errorf("inspect stdin for Pool confirmation: %w", err) }
				if info.Mode()&os.ModeCharDevice == 0 {
					return errors.New("Pool creation initializes on-disk metadata; use --yes for non-interactive execution")
				}
				fmt.Fprintf(os.Stderr, "This will initialize device %s as Pool %q. Type the Pool name to continue: ", args[1], args[0])
				line, err := bufio.NewReader(os.Stdin).ReadString('\n')
				if err != nil { return fmt.Errorf("read Pool creation confirmation: %w", err) }
				if strings.TrimSpace(line) != args[0] { return errors.New("Pool creation confirmation did not match the Pool name") }
			}
			socket, err := cmd.Root().PersistentFlags().GetString("socket")
			if err != nil { return err }
			return callLifecycle(socket, "poolcreate", lifecycleArgs{Name: args[0], Device: args[1], Confirmed: true})
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm initialization without an interactive prompt")
	return cmd
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
