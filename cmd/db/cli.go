package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"aieas_backend/internal/config"
)

const usage = "usage: go run ./cmd/db -config configs/config.yaml migrate up|down|status\n       go run ./cmd/db -config configs/config.yaml seed-dev"

type cliCommand struct {
	configPath       string
	name             string
	migrateDirection string
}

type configLoader func(path string) (config.Config, error)

type commandRunner interface {
	Migrate(ctx context.Context, cfg config.Config, direction string) error
	SeedDev(ctx context.Context, cfg config.Config) error
}

type cli struct {
	loadConfig configLoader
	runner     commandRunner
	stdout     io.Writer
	stderr     io.Writer
}

func newCLI(stdout, stderr io.Writer) *cli {
	return &cli{
		loadConfig: config.Load,
		runner:     dbRunner{},
		stdout:     stdout,
		stderr:     stderr,
	}
}

func (c *cli) run(ctx context.Context, args []string) error {
	cmd, err := parseArgs(args)
	if err != nil {
		return err
	}

	cfg, err := c.loadConfig(cmd.configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	switch cmd.name {
	case "migrate":
		if err := c.runner.Migrate(ctx, cfg, cmd.migrateDirection); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.stdout, "migrate %s complete\n", cmd.migrateDirection)
	case "seed-dev":
		if err := c.runner.SeedDev(ctx, cfg); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(c.stdout, "seed-dev complete")
	default:
		return usageError(fmt.Sprintf("unknown command %q", cmd.name))
	}
	return nil
}

func parseArgs(args []string) (cliCommand, error) {
	fs := flag.NewFlagSet("db", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", config.DefaultPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return cliCommand{}, usageError(err.Error())
	}

	rest := fs.Args()
	if len(rest) == 0 {
		return cliCommand{}, usageError("missing command")
	}

	cmd := cliCommand{configPath: *configPath, name: rest[0]}
	switch rest[0] {
	case "migrate":
		if len(rest) != 2 {
			return cliCommand{}, usageError("migrate requires exactly one direction: up, down, or status")
		}
		switch rest[1] {
		case "up", "down", "status":
			cmd.migrateDirection = rest[1]
			return cmd, nil
		default:
			return cliCommand{}, usageError(fmt.Sprintf("unsupported migrate direction %q", rest[1]))
		}
	case "seed-dev":
		if len(rest) != 1 {
			return cliCommand{}, usageError("seed-dev does not accept positional arguments")
		}
		return cmd, nil
	default:
		return cliCommand{}, usageError(fmt.Sprintf("unknown command %q", rest[0]))
	}
}

func usageError(message string) error {
	return fmt.Errorf("%s\n%s", message, usage)
}
