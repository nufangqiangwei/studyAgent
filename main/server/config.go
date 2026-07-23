package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type serverConfig struct {
	Address           string
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

func parseServerConfig(args []string, stderr io.Writer, getenv func(string) string) (serverConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	config := serverConfig{}
	flags := flag.NewFlagSet("agent-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.Address, "address", envOrDefault(getenv, "AGENT_SERVER_ADDRESS", "127.0.0.1:8080"), "HTTP listen address")
	flags.DurationVar(&config.ReadHeaderTimeout, "read-header-timeout", 5*time.Second, "maximum time to read request headers")
	flags.DurationVar(&config.IdleTimeout, "idle-timeout", 60*time.Second, "HTTP keep-alive idle timeout")
	flags.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: go run ./main/server [options]")
		fmt.Fprintln(stderr, "\nThis entry exposes the Web API boundary; Runtime integration is not connected yet.")
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if flags.NArg() != 0 {
		return serverConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	config.Address = strings.TrimSpace(config.Address)
	if config.Address == "" {
		return serverConfig{}, fmt.Errorf("HTTP listen address is required")
	}
	if config.ReadHeaderTimeout <= 0 || config.IdleTimeout <= 0 || config.ShutdownTimeout <= 0 {
		return serverConfig{}, fmt.Errorf("server timeouts must be positive")
	}
	return config, nil
}

func envOrDefault(getenv func(string) string, key, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}
