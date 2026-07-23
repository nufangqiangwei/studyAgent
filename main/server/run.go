package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type runOptions struct {
	runtime RuntimePort
	listen  func(string, string) (net.Listener, error)
	getenv  func(string) string
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithOptions(ctx, args, stdout, stderr, runOptions{})
}

func runWithOptions(ctx context.Context, args []string, stdout, stderr io.Writer, options runOptions) error {
	config, err := parseServerConfig(args, stderr, options.getenv)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	listen := options.listen
	if listen == nil {
		listen = net.Listen
	}
	listener, err := listen("tcp", config.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.Address, err)
	}
	defer listener.Close()

	server := &http.Server{
		Handler:           newAPIHandler(options.runtime),
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		IdleTimeout:       config.IdleTimeout,
	}
	if _, err := fmt.Fprintf(stdout, "Agent Web Server listening on http://%s\n", listener.Addr()); err != nil {
		return fmt.Errorf("write server address: %w", err)
	}

	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		select {
		case err := <-serveErrors:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve HTTP: %w", err)
			}
		case <-time.After(config.ShutdownTimeout):
			return fmt.Errorf("HTTP server did not stop after shutdown")
		}
		return nil
	}
}
