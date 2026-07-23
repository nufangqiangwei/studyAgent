package main

import (
	serviceruntime "agent/serviceruntime"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type runApplication interface {
	RuntimePort() RuntimePort
	Start(context.Context) error
	Serve(context.Context) error
	Drain(context.Context) error
	Live() bool
	MarkUnavailable() error
	Close() error
}

type runHTTPServer interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
	Close() error
}

type runOptions struct {
	runtime          RuntimePort
	buildApplication func(context.Context, serverConfig) (runApplication, error)
	listen           func(string, string) (net.Listener, error)
	newServer        func(serverConfig, http.Handler) runHTTPServer
	getenv           func(string) string
}

type productionRunApplication struct {
	application *application
}

func (a *productionRunApplication) RuntimePort() RuntimePort {
	if a == nil || a.application == nil {
		return nil
	}
	return a.application.runtimePort
}

func (a *productionRunApplication) Start(ctx context.Context) error {
	if a == nil || a.application == nil || a.application.runtime == nil {
		return fmt.Errorf("Web Runtime application is unavailable")
	}
	_, err := a.application.runtime.Start(ctx)
	return err
}

func (a *productionRunApplication) Serve(ctx context.Context) error {
	if a == nil || a.application == nil || a.application.runtime == nil {
		return fmt.Errorf("Web Runtime application is unavailable")
	}
	return a.application.runtime.Serve(ctx)
}

func (a *productionRunApplication) Drain(ctx context.Context) error {
	if a == nil || a.application == nil || a.application.runtime == nil {
		return nil
	}
	return a.application.runtime.Drain(ctx)
}

func (a *productionRunApplication) Live() bool {
	return a != nil &&
		a.application != nil &&
		a.application.runtime != nil &&
		a.application.runtime.Status() == serviceruntime.RuntimeLive
}

func (a *productionRunApplication) MarkUnavailable() error {
	if a == nil || a.application == nil || a.application.adapter == nil {
		return nil
	}
	return a.application.adapter.Close()
}

func (a *productionRunApplication) Close() error {
	if a == nil || a.application == nil {
		return nil
	}
	return a.application.Close()
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

	build := options.buildApplication
	if build == nil {
		build = func(ctx context.Context, config serverConfig) (runApplication, error) {
			built, buildErr := buildApplication(ctx, config, applicationOptions{})
			if built == nil {
				return nil, buildErr
			}
			return &productionRunApplication{application: built}, buildErr
		}
	}
	app, err := build(ctx, config)
	if err != nil {
		var closeErr error
		if app != nil {
			closeErr = app.Close()
		}
		return joinRunErrors(
			fmt.Errorf("build Web Runtime application: %w", err),
			wrapRunError("close Web Runtime application", closeErr),
		)
	}
	if app == nil {
		return fmt.Errorf("build Web Runtime application: builder returned no application")
	}

	var closeOnce sync.Once
	var closeErr error
	closeApplication := func() error {
		closeOnce.Do(func() {
			closeErr = app.Close()
		})
		return closeErr
	}

	if err := app.Start(ctx); err != nil {
		return joinRunErrors(
			fmt.Errorf("start Web Runtime: %w", err),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}
	if !app.Live() {
		return joinRunErrors(
			fmt.Errorf("start Web Runtime: Runtime did not become live"),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}

	runtimeContext, cancelRuntime := context.WithCancel(context.Background())
	runtimeErrors := make(chan error, 1)
	runtimeStarted := make(chan struct{})
	go func() {
		close(runtimeStarted)
		runtimeErrors <- app.Serve(runtimeContext)
	}()
	<-runtimeStarted
	select {
	case serveErr := <-runtimeErrors:
		cancelRuntime()
		return joinRunErrors(
			runtimeServeError(serveErr),
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	default:
	}
	if !app.Live() {
		cancelRuntime()
		serveErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		return joinRunErrors(
			fmt.Errorf("start Web Runtime Serve: Runtime is not live"),
			runtimeServeResultError(serveErr),
			waitErr,
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}

	listen := options.listen
	if listen == nil {
		listen = net.Listen
	}
	listener, err := listen("tcp", config.Address)
	if err != nil {
		cancelRuntime()
		serveErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		return joinRunErrors(
			fmt.Errorf("listen on %s: %w", config.Address, err),
			runtimeServeResultError(serveErr),
			waitErr,
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}

	port := options.runtime
	if port == nil {
		port = app.RuntimePort()
	}
	newServer := options.newServer
	if newServer == nil {
		newServer = func(config serverConfig, handler http.Handler) runHTTPServer {
			return &http.Server{
				Handler:           handler,
				ReadHeaderTimeout: config.ReadHeaderTimeout,
				IdleTimeout:       config.IdleTimeout,
			}
		}
	}
	server := newServer(config, newAPIHandler(port))
	if server == nil {
		cancelRuntime()
		serveErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		return joinRunErrors(
			fmt.Errorf("create HTTP server: server factory returned nil"),
			wrapRunError("close HTTP listener", listener.Close()),
			runtimeServeResultError(serveErr),
			waitErr,
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}

	select {
	case serveErr := <-runtimeErrors:
		cancelRuntime()
		return joinRunErrors(
			runtimeServeError(serveErr),
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
			wrapRunError("close HTTP listener", listener.Close()),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	default:
	}
	if !app.Live() {
		cancelRuntime()
		serveErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		return joinRunErrors(
			fmt.Errorf("start HTTP server: Runtime is not live"),
			wrapRunError("close HTTP listener", listener.Close()),
			runtimeServeResultError(serveErr),
			waitErr,
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}
	if _, err := fmt.Fprintf(stdout, "Agent Web Server listening on http://%s\n", listener.Addr()); err != nil {
		cancelRuntime()
		serveErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		return joinRunErrors(
			fmt.Errorf("write server address: %w", err),
			wrapRunError("close HTTP listener", listener.Close()),
			runtimeServeResultError(serveErr),
			waitErr,
			wrapRunError("close Web Runtime application", closeApplication()),
		)
	}

	httpErrors := make(chan error, 1)
	go func() {
		httpErrors <- server.Serve(listener)
	}()

	select {
	case runtimeErr := <-runtimeErrors:
		cancelRuntime()
		result := []error{
			runtimeServeError(runtimeErr),
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
		}
		result = append(result, shutDownHTTP(server, httpErrors, config.ShutdownTimeout)...)
		result = append(result, wrapRunError("close Web Runtime application", closeApplication()))
		return joinRunErrors(result...)

	case httpErr := <-httpErrors:
		result := []error{
			httpServeError(httpErr),
			wrapRunError("mark Runtime adapter unavailable", app.MarkUnavailable()),
		}
		result = append(result, drainRuntime(app, config.ShutdownTimeout)...)
		cancelRuntime()
		runtimeErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		result = append(result, runtimeServeResultError(runtimeErr), waitErr)
		if waitErr != nil {
			result = append(result, wrapRunError("close Web Runtime application", closeApplication()))
			runtimeErr, secondWaitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve after close")
			result = append(result, runtimeServeResultError(runtimeErr), secondWaitErr)
		}
		result = append(result, wrapRunError("close Web Runtime application", closeApplication()))
		return joinRunErrors(result...)

	case <-ctx.Done():
		var result []error
		result = append(result, shutDownHTTP(server, httpErrors, config.ShutdownTimeout)...)
		result = append(result, drainRuntime(app, config.ShutdownTimeout)...)
		cancelRuntime()
		runtimeErr, waitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve")
		result = append(result, runtimeServeResultError(runtimeErr), waitErr)
		if waitErr != nil {
			result = append(result, wrapRunError("close Web Runtime application", closeApplication()))
			runtimeErr, secondWaitErr := waitRunResult(runtimeErrors, config.ShutdownTimeout, "Runtime Serve after close")
			result = append(result, runtimeServeResultError(runtimeErr), secondWaitErr)
		}
		result = append(result, wrapRunError("close Web Runtime application", closeApplication()))
		return joinRunErrors(result...)
	}
}

func shutDownHTTP(server runHTTPServer, serveErrors <-chan error, timeout time.Duration) []error {
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	shutdownErr := server.Shutdown(shutdownContext)
	cancel()

	var result []error
	if shutdownErr != nil {
		result = append(result, fmt.Errorf("shut down HTTP server: %w", shutdownErr))
		if err := server.Close(); err != nil {
			result = append(result, fmt.Errorf("close HTTP server after shutdown failure: %w", err))
		}
	}
	serveErr, waitErr := waitRunResult(serveErrors, timeout, "HTTP Serve")
	if waitErr != nil && shutdownErr == nil {
		result = append(result, waitErr)
		if err := server.Close(); err != nil {
			result = append(result, fmt.Errorf("close HTTP server after shutdown timeout: %w", err))
		}
		serveErr, waitErr = waitRunResult(serveErrors, timeout, "HTTP Serve after close")
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		result = append(result, fmt.Errorf("serve HTTP during shutdown: %w", serveErr))
	}
	if waitErr != nil {
		result = append(result, waitErr)
	}
	return result
}

func drainRuntime(app runApplication, timeout time.Duration) []error {
	drainContext, cancel := context.WithTimeout(context.Background(), timeout)
	err := app.Drain(drainContext)
	cancel()
	if err == nil {
		return nil
	}
	return []error{fmt.Errorf("drain Web Runtime: %w", err)}
}

func waitRunResult(results <-chan error, timeout time.Duration, operation string) (error, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-results:
		return err, nil
	case <-timer.C:
		return nil, fmt.Errorf("%s did not stop within %s", operation, timeout)
	}
}

func runtimeServeError(err error) error {
	if err == nil {
		return fmt.Errorf("serve Web Runtime: stopped unexpectedly")
	}
	return fmt.Errorf("serve Web Runtime: %w", err)
}

func runtimeServeResultError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("serve Web Runtime during shutdown: %w", err)
}

func httpServeError(err error) error {
	if err == nil {
		return fmt.Errorf("serve HTTP: stopped unexpectedly")
	}
	return fmt.Errorf("serve HTTP: %w", err)
}

func wrapRunError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

type joinedRunError struct {
	errors []error
}

func joinRunErrors(values ...error) error {
	filtered := make([]error, 0, len(values))
	for _, err := range values {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		return &joinedRunError{errors: filtered}
	}
}

func (e *joinedRunError) Error() string {
	if e == nil {
		return ""
	}
	messages := make([]string, 0, len(e.errors))
	for _, err := range e.errors {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "\n")
}

func (e *joinedRunError) Unwrap() error {
	if e == nil || len(e.errors) == 0 {
		return nil
	}
	return e.errors[0]
}

func (e *joinedRunError) Is(target error) bool {
	if e == nil {
		return false
	}
	for _, err := range e.errors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
