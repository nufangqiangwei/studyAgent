package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunDefaultsToRealApplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newLifecycleLog()
	server := newFakeRunHTTPServer(log)
	dataDir := t.TempDir()
	result := make(chan error, 1)
	var stdout bytes.Buffer

	go func() {
		result <- runWithOptions(
			ctx,
			[]string{
				"-model=test-model",
				"-data-dir=" + dataDir,
				"-shutdown-timeout=1s",
			},
			&stdout,
			&bytes.Buffer{},
			runOptions{
				listen: func(network, address string) (net.Listener, error) {
					log.record("listen")
					return newFakeRunListener(address, log), nil
				},
				newServer: func(_ serverConfig, handler http.Handler) runHTTPServer {
					server.handler = handler
					return server
				},
				getenv: func(string) string { return "" },
			},
		)
	}()

	log.waitFor(t, "http.serve")
	cancel()
	if err := waitRunTestResult(t, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Agent Web Server listening on http://") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dataDir, "runtime.db")); err != nil {
		t.Fatalf("production Run did not build SQLite Runtime application: %v", err)
	}
	if got := server.handler; got == nil {
		t.Fatal("production Run did not create the API handler")
	}
}

func TestRunCancellationOrdersHTTPRuntimeAndApplicationShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	server := newFakeRunHTTPServer(log)
	result := startFakeRun(t, ctx, app, server, log, nil)

	log.waitFor(t, "http.serve")
	cancel()
	if err := waitRunTestResult(t, result); err != nil {
		t.Fatal(err)
	}
	log.requireOrder(t,
		"runtime.start",
		"runtime.serve",
		"listen",
		"http.serve",
		"http.shutdown",
		"runtime.drain",
		"runtime.serve.stop",
		"application.close",
	)
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
	if server.closeCount() != 0 {
		t.Fatalf("HTTP force-close count=%d", server.closeCount())
	}
}

func TestRunBuildFailureClosesReturnedApplicationWithoutListening(t *testing.T) {
	buildFailure := errors.New("build failed")
	closeFailure := errors.New("close failed")
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	app.closeErr = closeFailure
	listened := false

	err := runWithOptions(
		context.Background(),
		fakeRunArgs(),
		&bytes.Buffer{},
		&bytes.Buffer{},
		runOptions{
			buildApplication: func(context.Context, serverConfig) (runApplication, error) {
				log.record("build")
				return app, buildFailure
			},
			listen: func(string, string) (net.Listener, error) {
				listened = true
				return nil, errors.New("unexpected listen")
			},
			getenv: func(string) string { return "" },
		},
	)
	if !errors.Is(err, buildFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("error=%v", err)
	}
	if listened {
		t.Fatal("Build failure created an HTTP listener")
	}
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func TestRunStartFailureClosesApplicationWithoutListening(t *testing.T) {
	startFailure := errors.New("start failed")
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	app.startErr = startFailure
	listened := false

	err := runFakeSynchronously(app, newFakeRunHTTPServer(log), log, func(string, string) (net.Listener, error) {
		listened = true
		return nil, errors.New("unexpected listen")
	})
	if !errors.Is(err, startFailure) {
		t.Fatalf("error=%v", err)
	}
	if listened {
		t.Fatal("Start failure created an HTTP listener")
	}
	log.requireOrder(t, "runtime.start", "application.close")
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func TestRunListenFailureStopsRuntimeAndClosesApplication(t *testing.T) {
	listenFailure := errors.New("listen failed")
	log := newLifecycleLog()
	app := newFakeRunApplication(log)

	err := runFakeSynchronously(app, newFakeRunHTTPServer(log), log, func(string, string) (net.Listener, error) {
		log.record("listen")
		return nil, listenFailure
	})
	if !errors.Is(err, listenFailure) {
		t.Fatalf("error=%v", err)
	}
	log.requireOrder(t,
		"runtime.start",
		"runtime.serve",
		"listen",
		"runtime.serve.stop",
		"application.close",
	)
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func TestRunRejectsRuntimeServeFatalBeforeListening(t *testing.T) {
	runtimeFailure := errors.New("runtime failed immediately")
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	app.serveResult <- runtimeFailure
	app.waitForServeOnSecondLiveCheck = true
	listened := false

	err := runFakeSynchronously(app, newFakeRunHTTPServer(log), log, func(string, string) (net.Listener, error) {
		listened = true
		return nil, errors.New("unexpected listen")
	})
	if !errors.Is(err, runtimeFailure) {
		t.Fatalf("error=%v", err)
	}
	if listened {
		t.Fatal("immediate Runtime fatal created an HTTP listener")
	}
	log.requireOrder(t, "runtime.start", "runtime.serve", "adapter.unavailable", "application.close")
}

func TestRunRuntimeFatalMarksAdapterUnavailableAndStopsHTTP(t *testing.T) {
	runtimeFailure := errors.New("runtime fatal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	server := newFakeRunHTTPServer(log)
	result := startFakeRun(t, ctx, app, server, log, nil)

	log.waitFor(t, "http.serve")
	app.serveResult <- runtimeFailure
	err := waitRunTestResult(t, result)
	if !errors.Is(err, runtimeFailure) {
		t.Fatalf("error=%v", err)
	}
	log.requireOrder(t,
		"runtime.serve",
		"http.serve",
		"adapter.unavailable",
		"http.shutdown",
		"application.close",
	)
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func TestRunHTTPFatalDrainsAndStopsRuntime(t *testing.T) {
	httpFailure := errors.New("HTTP fatal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	server := newFakeRunHTTPServer(log)
	result := startFakeRun(t, ctx, app, server, log, nil)

	log.waitFor(t, "http.serve")
	server.serveResult <- httpFailure
	err := waitRunTestResult(t, result)
	if !errors.Is(err, httpFailure) {
		t.Fatalf("error=%v", err)
	}
	log.requireOrder(t,
		"http.serve",
		"adapter.unavailable",
		"runtime.drain",
		"runtime.serve.stop",
		"application.close",
	)
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func TestRunShutdownTimeoutForceClosesHTTPAndContinuesCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newLifecycleLog()
	app := newFakeRunApplication(log)
	server := newFakeRunHTTPServer(log)
	server.shutdownErr = context.DeadlineExceeded
	result := startFakeRun(t, ctx, app, server, log, nil)

	log.waitFor(t, "http.serve")
	cancel()
	err := waitRunTestResult(t, result)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	log.requireOrder(t,
		"http.shutdown",
		"http.close",
		"runtime.drain",
		"runtime.serve.stop",
		"application.close",
	)
	if server.closeCount() != 1 {
		t.Fatalf("HTTP force-close count=%d", server.closeCount())
	}
	if app.closeCount() != 1 {
		t.Fatalf("application close count=%d", app.closeCount())
	}
}

func startFakeRun(
	t *testing.T,
	ctx context.Context,
	app *fakeRunApplication,
	server *fakeRunHTTPServer,
	log *lifecycleLog,
	listen func(string, string) (net.Listener, error),
) <-chan error {
	t.Helper()
	if listen == nil {
		listen = func(_ string, address string) (net.Listener, error) {
			log.record("listen")
			return newFakeRunListener(address, log), nil
		}
	}
	result := make(chan error, 1)
	go func() {
		result <- runWithOptions(
			ctx,
			fakeRunArgs(),
			&bytes.Buffer{},
			&bytes.Buffer{},
			runOptions{
				buildApplication: func(context.Context, serverConfig) (runApplication, error) {
					log.record("build")
					return app, nil
				},
				listen: listen,
				newServer: func(serverConfig, http.Handler) runHTTPServer {
					return server
				},
				getenv: func(string) string { return "" },
			},
		)
	}()
	return result
}

func runFakeSynchronously(
	app *fakeRunApplication,
	server *fakeRunHTTPServer,
	log *lifecycleLog,
	listen func(string, string) (net.Listener, error),
) error {
	return runWithOptions(
		context.Background(),
		fakeRunArgs(),
		&bytes.Buffer{},
		&bytes.Buffer{},
		runOptions{
			buildApplication: func(context.Context, serverConfig) (runApplication, error) {
				log.record("build")
				return app, nil
			},
			listen: listen,
			newServer: func(serverConfig, http.Handler) runHTTPServer {
				return server
			},
			getenv: func(string) string { return "" },
		},
	)
}

func fakeRunArgs() []string {
	return []string{"-model=test-model", "-shutdown-timeout=250ms"}
}

func waitRunTestResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

type fakeRunApplication struct {
	log         *lifecycleLog
	serveResult chan error
	serveCalled chan struct{}

	mu                            sync.Mutex
	live                          bool
	liveChecks                    int
	waitForServeOnSecondLiveCheck bool
	startErr                      error
	drainErr                      error
	markErr                       error
	closeErr                      error
	closes                        int
	serveOnce                     sync.Once
}

func newFakeRunApplication(log *lifecycleLog) *fakeRunApplication {
	return &fakeRunApplication{
		log:         log,
		serveResult: make(chan error, 1),
		serveCalled: make(chan struct{}),
	}
}

func (*fakeRunApplication) RuntimePort() RuntimePort { return runtimePortStubForLifecycle{} }

func (a *fakeRunApplication) Start(context.Context) error {
	a.log.record("runtime.start")
	if a.startErr != nil {
		return a.startErr
	}
	a.mu.Lock()
	a.live = true
	a.mu.Unlock()
	return nil
}

func (a *fakeRunApplication) Serve(ctx context.Context) error {
	a.log.record("runtime.serve")
	a.serveOnce.Do(func() { close(a.serveCalled) })
	select {
	case err := <-a.serveResult:
		a.mu.Lock()
		a.live = false
		a.mu.Unlock()
		return err
	case <-ctx.Done():
		a.log.record("runtime.serve.stop")
		return nil
	}
}

func (a *fakeRunApplication) Drain(context.Context) error {
	a.log.record("runtime.drain")
	return a.drainErr
}

func (a *fakeRunApplication) Live() bool {
	a.mu.Lock()
	a.liveChecks++
	check := a.liveChecks
	wait := a.waitForServeOnSecondLiveCheck && check == 2
	live := a.live
	a.mu.Unlock()
	if wait {
		<-a.serveCalled
		a.mu.Lock()
		live = a.live
		a.mu.Unlock()
	}
	return live
}

func (a *fakeRunApplication) MarkUnavailable() error {
	a.log.record("adapter.unavailable")
	return a.markErr
}

func (a *fakeRunApplication) Close() error {
	a.log.record("application.close")
	a.mu.Lock()
	a.closes++
	a.live = false
	a.mu.Unlock()
	return a.closeErr
}

func (a *fakeRunApplication) closeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closes
}

type runtimePortStubForLifecycle struct{}

func (runtimePortStubForLifecycle) CreateTask(context.Context, Actor, CreateTaskInput) (TaskView, error) {
	return TaskView{}, ErrRuntimeUnavailable
}

func (runtimePortStubForLifecycle) GetTask(context.Context, Actor, string) (TaskView, error) {
	return TaskView{}, ErrRuntimeUnavailable
}

func (runtimePortStubForLifecycle) SubscribeApprovalRequests(context.Context, Actor) (<-chan ApprovalRequest, error) {
	return nil, ErrRuntimeUnavailable
}

type fakeRunHTTPServer struct {
	log         *lifecycleLog
	serveResult chan error
	handler     http.Handler

	shutdownErr error
	closeErr    error

	mu     sync.Mutex
	closes int
	stop   chan struct{}
	once   sync.Once
}

func newFakeRunHTTPServer(log *lifecycleLog) *fakeRunHTTPServer {
	return &fakeRunHTTPServer{
		log:         log,
		serveResult: make(chan error, 1),
		stop:        make(chan struct{}),
	}
}

func (s *fakeRunHTTPServer) Serve(listener net.Listener) error {
	s.log.record("http.serve")
	defer listener.Close()
	select {
	case err := <-s.serveResult:
		return err
	case <-s.stop:
		return http.ErrServerClosed
	}
}

func (s *fakeRunHTTPServer) Shutdown(context.Context) error {
	s.log.record("http.shutdown")
	if s.shutdownErr != nil {
		return s.shutdownErr
	}
	s.once.Do(func() { close(s.stop) })
	return nil
}

func (s *fakeRunHTTPServer) Close() error {
	s.log.record("http.close")
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	s.once.Do(func() { close(s.stop) })
	return s.closeErr
}

func (s *fakeRunHTTPServer) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

type fakeRunListener struct {
	address net.Addr
	log     *lifecycleLog
	once    sync.Once
	closed  chan struct{}
}

func newFakeRunListener(address string, log *lifecycleLog) *fakeRunListener {
	return &fakeRunListener{
		address: fakeRunAddr(address),
		log:     log,
		closed:  make(chan struct{}),
	}
}

func (l *fakeRunListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *fakeRunListener) Close() error {
	l.once.Do(func() {
		l.log.record("listener.close")
		close(l.closed)
	})
	return nil
}

func (l *fakeRunListener) Addr() net.Addr { return l.address }

type fakeRunAddr string

func (fakeRunAddr) Network() string  { return "tcp" }
func (a fakeRunAddr) String() string { return string(a) }

type lifecycleLog struct {
	mu     sync.Mutex
	values []string
	events chan string
}

func newLifecycleLog() *lifecycleLog {
	return &lifecycleLog{events: make(chan string, 64)}
}

func (l *lifecycleLog) record(value string) {
	l.mu.Lock()
	l.values = append(l.values, value)
	l.mu.Unlock()
	l.events <- value
}

func (l *lifecycleLog) waitFor(t *testing.T, target string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case value := <-l.events:
			if value == target {
				return
			}
		case <-timer.C:
			t.Fatalf("did not observe %q; events=%v", target, l.snapshot())
		}
	}
}

func (l *lifecycleLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.values...)
}

func (l *lifecycleLog) requireOrder(t *testing.T, expected ...string) {
	t.Helper()
	values := l.snapshot()
	position := 0
	for _, value := range values {
		if position < len(expected) && value == expected[position] {
			position++
		}
	}
	if position != len(expected) {
		t.Fatalf("events=%v, want ordered subsequence=%v", values, expected)
	}
}

func (l *lifecycleLog) String() string {
	return fmt.Sprint(l.snapshot())
}
