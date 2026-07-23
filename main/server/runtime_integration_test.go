package main

import (
	"agent/serviceruntime/artifact"
	artifactlocal "agent/serviceruntime/artifact/local"
	persistencesqlite "agent/serviceruntime/persistence/sqlite"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const runtimeIntegrationTimeout = 5 * time.Second

type runningRuntimeIntegrationApplication struct {
	application     *application
	cancel          context.CancelFunc
	serveDone       chan error
	serveWaited     bool
	serveErr        error
	allowCloseError bool
}

type runtimeIntegrationTaskEnvelope struct {
	Task TaskView `json:"task"`
}

func TestWebRuntimeSQLiteRestartIntegration(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	config := validRuntimeApplicationConfig(dataDir)
	config.RuntimeID = "web-runtime-restart-integration"
	options := applicationOptions{
		modelClient: noNetworkModelClient{},
		clock:       runtimeAppTestClock{now: now},
	}

	first := startRuntimeIntegrationApplication(t, config, options)
	if _, ok := first.application.storage.(*persistencesqlite.Store); !ok {
		t.Fatalf("storage type=%T, want *sqlite.Store", first.application.storage)
	}
	if _, ok := first.application.artifacts.(*artifactlocal.Store); !ok {
		t.Fatalf("artifact type=%T, want *local.Store", first.application.artifacts)
	}

	artifactBody := "local artifact survives restart"
	ref, err := first.application.runtime.WriteArtifact(
		context.Background(),
		artifact.WriteRequest{Key: "integration/restart-probe.txt", ContentType: "text/plain"},
		strings.NewReader(artifactBody),
	)
	if err != nil {
		t.Fatalf("write local artifact: %v", err)
	}

	create := CreateTaskInput{
		TaskID: "task-restart-1",
		GoalID: "goal-restart-1",
		Title:  "SQLite restart",
		Input:  "verify durable Web Runtime state",
	}
	created := requestRuntimeIntegrationTask(t, first.application.runtimePort, http.MethodPost, "/v1/tasks", "user-1", create, http.StatusCreated)
	assertRuntimeIntegrationTask(t, created, create, "user-1", now)

	first.stop(t)
	assertRuntimeIntegrationDataDirReleased(t, dataDir)

	second := startRuntimeIntegrationApplication(t, config, options)
	t.Cleanup(func() {
		second.stop(t)
		assertRuntimeIntegrationDataDirReleased(t, dataDir)
	})

	reader, _, err := second.application.runtime.OpenArtifact(context.Background(), ref)
	if err != nil {
		t.Fatalf("open recovered local artifact: %v", err)
	}
	recoveredArtifact, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read recovered local artifact: read=%v close=%v", readErr, closeErr)
	}
	if string(recoveredArtifact) != artifactBody {
		t.Fatalf("recovered artifact=%q, want %q", recoveredArtifact, artifactBody)
	}

	found := requestRuntimeIntegrationTask(
		t,
		second.application.runtimePort,
		http.MethodGet,
		"/v1/tasks/"+create.TaskID,
		"user-1",
		nil,
		http.StatusOK,
	)
	assertRuntimeIntegrationTask(t, found, create, "user-1", now)

	idempotent := requestRuntimeIntegrationTask(
		t,
		second.application.runtimePort,
		http.MethodPost,
		"/v1/tasks",
		"user-1",
		create,
		http.StatusCreated,
	)
	assertRuntimeIntegrationTask(t, idempotent, create, "user-1", now)

	conflicting := create
	conflicting.Input = "different content for the same task"
	assertRuntimeIntegrationError(
		t,
		second.application.runtimePort,
		http.MethodPost,
		"/v1/tasks",
		"user-1",
		conflicting,
		http.StatusConflict,
		"task_conflict",
	)
	assertRuntimeIntegrationError(
		t,
		second.application.runtimePort,
		http.MethodGet,
		"/v1/tasks/"+create.TaskID,
		"user-2",
		nil,
		http.StatusNotFound,
		"task_not_found",
	)
}

func TestWebRuntimeUnavailableReturnsAreBounded(t *testing.T) {
	t.Run("runtime not live", func(t *testing.T) {
		config := validRuntimeApplicationConfig(t.TempDir())
		config.RuntimeID = "web-runtime-not-live"
		app, err := buildApplication(context.Background(), config, applicationOptions{
			modelClient: noNetworkModelClient{},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer app.Close()
		assertRuntimeIntegrationUnavailable(t, app.runtimePort)
	})

	t.Run("runtime fatal", func(t *testing.T) {
		config := validRuntimeApplicationConfig(t.TempDir())
		config.RuntimeID = "web-runtime-fatal"
		running := startRuntimeIntegrationApplication(t, config, applicationOptions{
			modelClient: noNetworkModelClient{},
		})
		defer running.stop(t)

		if err := running.application.storage.Close(); err != nil {
			t.Fatalf("close SQLite to trigger Runtime fatal: %v", err)
		}
		running.allowCloseError = true
		assertRuntimeIntegrationUnavailable(t, running.application.runtimePort)
		if err := running.waitServe(t); err == nil {
			t.Fatal("Runtime Serve did not report the forced SQLite failure")
		}
	})

	t.Run("adapter closed", func(t *testing.T) {
		config := validRuntimeApplicationConfig(t.TempDir())
		config.RuntimeID = "web-runtime-adapter-closed"
		running := startRuntimeIntegrationApplication(t, config, applicationOptions{
			modelClient: noNetworkModelClient{},
		})
		defer running.stop(t)

		if err := running.application.adapter.Close(); err != nil {
			t.Fatal(err)
		}
		assertRuntimeIntegrationUnavailable(t, running.application.runtimePort)
	})
}

func startRuntimeIntegrationApplication(
	t *testing.T,
	config serverConfig,
	options applicationOptions,
) *runningRuntimeIntegrationApplication {
	t.Helper()
	app, err := buildApplication(context.Background(), config, options)
	if err != nil {
		t.Fatalf("build Runtime application: %v", err)
	}
	startCtx, cancelStart := context.WithTimeout(context.Background(), runtimeIntegrationTimeout)
	_, err = app.runtime.Start(startCtx)
	cancelStart()
	if err != nil {
		_ = app.Close()
		t.Fatalf("start Runtime application: %v", err)
	}

	serveCtx, cancelServe := context.WithCancel(context.Background())
	running := &runningRuntimeIntegrationApplication{
		application: app,
		cancel:      cancelServe,
		serveDone:   make(chan error, 1),
	}
	started := make(chan struct{})
	go func() {
		close(started)
		running.serveDone <- app.runtime.Serve(serveCtx)
	}()
	<-started
	t.Cleanup(func() { running.stop(t) })
	return running
}

func (r *runningRuntimeIntegrationApplication) waitServe(t *testing.T) error {
	t.Helper()
	if r.serveWaited {
		return r.serveErr
	}
	select {
	case r.serveErr = <-r.serveDone:
		r.serveWaited = true
		return r.serveErr
	case <-time.After(runtimeIntegrationTimeout):
		t.Fatal("Runtime Serve did not stop")
		return nil
	}
}

func (r *runningRuntimeIntegrationApplication) stop(t *testing.T) {
	t.Helper()
	if r == nil || r.application == nil {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
	if !r.serveWaited {
		if err := r.waitServe(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Runtime Serve stopped with error: %v", err)
		}
	}
	if err := r.application.Close(); err != nil && !r.allowCloseError {
		t.Errorf("close Runtime application: %v", err)
	}
	r.application = nil
}

func requestRuntimeIntegrationTask(
	t *testing.T,
	port RuntimePort,
	method string,
	path string,
	userID string,
	body any,
	wantStatus int,
) TaskView {
	t.Helper()
	response := requestRuntimeIntegrationHTTP(t, port, method, path, userID, body)
	if response.Code != wantStatus {
		t.Fatalf("status=%d, want %d; body=%s", response.Code, wantStatus, response.Body.String())
	}
	var envelope runtimeIntegrationTaskEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode task response: %v", err)
	}
	return envelope.Task
}

func assertRuntimeIntegrationTask(
	t *testing.T,
	got TaskView,
	want CreateTaskInput,
	userID string,
	now time.Time,
) {
	t.Helper()
	if got.TaskID != want.TaskID || got.GoalID != want.GoalID || got.UserID != userID ||
		got.Title != want.Title || got.Input != want.Input || got.Phase != "running" {
		t.Fatalf("task=%#v", got)
	}
	if !got.CreatedAt.Equal(now) || got.UpdatedAt.IsZero() || got.CompletedAt != nil {
		t.Fatalf("task timestamps=%#v", got)
	}
}

func assertRuntimeIntegrationError(
	t *testing.T,
	port RuntimePort,
	method string,
	path string,
	userID string,
	body any,
	wantStatus int,
	wantCode string,
) {
	t.Helper()
	response := requestRuntimeIntegrationHTTP(t, port, method, path, userID, body)
	if response.Code != wantStatus {
		t.Fatalf("status=%d, want %d; body=%s", response.Code, wantStatus, response.Body.String())
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("error code=%q, want %q; body=%s", envelope.Error.Code, wantCode, response.Body.String())
	}
}

func assertRuntimeIntegrationUnavailable(t *testing.T, port RuntimePort) {
	t.Helper()
	result := make(chan *httptest.ResponseRecorder, 1)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	go func() {
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/tasks",
			strings.NewReader(`{"task_id":"task-unavailable","input":"bounded response"}`),
		).WithContext(requestCtx)
		request.Header.Set("Content-Type", contentTypeJSON)
		request.Header.Set(userIDHeader, "user-1")
		response := httptest.NewRecorder()
		newAPIHandler(port).ServeHTTP(response, request)
		result <- response
	}()

	select {
	case response := <-result:
		if response.Code != http.StatusServiceUnavailable ||
			!strings.Contains(response.Body.String(), `"code":"runtime_unavailable"`) {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	case <-time.After(time.Second):
		cancelRequest()
		select {
		case <-result:
		case <-time.After(runtimeIntegrationTimeout):
			t.Fatal("unavailable Runtime request did not return after context cancellation")
		}
		t.Fatal("unavailable Runtime request exceeded its bounded response window")
	}
}

func requestRuntimeIntegrationHTTP(
	t *testing.T,
	port RuntimePort,
	method string,
	path string,
	userID string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		requestBody = bytes.NewReader(encoded)
	}
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), runtimeIntegrationTimeout)
	defer cancelRequest()
	request := httptest.NewRequest(method, path, requestBody).WithContext(requestCtx)
	request.Header.Set(userIDHeader, userID)
	if body != nil {
		request.Header.Set("Content-Type", contentTypeJSON)
	}
	response := httptest.NewRecorder()
	newAPIHandler(port).ServeHTTP(response, request)
	return response
}

func assertRuntimeIntegrationDataDirReleased(t *testing.T, dataDir string) {
	t.Helper()
	releasedPath := dataDir + "-released"
	if err := os.Rename(dataDir, releasedPath); err != nil {
		t.Fatalf("rename closed Runtime data directory: %v", err)
	}
	if err := os.Rename(releasedPath, dataDir); err != nil {
		t.Fatalf("restore closed Runtime data directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "runtime.db")); err != nil {
		t.Fatalf("stat released Runtime database: %v", err)
	}
}
