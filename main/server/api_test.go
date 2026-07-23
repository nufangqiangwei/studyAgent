package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type runtimePortStub struct {
	create    func(context.Context, Actor, CreateTaskInput) (TaskView, error)
	get       func(context.Context, Actor, string) (TaskView, error)
	subscribe func(context.Context, Actor) (<-chan ApprovalRequest, error)
}

func (s runtimePortStub) CreateTask(ctx context.Context, actor Actor, input CreateTaskInput) (TaskView, error) {
	return s.create(ctx, actor, input)
}

func (s runtimePortStub) GetTask(ctx context.Context, actor Actor, taskID string) (TaskView, error) {
	return s.get(ctx, actor, taskID)
}

func (s runtimePortStub) SubscribeApprovalRequests(ctx context.Context, actor Actor) (<-chan ApprovalRequest, error) {
	return s.subscribe(ctx, actor)
}

func TestCreateTask(t *testing.T) {
	now := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	port := runtimePortStub{
		create: func(_ context.Context, actor Actor, input CreateTaskInput) (TaskView, error) {
			if actor.UserID != "user-1" || input.GoalID != "goal-1" || input.Input != "do work" {
				t.Fatalf("actor=%#v input=%#v", actor, input)
			}
			return TaskView{TaskID: "task-1", GoalID: input.GoalID, Title: input.Title, Phase: "created", CreatedAt: now, UpdatedAt: now}, nil
		},
		get:       func(context.Context, Actor, string) (TaskView, error) { return TaskView{}, nil },
		subscribe: func(context.Context, Actor) (<-chan ApprovalRequest, error) { return nil, nil },
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(`{"goal_id":"goal-1","title":"Work","input":"do work"}`))
	request.Header.Set("Content-Type", contentTypeJSON)
	request.Header.Set(userIDHeader, "user-1")
	response := httptest.NewRecorder()
	newAPIHandler(port).ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body taskEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Task.TaskID != "task-1" || body.Task.Phase != "created" {
		t.Fatalf("task=%#v", body.Task)
	}
}

func TestCreateTaskFailureReturnsConfirmedTaskID(t *testing.T) {
	port := runtimePortStub{
		create: func(context.Context, Actor, CreateTaskInput) (TaskView, error) {
			return TaskView{TaskID: "task-derived-1"}, errRuntimeAdapterInternal
		},
		get:       func(context.Context, Actor, string) (TaskView, error) { return TaskView{}, nil },
		subscribe: func(context.Context, Actor) (<-chan ApprovalRequest, error) { return nil, nil },
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(`{"input":"do work"}`))
	request.Header.Set("Content-Type", contentTypeJSON)
	request.Header.Set(userIDHeader, "user-1")
	response := httptest.NewRecorder()
	newAPIHandler(port).ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "internal_error" || body.Error.TaskID != "task-derived-1" {
		t.Fatalf("error response lost confirmed task id: %#v", body)
	}
}

func TestHomepageAndStaticAssets(t *testing.T) {
	handler := newAPIHandler(nil)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Fatalf("content-type=%q", contentType)
	}
	if response.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("cache-control=%q", response.Header().Get("Cache-Control"))
	}
	if !strings.Contains(response.Body.String(), "Agent 工作台") {
		t.Fatalf("homepage does not contain the Agent workbench title")
	}

	stylesheet := regexp.MustCompile(`href="(/assets/[^"]+\.css)"`).FindStringSubmatch(response.Body.String())
	if len(stylesheet) != 2 {
		t.Fatalf("homepage does not reference its stylesheet")
	}
	assetRequest := httptest.NewRequest(http.MethodGet, stylesheet[1], nil)
	assetResponse := httptest.NewRecorder()
	handler.ServeHTTP(assetResponse, assetRequest)
	if assetResponse.Code != http.StatusOK {
		t.Fatalf("asset status=%d path=%s", assetResponse.Code, stylesheet[1])
	}
	if contentType := assetResponse.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/css") {
		t.Fatalf("asset content-type=%q", contentType)
	}
	if !strings.Contains(assetResponse.Header().Get("Cache-Control"), "immutable") {
		t.Fatalf("asset cache-control=%q", assetResponse.Header().Get("Cache-Control"))
	}
}

func TestHomepageMethodNotAllowed(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	response := httptest.NewRecorder()
	newAPIHandler(nil).ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("allow=%q", response.Header().Get("Allow"))
	}
}

func TestUnknownAPIRouteReturnsJSONError(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/unknown", nil)
	response := httptest.NewRecorder()
	newAPIHandler(nil).ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"code":"route_not_found"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != contentTypeJSON {
		t.Fatalf("content-type=%q", response.Header().Get("Content-Type"))
	}
}

func TestGetTaskNotFound(t *testing.T) {
	port := runtimePortStub{
		create:    func(context.Context, Actor, CreateTaskInput) (TaskView, error) { return TaskView{}, nil },
		get:       func(context.Context, Actor, string) (TaskView, error) { return TaskView{}, ErrTaskNotFound },
		subscribe: func(context.Context, Actor) (<-chan ApprovalRequest, error) { return nil, nil },
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/missing", nil)
	request.Header.Set(userIDHeader, "user-1")
	response := httptest.NewRecorder()
	newAPIHandler(port).ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"code":"task_not_found"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUnavailableRuntime(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(`{"input":"do work"}`))
	request.Header.Set("Content-Type", contentTypeJSON)
	request.Header.Set(userIDHeader, "user-1")
	response := httptest.NewRecorder()
	newAPIHandler(nil).ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"runtime_unavailable"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestApprovalWebSocketSendsRequestedEvent(t *testing.T) {
	events := make(chan ApprovalRequest, 2)
	port := runtimePortStub{
		create: func(context.Context, Actor, CreateTaskInput) (TaskView, error) { return TaskView{}, nil },
		get:    func(context.Context, Actor, string) (TaskView, error) { return TaskView{}, nil },
		subscribe: func(_ context.Context, actor Actor) (<-chan ApprovalRequest, error) {
			if actor.UserID != "user-1" {
				t.Fatalf("actor=%#v", actor)
			}
			return events, nil
		},
	}
	server := httptest.NewServer(newAPIHandler(port))
	defer server.Close()

	header := http.Header{}
	header.Set(userIDHeader, "user-1")
	connection, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/approvals/ws", header)
	if err != nil {
		if response != nil {
			t.Fatalf("dial status=%d err=%v", response.StatusCode, err)
		}
		t.Fatal(err)
	}
	defer connection.Close()

	requestedAt := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	events <- ApprovalRequest{
		ApprovalID: "approval-other", CallID: "call-other", UserID: "user-2",
		CapabilityRef: "workspace.write", CapabilityVersion: "v1",
		RiskSummary: "write files", ArgumentsDigest: "sha256:other", RequestedAt: requestedAt,
	}
	events <- ApprovalRequest{
		ApprovalID: "approval-1", CallID: "call-1", UserID: "user-1",
		CapabilityRef: "workspace.write", CapabilityVersion: "v1",
		RiskSummary: "write files", ArgumentsDigest: "sha256:test", RequestedAt: requestedAt,
	}
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	var event approvalEvent
	if err := connection.ReadJSON(&event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "approval.requested" || event.Version != apiVersion || event.Approval.ApprovalID != "approval-1" {
		t.Fatalf("event=%#v", event)
	}
}

func TestApprovalWebSocketUnavailableBeforeUpgrade(t *testing.T) {
	server := httptest.NewServer(newAPIHandler(nil))
	defer server.Close()
	header := http.Header{}
	header.Set(userIDHeader, "user-1")
	connection, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/approvals/ws", header)
	if connection != nil {
		connection.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("response=%#v err=%v", response, err)
	}
}

func TestRequiresUserIdentity(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil)
	response := httptest.NewRecorder()
	newAPIHandler(nil).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
