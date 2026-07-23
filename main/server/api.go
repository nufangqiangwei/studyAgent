package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	contentTypeJSON       = "application/json"
	maxRequestBodySize    = 64 << 10
	websocketWriteTimeout = 10 * time.Second
	websocketPingInterval = 30 * time.Second
)

var taskIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type apiServer struct {
	runtime  RuntimePort
	upgrader websocket.Upgrader
}

type taskEnvelope struct {
	Task TaskView `json:"task"`
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	TaskID  string `json:"task_id,omitempty"`
}

type approvalEvent struct {
	Type     string          `json:"type"`
	Version  int             `json:"version"`
	Approval ApprovalRequest `json:"approval"`
}

func newAPIHandler(runtime RuntimePort) http.Handler {
	if runtime == nil {
		runtime = unavailableRuntimePort{}
	}
	server := &apiServer{runtime: runtime, upgrader: websocket.Upgrader{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tasks", server.handleTasks)
	mux.HandleFunc("/v1/tasks/", server.handleTask)
	mux.HandleFunc("/v1/approvals/ws", server.handleApprovalWebSocket)
	mux.Handle("/", newWebHandler())
	return recoverHTTP(mux)
}

func (s *apiServer) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/tasks" {
		writeError(w, http.StatusNotFound, "route_not_found", "route was not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		return
	}
	actor, ok := actorFromRequest(w, r)
	if !ok {
		return
	}
	if mediaType := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]); mediaType != contentTypeJSON {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	var input CreateTaskInput
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	input.TaskID = strings.TrimSpace(input.TaskID)
	input.GoalID = strings.TrimSpace(input.GoalID)
	input.Title = strings.TrimSpace(input.Title)
	if input.TaskID != "" && !taskIDPattern.MatchString(input.TaskID) {
		writeError(w, http.StatusBadRequest, "invalid_task_id", "task_id has an invalid format")
		return
	}
	if strings.TrimSpace(input.Input) == "" {
		writeError(w, http.StatusBadRequest, "invalid_task_input", "input is required")
		return
	}
	if len(input.Input) > maxInlineTaskInputSize {
		writeError(w, http.StatusRequestEntityTooLarge, "task_input_too_large", "input exceeds the inline task limit")
		return
	}
	task, err := s.runtime.CreateTask(r.Context(), actor, input)
	if err != nil {
		writeRuntimeError(w, err, task.TaskID)
		return
	}
	writeJSON(w, http.StatusCreated, taskEnvelope{Task: task})
}

func (s *apiServer) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		return
	}
	actor, ok := actorFromRequest(w, r)
	if !ok {
		return
	}
	taskID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if !taskIDPattern.MatchString(taskID) {
		writeError(w, http.StatusBadRequest, "invalid_task_id", "task_id has an invalid format")
		return
	}
	task, err := s.runtime.GetTask(r.Context(), actor, taskID)
	if err != nil {
		writeRuntimeError(w, err, "")
		return
	}
	writeJSON(w, http.StatusOK, taskEnvelope{Task: task})
}

func (s *apiServer) handleApprovalWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		return
	}
	actor, ok := actorFromRequest(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	events, err := s.runtime.SubscribeApprovalRequests(ctx, actor)
	if err != nil {
		writeRuntimeError(w, err, "")
		return
	}
	connection, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()

	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		connection.SetReadLimit(1024)
		for {
			if _, _, readErr := connection.NextReader(); readErr != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(websocketPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-clientClosed:
			return
		case event, open := <-events:
			if !open {
				_ = connection.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "approval stream closed"),
					time.Now().Add(websocketWriteTimeout))
				return
			}
			if event.UserID != actor.UserID {
				continue
			}
			if err := connection.SetWriteDeadline(time.Now().Add(websocketWriteTimeout)); err != nil {
				return
			}
			if err := connection.WriteJSON(approvalEvent{Type: "approval.requested", Version: apiVersion, Approval: event}); err != nil {
				return
			}
		case <-ping.C:
			if err := connection.WriteControl(websocket.PingMessage, nil, time.Now().Add(websocketWriteTimeout)); err != nil {
				return
			}
		}
	}
}

func actorFromRequest(w http.ResponseWriter, r *http.Request) (Actor, bool) {
	userID := strings.TrimSpace(r.Header.Get(userIDHeader))
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user_required", userIDHeader+" is required")
		return Actor{}, false
	}
	if len(userID) > 128 {
		writeError(w, http.StatusBadRequest, "invalid_user", userIDHeader+" is too long")
		return Actor{}, false
	}
	return Actor{UserID: userID}, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			return fmt.Errorf("request body exceeds %d bytes", maxRequestBodySize)
		}
		return fmt.Errorf("request body is invalid: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("request body must contain one JSON object")
		}
		return fmt.Errorf("request body is invalid: %w", err)
	}
	return nil
}

func writeRuntimeError(w http.ResponseWriter, err error, taskID string) {
	switch {
	case errors.Is(err, ErrRuntimeUnavailable):
		writeErrorWithTask(w, http.StatusServiceUnavailable, "runtime_unavailable", "Runtime interaction is not connected", taskID)
	case errors.Is(err, ErrTaskNotFound):
		writeErrorWithTask(w, http.StatusNotFound, "task_not_found", "task was not found", taskID)
	case errors.Is(err, ErrTaskConflict):
		writeErrorWithTask(w, http.StatusConflict, "task_conflict", "task conflicts with an existing task", taskID)
	default:
		writeErrorWithTask(w, http.StatusInternalServerError, "internal_error", "request could not be completed", taskID)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeErrorWithTask(w, status, code, message, "")
}

func writeErrorWithTask(w http.ResponseWriter, status int, code, message, taskID string) {
	writeJSON(w, status, errorEnvelope{Error: apiError{Code: code, Message: message, TaskID: taskID}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func recoverHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
