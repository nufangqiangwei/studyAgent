package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const debugLockRetry = 20 * time.Millisecond

type BodyDebugRecorder interface {
	Record(ctx context.Context, entry HTTPExchangeLog) error
}

type HTTPExchangeLog struct {
	Kind         string    `json:"kind"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	Provider     string    `json:"provider"`
	Model        string    `json:"model"`
	Endpoint     string    `json:"endpoint"`
	StatusCode   int       `json:"status_code,omitempty"`
	Status       string    `json:"status,omitempty"`
	RequestBody  DebugBody `json:"request_body"`
	ResponseBody DebugBody `json:"response_body"`
	Error        string    `json:"error,omitempty"`
}

type DebugBody []byte

func (b DebugBody) MarshalJSON() ([]byte, error) {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return []byte("null"), nil
	}
	if json.Valid(trimmed) {
		return trimmed, nil
	}
	return json.Marshal(string(trimmed))
}

type JSONLDebugRecorder struct {
	mu       sync.Mutex
	path     string
	lockPath string
}

func NewJSONLDebugRecorder(path string) (*JSONLDebugRecorder, error) {
	if path == "" {
		return nil, fmt.Errorf("debug recorder: path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create debug log directory %s: %w", filepath.Dir(path), err)
	}
	return &JSONLDebugRecorder{
		path:     path,
		lockPath: path + ".lock",
	}, nil
}

func (r *JSONLDebugRecorder) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

func (r *JSONLDebugRecorder) Record(ctx context.Context, entry HTTPExchangeLog) error {
	if r == nil {
		return fmt.Errorf("debug recorder: nil recorder")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if entry.Kind == "" {
		entry.Kind = "llm_http"
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0700); err != nil {
		return fmt.Errorf("create debug log directory %s: %w", filepath.Dir(r.path), err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.withLock(ctx, func() error {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode debug log entry: %w", err)
		}
		data = append(data, '\n')

		file, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open debug log %s: %w", r.path, err)
		}
		defer file.Close()

		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("write debug log %s: %w", r.path, err)
		}
		return nil
	})
}

func (r *JSONLDebugRecorder) withLock(ctx context.Context, fn func() error) error {
	for {
		lockFile, err := os.OpenFile(r.lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			_, writeErr := fmt.Fprintf(lockFile, "pid=%d\ncreated_at=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
			closeErr := lockFile.Close()
			if writeErr != nil {
				_ = os.Remove(r.lockPath)
				return fmt.Errorf("write debug log lock %s: %w", r.lockPath, writeErr)
			}
			if closeErr != nil {
				_ = os.Remove(r.lockPath)
				return fmt.Errorf("close debug log lock %s: %w", r.lockPath, closeErr)
			}
			defer os.Remove(r.lockPath)
			return fn()
		}
		if !debugLockBusy(err, r.lockPath) {
			return fmt.Errorf("acquire debug log lock %s: %w", r.lockPath, err)
		}

		timer := time.NewTimer(debugLockRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("acquire debug log lock %s: %w", r.lockPath, ctx.Err())
		case <-timer.C:
		}
	}
}

func debugLockBusy(err error, lockPath string) bool {
	if os.IsExist(err) {
		return true
	}
	if !os.IsPermission(err) {
		return false
	}
	_, statErr := os.Stat(lockPath)
	return statErr == nil
}
