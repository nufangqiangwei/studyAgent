package persistence

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const schemaVersion = 1

type TaskStateStore interface {
	statemachine2.StateStore
	List(ctx context.Context) ([]statemachine2.TaskState, error)
}

type SnapshotStore interface {
	agents2.SnapshotStore
	List(ctx context.Context) ([]agents2.AgentSnapshot, error)
}

type RuntimeStore interface {
	Load(ctx context.Context, taskID string) (RuntimeSnapshot, bool, error)
	Save(ctx context.Context, snapshot RuntimeSnapshot) error
	Delete(ctx context.Context, taskID string) error
	List(ctx context.Context) ([]RuntimeSnapshot, error)
}

type EventStore interface {
	Append(ctx context.Context, event eventbus.Event) (bool, error)
	List(ctx context.Context, taskID string) ([]eventbus.Event, error)
	Last(ctx context.Context, taskID string) (eventbus.Event, bool, error)
}

type RuntimeStorage interface {
	TaskStates() TaskStateStore
	AgentSnapshots() SnapshotStore
	Runtimes() RuntimeStore
	Events() EventStore
	Backend() string
	Close() error
}

type RuntimeSnapshot struct {
	TaskID               string            `json:"task_id"`
	Agent                string            `json:"agent,omitempty"`
	EffectTimeout        time.Duration     `json:"effect_timeout,omitempty"`
	MaxConcurrentEffects int               `json:"max_concurrent_effects,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty"`
	CreatedAt            time.Time         `json:"created_at"`
	UpdatedAt            time.Time         `json:"updated_at"`
}

func NewRuntimeSnapshot(runtime reactor.TaskRuntime, now time.Time) RuntimeSnapshot {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return RuntimeSnapshot{
		TaskID:               strings.TrimSpace(runtime.TaskID),
		Agent:                strings.TrimSpace(runtime.Agent),
		EffectTimeout:        runtime.EffectTimeout,
		MaxConcurrentEffects: runtime.MaxConcurrentEffects,
		Metadata:             cloneStringMap(runtime.Metadata),
		CreatedAt:            now.UTC(),
		UpdatedAt:            now.UTC(),
	}
}

func (s RuntimeSnapshot) Clone() RuntimeSnapshot {
	cloned := s
	cloned.Metadata = cloneStringMap(s.Metadata)
	return cloned
}

type RuntimeRebuilder struct {
	StateMachine                reactor.StateMachine
	Executors                   *reactor.ExecutorRegistry
	DefaultEffectTimeout        time.Duration
	DefaultMaxConcurrentEffects int
}

func (s RuntimeSnapshot) TaskRuntime(rebuilder RuntimeRebuilder) (reactor.TaskRuntime, error) {
	taskID := strings.TrimSpace(s.TaskID)
	if taskID == "" {
		return reactor.TaskRuntime{}, fmt.Errorf("runtime snapshot task_id is required")
	}
	if rebuilder.StateMachine == nil {
		return reactor.TaskRuntime{}, fmt.Errorf("runtime snapshot %q: state machine is required", taskID)
	}
	effectTimeout := s.EffectTimeout
	if effectTimeout <= 0 {
		effectTimeout = rebuilder.DefaultEffectTimeout
	}
	maxConcurrentEffects := s.MaxConcurrentEffects
	if maxConcurrentEffects <= 0 {
		maxConcurrentEffects = rebuilder.DefaultMaxConcurrentEffects
	}
	return reactor.TaskRuntime{
		TaskID:               taskID,
		Agent:                strings.TrimSpace(s.Agent),
		StateMachine:         rebuilder.StateMachine,
		Executors:            rebuilder.Executors,
		EffectTimeout:        effectTimeout,
		MaxConcurrentEffects: maxConcurrentEffects,
		Metadata:             cloneStringMap(s.Metadata),
	}, nil
}

func RestoreRuntimeRegistry(ctx context.Context, registry *reactor.RuntimeRegistry, store RuntimeStore, rebuilder RuntimeRebuilder) ([]reactor.TaskRuntime, error) {
	if registry == nil {
		return nil, fmt.Errorf("runtime registry is nil")
	}
	if store == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	snapshots, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{})
	for _, runtime := range registry.List() {
		existing[runtime.TaskID] = struct{}{}
	}

	restored := make([]reactor.TaskRuntime, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if _, ok := existing[snapshot.TaskID]; ok {
			continue
		}
		runtime, err := snapshot.TaskRuntime(rebuilder)
		if err != nil {
			return restored, err
		}
		if err := registry.Register(runtime); err != nil {
			return restored, err
		}
		existing[runtime.TaskID] = struct{}{}
		restored = append(restored, runtime.Clone())
	}
	return restored, nil
}

func MarshalJSON(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
