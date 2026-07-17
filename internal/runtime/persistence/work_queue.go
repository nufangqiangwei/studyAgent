package persistence

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WorkKind string

const (
	WorkEvent  WorkKind = "event"
	WorkEffect WorkKind = "effect"
)

type QueuedWork struct {
	Kind   WorkKind
	TaskID string
}

// WorkQueue persists the event/effect hand-off used by resumable runs. It is
// append-only so a process can stop after dispatch and reconstruct pending work
// after restart.
type WorkQueue struct {
	mu         sync.Mutex
	eventPath  string
	effectPath string
}

type queuedEventRecord struct {
	ID        string         `json:"id"`
	TaskID    string         `json:"task_id"`
	Status    string         `json:"status"`
	WrittenAt time.Time      `json:"written_at"`
	Event     eventbus.Event `json:"event,omitempty"`
}

type queuedEffectRecord struct {
	ID        string         `json:"id"`
	TaskID    string         `json:"task_id"`
	Status    string         `json:"status"`
	WrittenAt time.Time      `json:"written_at"`
	Effect    reactor.Effect `json:"effect,omitempty"`
}

func NewWorkQueue(root string) (*WorkQueue, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("work queue: root is required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create work queue %s: %w", root, err)
	}
	return &WorkQueue{
		eventPath:  filepath.Join(root, "events.jsonl"),
		effectPath: filepath.Join(root, "effects.jsonl"),
	}, nil
}

func (q *WorkQueue) DispatchEffect(ctx context.Context, request reactor.EffectDispatchRequest) error {
	if request.OnDone != nil {
		defer request.OnDone()
	}
	return q.EnqueueEffect(ctx, request.Effect)
}

func (q *WorkQueue) EnqueueEvent(ctx context.Context, event eventbus.Event) error {
	if q == nil {
		return fmt.Errorf("work queue is nil")
	}
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("queued event id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return appendWorkRecord(ctx, q.eventPath, queuedEventRecord{
		ID: event.ID, TaskID: event.TaskID, Status: "pending",
		WrittenAt: time.Now().UTC(), Event: event.Clone(),
	})
}

func (q *WorkQueue) EnqueueEffect(ctx context.Context, effect reactor.Effect) error {
	if q == nil {
		return fmt.Errorf("work queue is nil")
	}
	if strings.TrimSpace(effect.ID) == "" {
		return fmt.Errorf("queued effect id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return appendWorkRecord(ctx, q.effectPath, queuedEffectRecord{
		ID: effect.ID, TaskID: effect.TaskID, Status: "pending",
		WrittenAt: time.Now().UTC(), Effect: effect.Clone(),
	})
}

func (q *WorkQueue) PopEvent(ctx context.Context, taskID string) (eventbus.Event, bool, error) {
	if q == nil {
		return eventbus.Event{}, false, fmt.Errorf("work queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, taskID)
	if err != nil || len(events) == 0 {
		return eventbus.Event{}, false, err
	}
	event := events[0]
	if err := appendWorkRecord(ctx, q.eventPath, queuedEventRecord{
		ID: event.ID, TaskID: event.TaskID, Status: "done", WrittenAt: time.Now().UTC(),
	}); err != nil {
		return eventbus.Event{}, false, err
	}
	return event.Clone(), true, nil
}

func (q *WorkQueue) PopEffect(ctx context.Context, taskID string) (reactor.Effect, bool, error) {
	if q == nil {
		return reactor.Effect{}, false, fmt.Errorf("work queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	effects, err := q.pendingEffectsLocked(ctx, taskID)
	if err != nil || len(effects) == 0 {
		return reactor.Effect{}, false, err
	}
	effect := effects[0]
	if err := appendWorkRecord(ctx, q.effectPath, queuedEffectRecord{
		ID: effect.ID, TaskID: effect.TaskID, Status: "done", WrittenAt: time.Now().UTC(),
	}); err != nil {
		return reactor.Effect{}, false, err
	}
	return effect.Clone(), true, nil
}

func (q *WorkQueue) PendingCounts(ctx context.Context, taskID string) (int, int, error) {
	if q == nil {
		return 0, 0, fmt.Errorf("work queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, taskID)
	if err != nil {
		return 0, 0, err
	}
	effects, err := q.pendingEffectsLocked(ctx, taskID)
	if err != nil {
		return 0, 0, err
	}
	return len(events), len(effects), nil
}

func (q *WorkQueue) Next(ctx context.Context) (QueuedWork, bool, error) {
	if q == nil {
		return QueuedWork{}, false, fmt.Errorf("work queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, "")
	if err != nil {
		return QueuedWork{}, false, err
	}
	if len(events) > 0 {
		return QueuedWork{Kind: WorkEvent, TaskID: events[0].TaskID}, true, nil
	}
	effects, err := q.pendingEffectsLocked(ctx, "")
	if err != nil {
		return QueuedWork{}, false, err
	}
	if len(effects) > 0 {
		return QueuedWork{Kind: WorkEffect, TaskID: effects[0].TaskID}, true, nil
	}
	return QueuedWork{}, false, nil
}

func (q *WorkQueue) pendingEventsLocked(ctx context.Context, taskID string) ([]eventbus.Event, error) {
	records, err := readWorkRecords[queuedEventRecord](ctx, q.eventPath)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]eventbus.Event)
	order := make([]string, 0, len(records))
	for _, record := range records {
		if taskID != "" && record.TaskID != taskID || record.ID == "" {
			continue
		}
		if record.Status == "done" {
			delete(pending, record.ID)
			continue
		}
		if _, exists := pending[record.ID]; !exists {
			order = append(order, record.ID)
		}
		pending[record.ID] = record.Event.Clone()
	}
	out := make([]eventbus.Event, 0, len(order))
	for _, id := range order {
		if event, ok := pending[id]; ok {
			out = append(out, event.Clone())
		}
	}
	return out, nil
}

func (q *WorkQueue) pendingEffectsLocked(ctx context.Context, taskID string) ([]reactor.Effect, error) {
	records, err := readWorkRecords[queuedEffectRecord](ctx, q.effectPath)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]reactor.Effect)
	order := make([]string, 0, len(records))
	for _, record := range records {
		if taskID != "" && record.TaskID != taskID || record.ID == "" {
			continue
		}
		if record.Status == "done" {
			delete(pending, record.ID)
			continue
		}
		if _, exists := pending[record.ID]; !exists {
			order = append(order, record.ID)
		}
		pending[record.ID] = record.Effect.Clone()
	}
	out := make([]reactor.Effect, 0, len(order))
	for _, id := range order {
		if effect, ok := pending[id]; ok {
			out = append(out, effect.Clone())
		}
	}
	return out, nil
}

func appendWorkRecord(ctx context.Context, path string, record any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create queue directory for %s: %w", path, err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal queue record: %w", err)
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open queue file %s: %w", path, err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write queue file %s: %w", path, err)
	}
	return nil
}

func readWorkRecords[T any](ctx context.Context, path string) ([]T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read queue file %s: %w", path, err)
	}
	lines := strings.Split(string(data), "\n")
	records := make([]T, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record T
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("parse queue file %s: %w", path, err)
		}
		records = append(records, record)
	}
	return records, nil
}
