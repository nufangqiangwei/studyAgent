package eventbus

import (
	"context"
	"time"
)

const (
	LogSubscribe            = "subscribe"
	LogUnsubscribe          = "unsubscribe"
	LogPublishSync          = "publish.sync"
	LogPublishAsyncQueued   = "publish.async.queued"
	LogBroadcastSync        = "broadcast.sync"
	LogBroadcastAsyncQueued = "broadcast.async.queued"
	LogDeliverStarted       = "deliver.started"
	LogDeliverCompleted     = "deliver.completed"
	LogDeliverFailed        = "deliver.failed"
	LogBusClosed            = "bus.closed"
)

type LogEntry struct {
	Time           time.Time    `json:"time"`
	Action         string       `json:"action"`
	Mode           DeliveryMode `json:"mode,omitempty"`
	EventID        string       `json:"event_id,omitempty"`
	Topic          string       `json:"topic,omitempty"`
	EventType      EventType    `json:"event_type,omitempty"`
	TaskID         string       `json:"task_id,omitempty"`
	SubscriptionID string       `json:"subscription_id,omitempty"`
	QueueDepth     int          `json:"queue_depth,omitempty"`
	Error          string       `json:"error,omitempty"`
}

type Logger interface {
	Log(ctx context.Context, entry LogEntry)
}

type LogFunc func(ctx context.Context, entry LogEntry)

func (f LogFunc) Log(ctx context.Context, entry LogEntry) {
	if f != nil {
		f(ctx, entry)
	}
}
