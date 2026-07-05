package eventbus

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type DeliveryMode string

const (
	DeliverySync  DeliveryMode = "sync"
	DeliveryAsync DeliveryMode = "async"
)

type BusOption func(*busConfig)

type busConfig struct {
	queueSize   int
	workerCount int
	logger      Logger
}

func WithQueueSize(size int) BusOption {
	return func(config *busConfig) {
		config.queueSize = size
	}
}

func WithWorkerCount(count int) BusOption {
	return func(config *busConfig) {
		config.workerCount = count
	}
}

func WithLogger(logger Logger) BusOption {
	return func(config *busConfig) {
		config.logger = logger
	}
}

type Bus struct {
	mu            sync.RWMutex
	subscriptions map[string]registeredSubscription
	nextSubID     int
	queue         chan queuedEvent
	logger        Logger
	closed        bool
	workers       sync.WaitGroup
}

type queuedEvent struct {
	event     Event
	broadcast bool
}

type PublishResult struct {
	Event      Event            `json:"event"`
	Mode       DeliveryMode     `json:"mode"`
	Broadcast  bool             `json:"broadcast"`
	Queued     bool             `json:"queued"`
	QueueDepth int              `json:"queue_depth,omitempty"`
	Deliveries []DeliveryResult `json:"deliveries,omitempty"`
}

type DeliveryResult struct {
	SubscriptionID string `json:"subscription_id"`
	Matched        bool   `json:"matched"`
	Error          string `json:"error,omitempty"`
}

func New(options ...BusOption) (*Bus, error) {
	config := busConfig{
		queueSize:   128,
		workerCount: 1,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.queueSize < 0 {
		return nil, fmt.Errorf("event bus: queue size must be >= 0")
	}
	if config.workerCount < 1 {
		return nil, fmt.Errorf("event bus: worker count must be >= 1")
	}

	bus := &Bus{
		subscriptions: make(map[string]registeredSubscription),
		queue:         make(chan queuedEvent, config.queueSize),
		logger:        config.logger,
	}
	for i := 0; i < config.workerCount; i++ {
		bus.workers.Add(1)
		go bus.worker()
	}
	return bus, nil
}

func (b *Bus) Subscribe(filter Filter, handler Handler, options ...SubscribeOption) (Subscription, error) {
	if b == nil {
		return Subscription{}, fmt.Errorf("event bus is nil")
	}
	config := subscribeConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return Subscription{}, ErrBusClosed
	}
	b.nextSubID++
	order := b.nextSubID
	id := config.id
	if id == "" {
		id = fmt.Sprintf("sub_%d", order)
	}
	subscription := Subscription{
		ID:       id,
		Filter:   filter,
		ReadOnly: config.readOnly,
	}
	if err := validateSubscription(subscription, handler); err != nil {
		b.mu.Unlock()
		return Subscription{}, err
	}
	if _, exists := b.subscriptions[subscription.ID]; exists {
		b.mu.Unlock()
		return Subscription{}, fmt.Errorf("subscription %q: already exists", subscription.ID)
	}
	// 检查是否有同一个task ID下的相同的事件的订阅者。
	for _, existing := range b.subscriptions {
		if existing.subscription.ReadOnly || subscription.ReadOnly {
			continue
		}
		if existing.subscription.Filter.Overlaps(subscription.Filter) {
			b.mu.Unlock()
			return Subscription{}, fmt.Errorf("subscription %q: filter overlaps with subscription %q", subscription.ID, existing.subscription.ID)
		}
	}

	b.subscriptions[subscription.ID] = registeredSubscription{
		subscription: subscription,
		handler:      handler,
		order:        order,
	}
	b.mu.Unlock()
	b.log(context.Background(), LogEntry{
		Action:         LogSubscribe,
		SubscriptionID: subscription.ID,
	})
	return subscription, nil
}

func (b *Bus) SubscribeFunc(filter Filter, handler func(context.Context, Event) error, options ...SubscribeOption) (Subscription, error) {
	return b.Subscribe(filter, HandlerFunc(handler), options...)
}

func (b *Bus) SubscribeReadOnly(filter Filter, handler Handler, options ...SubscribeOption) (Subscription, error) {
	options = append(options, WithReadOnly())
	return b.Subscribe(filter, handler, options...)
}

func (b *Bus) SubscribeReadOnlyFunc(filter Filter, handler func(context.Context, Event) error, options ...SubscribeOption) (Subscription, error) {
	return b.SubscribeReadOnly(filter, HandlerFunc(handler), options...)
}

func (b *Bus) Unsubscribe(id string) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	if _, exists := b.subscriptions[id]; !exists {
		b.mu.Unlock()
		return false
	}
	delete(b.subscriptions, id)
	b.mu.Unlock()
	b.log(context.Background(), LogEntry{
		Action:         LogUnsubscribe,
		SubscriptionID: id,
	})
	return true
}

func (b *Bus) Subscriptions() []Subscription {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()

	subscriptions := make([]Subscription, 0, len(b.subscriptions))
	for _, registered := range b.subscriptions {
		subscriptions = append(subscriptions, registered.subscription)
	}
	sort.Slice(subscriptions, func(i, j int) bool {
		return subscriptions[i].ID < subscriptions[j].ID
	})
	return subscriptions
}

func (b *Bus) Publish(ctx context.Context, event Event) (PublishResult, error) {
	if b == nil {
		return PublishResult{}, fmt.Errorf("event bus is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	event, err := completeEvent(event)
	if err != nil {
		return PublishResult{}, err
	}
	b.log(ctx, logEntry(LogPublishSync, DeliverySync, event, "", 0, nil))
	return b.deliver(ctx, event, false, DeliverySync)
}

func (b *Bus) PublishAsync(ctx context.Context, event Event) (PublishResult, error) {
	return b.enqueue(ctx, event, false)
}

func (b *Bus) Enqueue(ctx context.Context, event Event) (PublishResult, error) {
	return b.PublishAsync(ctx, event)
}

func (b *Bus) Broadcast(ctx context.Context, event Event) (PublishResult, error) {
	if b == nil {
		return PublishResult{}, fmt.Errorf("event bus is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	event, err := completeEvent(event)
	if err != nil {
		return PublishResult{}, err
	}
	b.log(ctx, logEntry(LogBroadcastSync, DeliverySync, event, "", 0, nil))
	return b.deliver(ctx, event, true, DeliverySync)
}

func (b *Bus) BroadcastAsync(ctx context.Context, event Event) (PublishResult, error) {
	return b.enqueue(ctx, event, true)
}

func (b *Bus) QueueDepth() int {
	if b == nil || b.queue == nil {
		return 0
	}
	return len(b.queue)
}

func (b *Bus) Close() error {
	if b == nil {
		return nil
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.queue)
	b.mu.Unlock()

	b.workers.Wait()
	b.log(context.Background(), LogEntry{Action: LogBusClosed})
	return nil
}

func (b *Bus) enqueue(ctx context.Context, event Event, broadcast bool) (PublishResult, error) {
	if b == nil {
		return PublishResult{}, fmt.Errorf("event bus is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	event, err := completeEvent(event)
	if err != nil {
		return PublishResult{}, err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return PublishResult{}, ErrBusClosed
	}
	select {
	case b.queue <- queuedEvent{event: event, broadcast: broadcast}:
		depth := len(b.queue)
		b.mu.RUnlock()
		action := LogPublishAsyncQueued
		if broadcast {
			action = LogBroadcastAsyncQueued
		}
		b.log(ctx, logEntry(action, DeliveryAsync, event, "", depth, nil))
		return PublishResult{
			Event:      event.Clone(),
			Mode:       DeliveryAsync,
			Broadcast:  broadcast,
			Queued:     true,
			QueueDepth: depth,
		}, nil
	case <-ctx.Done():
		b.mu.RUnlock()
		return PublishResult{}, ctx.Err()
	}
}

func (b *Bus) worker() {
	defer b.workers.Done()
	for queued := range b.queue {
		_, _ = b.deliver(context.Background(), queued.event, queued.broadcast, DeliveryAsync)
	}
}

func (b *Bus) deliver(ctx context.Context, event Event, broadcast bool, mode DeliveryMode) (PublishResult, error) {
	subscribers := b.matchingSubscribers(event, broadcast)
	result := PublishResult{
		Event:     event.Clone(),
		Mode:      mode,
		Broadcast: broadcast,
		Queued:    false,
	}

	var failures DeliveryErrors
	for _, subscriber := range subscribers {
		if err := ctx.Err(); err != nil {
			result.Deliveries = append(result.Deliveries, DeliveryResult{
				SubscriptionID: subscriber.subscription.ID,
				Matched:        true,
				Error:          err.Error(),
			})
			failures = append(failures, DeliveryError{SubscriptionID: subscriber.subscription.ID, Err: err})
			break
		}
		subID := subscriber.subscription.ID
		b.log(ctx, logEntry(LogDeliverStarted, mode, event, subID, 0, nil))
		err := subscriber.handler.HandleEvent(ctx, event.Clone())
		delivery := DeliveryResult{
			SubscriptionID: subID,
			Matched:        true,
		}
		if err != nil {
			delivery.Error = err.Error()
			failures = append(failures, DeliveryError{SubscriptionID: subID, Err: err})
			b.log(ctx, logEntry(LogDeliverFailed, mode, event, subID, 0, err))
		} else {
			b.log(ctx, logEntry(LogDeliverCompleted, mode, event, subID, 0, nil))
		}
		result.Deliveries = append(result.Deliveries, delivery)
	}
	if len(failures) > 0 {
		return result, failures
	}
	return result, nil
}

func (b *Bus) matchingSubscribers(event Event, broadcast bool) []registeredSubscription {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()

	matched := make([]registeredSubscription, 0, len(b.subscriptions))
	for _, subscriber := range b.subscriptions {
		if broadcast || subscriber.subscription.Filter.Match(event) {
			matched = append(matched, subscriber)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].order < matched[j].order
	})
	return matched
}

func (b *Bus) log(ctx context.Context, entry LogEntry) {
	if b == nil || b.logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if entry.Time.IsZero() {
		entry.Time = time.Now().UTC()
	}
	b.logger.Log(ctx, entry)
}

func logEntry(action string, mode DeliveryMode, event Event, subscriptionID string, queueDepth int, err error) LogEntry {
	entry := LogEntry{
		Action:         action,
		Mode:           mode,
		EventID:        event.ID,
		Topic:          event.Topic,
		EventType:      event.Type,
		TaskID:         event.TaskID,
		SubscriptionID: subscriptionID,
		QueueDepth:     queueDepth,
	}
	if err != nil {
		entry.Error = err.Error()
	}
	return entry
}
