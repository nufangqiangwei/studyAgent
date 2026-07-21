package transport

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/instance"
	leaseguard "agent/serviceruntime/lease"
	"agent/serviceruntime/persistence"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrDeliveryPaused = errors.New("event bus delivery is paused")
var ErrBusClosed = errors.New("event bus is closed")

type DeliveryMode string

const (
	DeliveryPaused DeliveryMode = "paused"
	DeliveryLive   DeliveryMode = "live"
	DeliveryDrain  DeliveryMode = "draining"
	DeliveryClosed DeliveryMode = "closed"
)

type DeliveryReceipt struct {
	Address    contract.ServiceAddress
	InstanceID contract.ServiceInstanceID
	MailboxID  contract.MailboxID
	Accepted   bool
	Duplicate  bool
}

type PublishResult struct {
	MessageID string
	Targets   []DeliveryReceipt
	Duplicate bool
}

type DispatchResult struct {
	OutboxID  string
	MessageID string
	Delivered int
	Duplicate int
	Failed    int
	Idle      bool
}

type EventBus interface {
	Mode() DeliveryMode
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Drain(ctx context.Context) error
	Publish(ctx context.Context, message contract.Message) (PublishResult, error)
	DispatchNextOutbox(ctx context.Context, ownerID string) (DispatchResult, error)
	Close() error
}

type ReplySink interface {
	Address() contract.ServiceAddress
	DeliverReply(ctx context.Context, message contract.Message) error
}

type Bus struct {
	plan        *building.RuntimePlan
	plans       building.PlanResolver
	resolver    instance.AddressResolver
	inbox       persistence.InboxStore
	outbox      persistence.OutboxStore
	sequences   persistence.MessageSequenceStore
	clock       contract.Clock
	observer    contract.RuntimeEventRecorder
	ids         contract.IDGenerator
	outboxLease time.Duration
	maxAttempts int
	retryPolicy fault.RetryPolicy
	replySink   ReplySink

	mu   sync.RWMutex
	mode DeliveryMode
}

type Options struct {
	Plan        *building.RuntimePlan
	Plans       building.PlanResolver
	Resolver    instance.AddressResolver
	Inbox       persistence.InboxStore
	Outbox      persistence.OutboxStore
	Sequences   persistence.MessageSequenceStore
	Clock       contract.Clock
	Observer    contract.RuntimeEventRecorder
	IDs         contract.IDGenerator
	OutboxLease time.Duration
	MaxAttempts int
	RetryPolicy fault.RetryPolicy
	ReplySink   ReplySink
}

func New(options Options) (*Bus, error) {
	if options.Plan == nil || options.Resolver == nil || options.Inbox == nil || options.Outbox == nil || options.Sequences == nil {
		return nil, fmt.Errorf("event bus requires plan, resolver, inbox and outbox")
	}
	if options.Plans == nil {
		options.Plans = building.NewPlanCatalog(options.Plan)
	}
	policy := options.Plan.Recovery()
	if options.OutboxLease <= 0 {
		options.OutboxLease = policy.OutboxLease
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = policy.MaxDeliveryAttempts
	}
	if options.RetryPolicy == nil {
		options.RetryPolicy = fault.ExponentialRetryPolicy{}
	}
	return &Bus{
		plan: options.Plan, plans: options.Plans, resolver: options.Resolver,
		inbox: options.Inbox, outbox: options.Outbox, sequences: options.Sequences, clock: options.Clock,
		observer: options.Observer, ids: options.IDs,
		outboxLease: options.OutboxLease, maxAttempts: options.MaxAttempts, retryPolicy: options.RetryPolicy,
		replySink: options.ReplySink,
		mode:      DeliveryPaused,
	}, nil
}

func (b *Bus) Mode() DeliveryMode {
	if b == nil {
		return DeliveryClosed
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mode
}

func (b *Bus) Pause(ctx context.Context) error  { return b.setMode(ctx, DeliveryPaused) }
func (b *Bus) Resume(ctx context.Context) error { return b.setMode(ctx, DeliveryLive) }
func (b *Bus) Drain(ctx context.Context) error  { return b.setMode(ctx, DeliveryDrain) }

func (b *Bus) setMode(ctx context.Context, mode DeliveryMode) error {
	if b == nil {
		return ErrBusClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mode == DeliveryClosed {
		return ErrBusClosed
	}
	b.mode = mode
	return nil
}

func (b *Bus) Publish(ctx context.Context, message contract.Message) (PublishResult, error) {
	if b == nil {
		return PublishResult{}, ErrBusClosed
	}
	mode := b.Mode()
	if mode == DeliveryClosed {
		return PublishResult{}, ErrBusClosed
	}
	// Paused stops live dispatch and message handling during recovery. Durable
	// ingress remains available so external sources cannot lose data while
	// service resources are being restored.
	if err := message.Validate(); err != nil {
		return PublishResult{}, fault.Wrap(fault.Validation, "validate_message", err)
	}
	spec := b.plan.Runtime()
	if message.RuntimeID != spec.ID {
		return PublishResult{}, fault.Wrap(fault.Validation, "validate_message_runtime", fmt.Errorf("message runtime does not match the event bus runtime"))
	}
	messagePlan, found := b.plans.ResolvePlan(message.RuntimeID, message.PlanRevision)
	if !found {
		return PublishResult{}, fault.Wrap(fault.Permanent, "resolve_message_plan", fmt.Errorf("message plan revision %q is not available", message.PlanRevision))
	}
	if message.Kind == contract.MessageReply && b.replySink != nil && message.To == b.replySink.Address() {
		assigned, assignErr := b.sequences.Assign(ctx, "reply/"+string(message.To), message)
		if assignErr != nil {
			return PublishResult{}, fault.Wrap(fault.Conflict, "assign_reply_sequence", assignErr)
		}
		if err := b.replySink.DeliverReply(ctx, assigned); err != nil {
			return PublishResult{MessageID: message.ID}, err
		}
		b.record(ctx, contract.RuntimeDeliveryCompleted, assigned, map[string]string{"reply_sink": string(message.To)})
		return PublishResult{MessageID: message.ID, Targets: []DeliveryReceipt{{Address: message.To, Accepted: true}}}, nil
	}
	addresses, err := messagePlan.Routing().Resolve(message)
	if err != nil {
		return PublishResult{}, fault.Wrap(fault.Permanent, "resolve_message_route", err)
	}
	targets := make([]instance.DeliveryTarget, 0, len(addresses))
	for _, address := range addresses {
		target, resolveErr := b.resolver.ResolveAddress(ctx, message.RuntimeID, message.PlanRevision, address)
		if resolveErr != nil {
			return PublishResult{}, fault.Wrap(fault.NotFound, "resolve_delivery_target", resolveErr)
		}
		targets = append(targets, target)
	}
	result := PublishResult{MessageID: message.ID, Targets: make([]DeliveryReceipt, 0, len(targets))}
	for _, target := range targets {
		assigned, assignErr := b.sequences.Assign(ctx, "mailbox/"+string(target.MailboxID), message)
		if assignErr != nil {
			return result, fault.Wrap(fault.Conflict, "assign_message_sequence", fmt.Errorf("target %q: %w", target.Address, assignErr))
		}
		_, duplicate, enqueueErr := b.inbox.Enqueue(ctx, target, assigned)
		receipt := DeliveryReceipt{Address: target.Address, InstanceID: target.InstanceID, MailboxID: target.MailboxID, Accepted: enqueueErr == nil, Duplicate: duplicate}
		result.Targets = append(result.Targets, receipt)
		if duplicate {
			result.Duplicate = true
		}
		if enqueueErr != nil {
			return result, enqueueErr
		}
	}
	b.record(ctx, contract.RuntimeDeliveryCompleted, message, map[string]string{"targets": fmt.Sprint(len(targets))})
	return result, nil
}

func (b *Bus) DispatchNextOutbox(ctx context.Context, ownerID string) (DispatchResult, error) {
	if b == nil {
		return DispatchResult{}, ErrBusClosed
	}
	if b.Mode() == DeliveryPaused {
		return DispatchResult{}, ErrDeliveryPaused
	}
	spec := b.plan.Runtime()
	claim, ok, err := b.outbox.ClaimNext(ctx, spec.ID, ownerID, b.outboxLease)
	if err != nil || !ok {
		return DispatchResult{Idle: !ok}, err
	}
	heartbeat := leaseguard.Start(ctx, leaseguard.Interval(b.outboxLease), func(renewCtx context.Context) error {
		return b.outbox.RenewClaim(renewCtx, claim, b.outboxLease)
	})
	defer heartbeat.Stop()
	published, publishErr := b.Publish(heartbeat.Context(), claim.Record.Message)
	if publishErr == nil && heartbeat.Err() != nil {
		publishErr = fmt.Errorf("renew outbox claim: %w", heartbeat.Err())
	}
	result := DispatchResult{OutboxID: claim.Record.OutboxID, MessageID: claim.Record.Message.ID}
	for _, receipt := range published.Targets {
		if !receipt.Accepted {
			result.Failed++
		} else if receipt.Duplicate {
			result.Duplicate++
		} else {
			result.Delivered++
		}
	}
	if publishErr == nil {
		err = b.outbox.MarkDelivered(ctx, claim, persistence.DeliverySummary{MessageID: result.MessageID, Delivered: result.Delivered, Duplicate: result.Duplicate, Failed: result.Failed})
		return result, err
	}
	retry := b.retryPolicy.DecideRetry(fault.RetryInput{Error: publishErr, Attempt: claim.Record.Attempt, MaxAttempts: b.maxAttempts, Now: b.now()})
	if !retry.Retry {
		err = b.outbox.MoveToDeadLetter(ctx, claim, publishErr)
	} else {
		err = b.outbox.MarkRetry(ctx, claim, retry.RetryAt, publishErr)
	}
	if err != nil {
		return result, err
	}
	return result, publishErr
}

func (b *Bus) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	b.mode = DeliveryClosed
	b.mu.Unlock()
	return nil
}

func (b *Bus) now() time.Time {
	if b.clock == nil {
		return time.Now().UTC()
	}
	return b.clock.Now().UTC()
}

func (b *Bus) record(ctx context.Context, eventType contract.RuntimeEventType, message contract.Message, attributes map[string]string) {
	if b.observer == nil || b.ids == nil {
		return
	}
	id, err := b.ids.New("runtime-event")
	if err != nil {
		return
	}
	_ = b.observer.RecordRuntimeEvent(ctx, contract.RuntimeEvent{
		ID: id, Type: eventType, RuntimeID: message.RuntimeID, PlanRevision: message.PlanRevision,
		MessageID: message.ID, OccurredAt: b.now(), Attributes: attributes,
	})
}
