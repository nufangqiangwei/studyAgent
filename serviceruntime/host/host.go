package host

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	leaseguard "agent/serviceruntime/lease"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ServiceHost struct {
	plan            *building.RuntimePlan
	plans           building.PlanResolver
	definitions     building.DefinitionResolver
	activator       activation.Activator
	storage         persistence.RuntimeStorage
	ids             contract.IDGenerator
	clock           contract.Clock
	snapshots       SnapshotPolicy
	observer        contract.RuntimeEventRecorder
	ownerID         string
	inboxLease      time.Duration
	activationLease time.Duration
	maxAttempts     int
	retryPolicy     fault.RetryPolicy
	locks           *lockPool

	mu       sync.RWMutex
	started  bool
	draining bool
	stopped  bool
}

type Options struct {
	Plan            *building.RuntimePlan
	Plans           building.PlanResolver
	Definitions     building.DefinitionResolver
	Activator       activation.Activator
	Storage         persistence.RuntimeStorage
	IDs             contract.IDGenerator
	Clock           contract.Clock
	Snapshots       SnapshotPolicy
	Observer        contract.RuntimeEventRecorder
	OwnerID         string
	InboxLease      time.Duration
	ActivationLease time.Duration
	MaxAttempts     int
	RetryPolicy     fault.RetryPolicy
}

func New(options Options) (*ServiceHost, error) {
	if options.Plan == nil || options.Definitions == nil || options.Activator == nil || options.Storage == nil || options.IDs == nil {
		return nil, fmt.Errorf("service host requires plan, definitions, activator, storage and id generator")
	}
	if options.OwnerID == "" {
		return nil, fmt.Errorf("service host owner id is required")
	}
	if options.Plans == nil {
		options.Plans = building.NewPlanCatalog(options.Plan)
	}
	policy := options.Plan.Recovery()
	if options.InboxLease <= 0 {
		options.InboxLease = policy.InboxLease
	}
	if options.ActivationLease <= 0 {
		options.ActivationLease = policy.ActivationLease
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = policy.MaxDeliveryAttempts
	}
	if options.Snapshots == nil {
		options.Snapshots = EveryNEvents{N: policy.SnapshotEveryEvents}
	}
	if options.RetryPolicy == nil {
		options.RetryPolicy = fault.ExponentialRetryPolicy{}
	}
	return &ServiceHost{
		plan: options.Plan, plans: options.Plans, definitions: options.Definitions, activator: options.Activator, storage: options.Storage,
		ids: options.IDs, clock: options.Clock, snapshots: options.Snapshots,
		observer: options.Observer, ownerID: options.OwnerID,
		inboxLease: options.InboxLease, activationLease: options.ActivationLease,
		maxAttempts: options.MaxAttempts, retryPolicy: options.RetryPolicy,
		locks: newLockPool(),
	}, nil
}

func (h *ServiceHost) Start(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("service host is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return fmt.Errorf("service host is stopped")
	}
	h.started = true
	h.draining = false
	return nil
}

func (h *ServiceHost) Drain(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("service host is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return fmt.Errorf("service host is stopped")
	}
	h.draining = true
	h.mu.Unlock()
	return nil
}

func (h *ServiceHost) Stop(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	h.mu.Lock()
	h.started = false
	h.draining = false
	h.stopped = true
	h.mu.Unlock()
	return nil
}

func (h *ServiceHost) HandleNext(ctx context.Context, instanceID contract.ServiceInstanceID) (HandleResult, error) {
	if h == nil {
		return HandleResult{}, fmt.Errorf("service host is nil")
	}
	h.mu.RLock()
	available := h.started && !h.draining && !h.stopped
	h.mu.RUnlock()
	if !available {
		return HandleResult{}, fmt.Errorf("service host is not accepting work")
	}
	record, found, err := h.storage.Instances().Get(ctx, instanceID)
	if err != nil {
		return HandleResult{}, err
	}
	if !found {
		return HandleResult{}, fmt.Errorf("service instance %q not found", instanceID)
	}
	claim, ok, err := h.storage.Inbox().ClaimNext(ctx, record.MailboxID, h.ownerID, h.inboxLease)
	if err != nil || !ok {
		return HandleResult{Status: HandleIdle, InstanceID: instanceID}, err
	}
	return h.handleClaim(ctx, claim)
}

func (h *ServiceHost) handleClaim(ctx context.Context, claim persistence.InboxClaim) (HandleResult, error) {
	instanceID := claim.Record.InstanceID
	unlock := h.locks.lock(instanceID)
	defer unlock()
	message := claim.Record.Message
	result := HandleResult{InstanceID: instanceID, MessageID: message.ID}
	if message.Deadline != nil && !message.Deadline.After(h.now()) {
		err := h.runtimeError(fault.Permanent, "handle_message", result, fmt.Errorf("message deadline has expired"))
		_ = h.storage.Inbox().MoveToDeadLetter(ctx, claim, err)
		result.Status = HandleDeadLetter
		return result, err
	}
	active, err := h.activator.Activate(ctx, instanceID)
	if err != nil {
		return h.failClaim(ctx, claim, result, err)
	}
	if active.Instance.RuntimeID != message.RuntimeID || active.Instance.PlanRevision != message.PlanRevision || active.Instance.MailboxID != claim.Record.MailboxID {
		return h.failClaim(ctx, claim, result, fmt.Errorf("claimed message does not match the active service instance"))
	}
	heartbeat := leaseguard.Start(ctx, leaseguard.Interval(h.inboxLease, h.activationLease), func(renewCtx context.Context) error {
		if renewErr := h.storage.Inbox().RenewClaim(renewCtx, claim, h.inboxLease); renewErr != nil {
			return renewErr
		}
		return h.activator.Renew(renewCtx, instanceID)
	})
	defer heartbeat.Stop()
	state, sequence := active.Current()
	definition, found := h.definitions.ResolveDefinition(active.Instance.DefinitionRef)
	if !found {
		return h.failClaim(ctx, claim, result, fmt.Errorf("service definition %q not found", active.Instance.DefinitionRef.String()))
	}
	if err := validateConsumed(definition, message); err != nil {
		err = h.runtimeError(fault.Validation, "validate_consumed", result, err)
		_ = h.storage.Inbox().MoveToDeadLetter(ctx, claim, err)
		result.Status = HandleDeadLetter
		return result, err
	}
	handleCtx, cancelHandle := messageContext(heartbeat.Context(), message)
	defer cancelHandle()
	handleCtx = request.WithMessageContext(handleCtx, message)
	handleCtx = request.WithClient(handleCtx, active.Requests)
	decision, err := active.Service.Handle(handleCtx, state.Clone(), message.Clone())
	if err != nil {
		return h.failClaim(ctx, claim, result, err)
	}
	if err := heartbeat.Err(); err != nil {
		_ = h.activator.Passivate(ctx, instanceID)
		result.Status = HandleStale
		return h.failClaim(ctx, claim, result, fault.Wrap(fault.LeaseLost, "renew_handle_lease", err))
	}
	messagePlan, found := h.plans.ResolvePlan(message.RuntimeID, message.PlanRevision)
	if !found {
		return h.failClaim(ctx, claim, result, h.runtimeError(fault.Permanent, "resolve_message_plan", result, fmt.Errorf("plan revision %q is not available", message.PlanRevision)))
	}
	if err := decision.ValidateAt(message, messagePlan.KnowsEffect, h.now()); err != nil {
		err = h.runtimeError(fault.Validation, "validate_decision", result, err)
		_ = h.storage.Inbox().MoveToDeadLetter(ctx, claim, err)
		result.Status = HandleDeadLetter
		return result, err
	}
	if err := validateProduced(definition, decision); err != nil {
		err = h.runtimeError(fault.Validation, "validate_produced", result, err)
		_ = h.storage.Inbox().MoveToDeadLetter(ctx, claim, err)
		result.Status = HandleDeadLetter
		return result, err
	}
	now := h.now()
	events, nextState, err := h.materializeEvents(active, state, sequence, message, decision.Events, now)
	if err != nil {
		return h.failClaim(ctx, claim, result, err)
	}
	outbox := h.materializeOutgoing(active, message, decision, now)
	effects := h.materializeEffects(active, message, decision.Effects, now)
	lastSequence := sequence + uint64(len(events))
	var snapshot *contract.Snapshot
	if h.snapshots.ShouldSnapshot(SnapshotDecision{
		InstanceID: instanceID, StreamID: active.Instance.StateStreamID,
		PreviousSequence: sequence, CurrentSequence: lastSequence,
		EventsApplied: len(events), StateBytes: len(nextState.Data),
	}) {
		value := contract.Snapshot{
			StreamID: active.Instance.StateStreamID, AggregateType: string(active.Instance.DefinitionRef.Type),
			OwnerService: active.Instance.Address, PlanRevision: active.Instance.PlanRevision,
			SchemaVersion: nextState.SchemaVersion, LastSequence: lastSequence,
			State: contract.CloneRaw(nextState.Data), Checksum: contract.StateChecksum(nextState.Data), CreatedAt: now,
		}
		snapshot = &value
	}
	commit, err := h.storage.Committer().CommitMessage(heartbeat.Context(), persistence.MessageCommit{
		RuntimeID: active.Instance.RuntimeID, PlanRevision: active.Instance.PlanRevision,
		InstanceID: instanceID, ActivationEpoch: active.CurrentLease().Epoch,
		Ack:      persistence.InboxAck{InboxID: claim.Record.InboxID, MessageID: message.ID, LeaseToken: claim.LeaseToken, AckedAt: now},
		StreamID: active.Instance.StateStreamID, ExpectedSequence: sequence,
		Events: events, Snapshot: snapshot, Outbox: outbox, Effects: effects,
	})
	if err != nil {
		err = h.classifyPersistenceError("commit_message", result, err)
		kind := fault.KindOf(err)
		if kind == fault.Conflict || kind == fault.StaleActivation || kind == fault.LeaseLost {
			_ = h.activator.Passivate(ctx, instanceID)
		}
		if kind == fault.StaleActivation || kind == fault.LeaseLost {
			result.Status = HandleStale
		} else {
			result.Status = HandleRetry
		}
		return h.failClaim(ctx, claim, result, err)
	}
	if commit.Duplicate {
		result.Status = HandleDuplicate
		result.LastSequence = commit.LastSequence
		_ = h.activator.Passivate(ctx, instanceID)
		return result, nil
	}
	active.CommitState(nextState, commit.LastSequence)
	result.Status = HandleCommitted
	result.StreamID = active.Instance.StateStreamID
	result.LastSequence = commit.LastSequence
	result.EventIDs = append([]string(nil), commit.StoredEventIDs...)
	result.OutboxIDs = append([]string(nil), commit.StoredOutboxIDs...)
	result.EffectIDs = append([]string(nil), commit.StoredEffectIDs...)
	h.record(ctx, contract.RuntimeCommitCompleted, active, message, commit.LastSequence)
	return result, nil
}

func (h *ServiceHost) materializeEvents(active *activation.Activation, state service.State, sequence uint64, message contract.Message, decisions []service.NewEvent, now time.Time) ([]contract.StoredEvent, service.State, error) {
	events := make([]contract.StoredEvent, 0, len(decisions))
	next := state.Clone()
	correlationID := message.CorrelationID
	if correlationID == "" {
		correlationID = message.ID
	}
	for index, decided := range decisions {
		event := contract.StoredEvent{
			EventID:  h.ids.Derive("event", message.ID, decided.Key),
			StreamID: active.Instance.StateStreamID, StreamType: string(active.Instance.DefinitionRef.Type),
			Sequence: sequence + uint64(index) + 1, EventType: decided.Type, EventVersion: decided.Version,
			PlanRevision: active.Instance.PlanRevision, ServiceVersion: active.Instance.DefinitionRef.Version,
			CorrelationID: correlationID, CausationID: message.ID,
			Payload: contract.CloneRaw(decided.Payload), Metadata: contract.CloneStrings(decided.Metadata), OccurredAt: now,
		}
		var err error
		next, err = active.Service.Apply(next, event)
		if err != nil {
			return nil, state, fmt.Errorf("apply new event %q: %w", event.EventID, err)
		}
		events = append(events, event)
	}
	return events, next.Clone(), nil
}

func (h *ServiceHost) materializeOutgoing(active *activation.Activation, input contract.Message, decision service.Decision, now time.Time) []persistence.OutboxRecord {
	correlationID := input.CorrelationID
	if correlationID == "" {
		correlationID = input.ID
	}
	result := make([]persistence.OutboxRecord, 0, len(decision.Outgoing)+1)
	for _, outgoing := range decision.Outgoing {
		streamID := outgoing.StreamID
		if streamID == "" {
			streamID = input.StreamID
		}
		outgoingCorrelationID := outgoing.CorrelationID
		if outgoingCorrelationID == "" {
			outgoingCorrelationID = correlationID
		}
		outgoingCausationID := outgoing.CausationID
		if outgoingCausationID == "" {
			outgoingCausationID = input.ID
		}
		message := contract.Message{
			ID: h.ids.Derive("message", input.ID, outgoing.Key), Kind: outgoing.Kind, Type: outgoing.Type, Version: outgoing.Version,
			From: active.Instance.Address, To: outgoing.To, ReplyTo: outgoing.ReplyTo,
			RuntimeID: input.RuntimeID, PlanRevision: input.PlanRevision,
			UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
			CorrelationID: outgoingCorrelationID, CausationID: outgoingCausationID, StreamID: streamID,
			Deadline: cloneTime(outgoing.Deadline), Payload: contract.CloneRaw(outgoing.Payload), Metadata: contract.CloneStrings(outgoing.Metadata),
		}
		result = append(result, persistence.OutboxRecord{
			OutboxID: h.ids.Derive("outbox", input.ID, outgoing.Key), InstanceID: active.Instance.InstanceID,
			Message: message, Status: persistence.OutboxPending, AvailableAt: now, CreatedAt: now,
		})
	}
	if decision.Reply != nil {
		payload := contract.CloneRaw(decision.Reply.Payload)
		metadata := contract.CloneStrings(decision.Reply.Metadata)
		if decision.Reply.Error != nil {
			payload, _ = json.Marshal(decision.Reply.Error)
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata[contract.MetadataReplyError] = "true"
		}
		message := contract.Message{
			ID: h.ids.Derive("reply", input.ID, decision.Reply.Key), Kind: contract.MessageReply,
			Type: decision.Reply.Type, Version: decision.Reply.Version,
			From: active.Instance.Address, To: input.ReplyTo,
			RuntimeID: input.RuntimeID, PlanRevision: input.PlanRevision,
			UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
			CorrelationID: correlationID, CausationID: input.ID, StreamID: input.StreamID,
			Payload: payload, Metadata: metadata,
		}
		result = append(result, persistence.OutboxRecord{
			OutboxID: h.ids.Derive("outbox", input.ID, decision.Reply.Key), InstanceID: active.Instance.InstanceID,
			Message: message, Status: persistence.OutboxPending, AvailableAt: now, CreatedAt: now,
		})
	}
	return result
}

func (h *ServiceHost) materializeEffects(active *activation.Activation, input contract.Message, decisions []service.PlannedEffect, now time.Time) []persistence.EffectRecord {
	result := make([]persistence.EffectRecord, 0, len(decisions))
	for _, decided := range decisions {
		result = append(result, persistence.EffectRecord{
			EffectID: h.ids.Derive("effect", input.ID, decided.Key), RuntimeID: input.RuntimeID,
			PlanRevision: input.PlanRevision, InstanceID: active.Instance.InstanceID,
			SourceMessageID: input.ID, Type: decided.Type, Version: decided.Version,
			ExecutorRef: decided.ExecutorRef, IdempotencyKey: decided.IdempotencyKey,
			Status: persistence.EffectPlanned, Payload: contract.CloneRaw(decided.Payload), PlannedAt: now,
			Deadline: cloneTime(decided.Deadline), Metadata: contract.CloneStrings(decided.Metadata),
		})
	}
	return result
}

func (h *ServiceHost) failClaim(ctx context.Context, claim persistence.InboxClaim, result HandleResult, cause error) (HandleResult, error) {
	cause = h.classifyPersistenceError("handle_message", result, cause)
	if fault.KindOf(cause) == fault.CorruptState {
		_ = h.storage.Inbox().ReleaseClaim(ctx, claim, h.now(), cause)
		result.Status = HandleCorrupt
		return result, cause
	}
	decision := h.retryPolicy.DecideRetry(fault.RetryInput{
		Error: cause, Attempt: claim.Record.Attempt, MaxAttempts: h.maxAttempts, Now: h.now(),
	})
	if !decision.Retry {
		_ = h.storage.Inbox().MoveToDeadLetter(ctx, claim, cause)
		result.Status = HandleDeadLetter
	} else {
		_ = h.storage.Inbox().ReleaseClaim(ctx, claim, decision.RetryAt, cause)
		if result.Status != HandleStale {
			result.Status = HandleRetry
		}
	}
	return result, cause
}

func (h *ServiceHost) classifyPersistenceError(operation string, result HandleResult, cause error) error {
	var runtimeErr *fault.RuntimeError
	if errors.As(cause, &runtimeErr) {
		return cause
	}
	kind := fault.Retryable
	switch {
	case errors.Is(cause, persistence.ErrSequenceConflict), errors.Is(cause, persistence.ErrDuplicateID):
		kind = fault.Conflict
	case errors.Is(cause, persistence.ErrStaleActivation):
		kind = fault.StaleActivation
	case errors.Is(cause, persistence.ErrLeaseLost):
		kind = fault.LeaseLost
	}
	return h.runtimeError(kind, operation, result, cause)
}

func (h *ServiceHost) runtimeError(kind fault.Kind, operation string, result HandleResult, cause error) error {
	spec := h.plan.Runtime()
	value := fault.New(kind, operation, cause)
	value.RuntimeID = spec.ID
	value.InstanceID = result.InstanceID
	value.MessageID = result.MessageID
	return value
}

func (h *ServiceHost) now() time.Time {
	if h.clock == nil {
		return time.Now().UTC()
	}
	return h.clock.Now().UTC()
}

func (h *ServiceHost) record(ctx context.Context, eventType contract.RuntimeEventType, active *activation.Activation, message contract.Message, sequence uint64) {
	if h.observer == nil {
		return
	}
	id, err := h.ids.New("runtime-event")
	if err != nil {
		return
	}
	_ = h.observer.RecordRuntimeEvent(ctx, contract.RuntimeEvent{
		ID: id, Type: eventType, RuntimeID: active.Instance.RuntimeID, PlanRevision: active.Instance.PlanRevision,
		InstanceID: active.Instance.InstanceID, ServiceAddress: active.Instance.Address,
		MessageID: message.ID, StreamID: active.Instance.StateStreamID, Sequence: sequence, OccurredAt: h.now(),
	})
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func messageContext(ctx context.Context, message contract.Message) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if message.Deadline == nil {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, *message.Deadline)
}

func validateConsumed(definition building.ServiceDefinition, message contract.Message) error {
	for _, consumed := range definition.Consumes {
		if consumed.Kind == message.Kind && consumed.Type == message.Type && consumed.Version == message.Version {
			return nil
		}
	}
	return fmt.Errorf("service %q does not consume %s %q version %d", definition.Component.String(), message.Kind, message.Type, message.Version)
}

func validateProduced(definition building.ServiceDefinition, decision service.Decision) error {
	for _, outgoing := range decision.Outgoing {
		if !declaresMessage(definition.Produces, outgoing.Kind, outgoing.Type, outgoing.Version) {
			return fmt.Errorf("service %q does not declare producing %s %q version %d", definition.Component.String(), outgoing.Kind, outgoing.Type, outgoing.Version)
		}
	}
	if decision.Reply != nil && !declaresMessage(definition.Produces, contract.MessageReply, decision.Reply.Type, decision.Reply.Version) {
		return fmt.Errorf("service %q does not declare producing reply %q version %d", definition.Component.String(), decision.Reply.Type, decision.Reply.Version)
	}
	allowedEffects := make(map[string]struct{}, len(definition.EffectExecutors))
	for _, ref := range definition.EffectExecutors {
		allowedEffects[ref] = struct{}{}
	}
	for _, planned := range decision.Effects {
		if _, ok := allowedEffects[planned.ExecutorRef]; !ok {
			return fmt.Errorf("service %q does not declare effect executor %q", definition.Component.String(), planned.ExecutorRef)
		}
	}
	return nil
}

func declaresMessage(contracts []building.MessageContract, kind contract.MessageKind, messageType contract.MessageType, version int) bool {
	for _, declared := range contracts {
		if declared.Kind == kind && declared.Type == messageType && declared.Version == version {
			return true
		}
	}
	return false
}
