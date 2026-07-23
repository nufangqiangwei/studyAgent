package main

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/contract"
	"agent/services/interaction"
	"agent/services/webgateway"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	webAdapterAddress            contract.ServiceAddress = "web.adapter"
	defaultPresentationDedupSize                         = 1024
	defaultApprovalBufferSize                            = 16
)

var errRuntimeAdapterInternal = errors.New("runtime request could not be completed")

// RuntimeIngress is the adapter's entire Runtime-facing surface. Sending a
// Message durably enqueues it; it must not synchronously execute a Service.
type RuntimeIngress interface {
	Send(context.Context, contract.Message) error
}

// RuntimeAdapterOptions contains only the immutable message context and the
// narrow durable ingress needed by the Web adapter.
type RuntimeAdapterOptions struct {
	Ingress      RuntimeIngress
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	IDs          contract.IDGenerator

	PresentationDedupCapacity int
	ApprovalSubscriberBuffer  int
}

// RuntimeAdapter correlates durable Gateway presentations with current HTTP
// waiters and fans committed approval presentations out to Web subscribers.
// Its in-memory state is only a response optimization; the Gateway remains the
// owner of durable request correctness.
type RuntimeAdapter struct {
	ingress      RuntimeIngress
	runtimeID    contract.RuntimeID
	planRevision contract.PlanRevision
	ids          contract.IDGenerator

	dedupCapacity int
	approvalSize  int

	mu            sync.Mutex
	availability  func() bool
	closed        bool
	waiters       map[string]*runtimeWaiter
	subscriptions map[string]map[uint64]*approvalSubscription
	nextSubID     uint64
	seen          map[string]struct{}
	seenOrder     []string
}

type runtimeWaiter struct {
	operation webgateway.Operation
	result    chan runtimeResult
}

type runtimeResult struct {
	task TaskView
	err  error
}

type approvalSubscription struct {
	id     uint64
	ctx    context.Context
	events chan ApprovalRequest
	done   chan struct{}
}

func NewRuntimeAdapter(options RuntimeAdapterOptions) (*RuntimeAdapter, error) {
	if options.Ingress == nil {
		return nil, fmt.Errorf("runtime ingress is required")
	}
	if strings.TrimSpace(string(options.RuntimeID)) == "" ||
		strings.TrimSpace(string(options.PlanRevision)) == "" {
		return nil, fmt.Errorf("runtime id and plan revision are required")
	}
	if options.IDs == nil {
		options.IDs = serviceruntime.StableIDs{}
	}
	if options.PresentationDedupCapacity <= 0 {
		options.PresentationDedupCapacity = defaultPresentationDedupSize
	}
	if options.ApprovalSubscriberBuffer <= 0 {
		options.ApprovalSubscriberBuffer = defaultApprovalBufferSize
	}
	return &RuntimeAdapter{
		ingress: options.Ingress, runtimeID: options.RuntimeID, planRevision: options.PlanRevision,
		ids: options.IDs, dedupCapacity: options.PresentationDedupCapacity,
		approvalSize: options.ApprovalSubscriberBuffer,
		waiters:      make(map[string]*runtimeWaiter), subscriptions: make(map[string]map[uint64]*approvalSubscription),
		seen: make(map[string]struct{}),
	}, nil
}

func (a *RuntimeAdapter) CreateTask(ctx context.Context, actor Actor, input CreateTaskInput) (TaskView, error) {
	requestID, messageID, err := a.newRequestIdentity(webgateway.OperationCreate)
	if err != nil {
		return TaskView{}, err
	}
	if strings.TrimSpace(input.TaskID) == "" {
		input.TaskID = a.ids.Derive("task", requestID)
		if strings.TrimSpace(input.TaskID) == "" {
			return TaskView{}, errRuntimeAdapterInternal
		}
	}
	payload, err := json.Marshal(webgateway.CreateTaskRequest{
		RequestID: requestID,
		TaskID:    input.TaskID,
		GoalID:    input.GoalID,
		Title:     input.Title,
		Input:     input.Input,
	})
	if err != nil {
		return TaskView{}, errRuntimeAdapterInternal
	}
	message := contract.Message{
		ID: messageID, Kind: contract.MessageCommand,
		Type: webgateway.CreateTaskMessageType, Version: webgateway.ProtocolVersion,
		From: webAdapterAddress, To: webgateway.DefaultAddress,
		RuntimeID: a.runtimeID, PlanRevision: a.planRevision,
		UserID: actor.UserID, GoalID: input.GoalID,
		CorrelationID: requestID, Payload: payload,
	}
	return a.sendAndWait(ctx, requestID, webgateway.OperationCreate, message)
}

func (a *RuntimeAdapter) GetTask(ctx context.Context, actor Actor, taskID string) (TaskView, error) {
	requestID, messageID, err := a.newRequestIdentity(webgateway.OperationGet)
	if err != nil {
		return TaskView{}, err
	}
	payload, err := json.Marshal(webgateway.GetTaskRequest{RequestID: requestID, TaskID: taskID})
	if err != nil {
		return TaskView{}, errRuntimeAdapterInternal
	}
	message := contract.Message{
		ID: messageID, Kind: contract.MessageCommand,
		Type: webgateway.GetTaskMessageType, Version: webgateway.ProtocolVersion,
		From: webAdapterAddress, To: webgateway.DefaultAddress,
		RuntimeID: a.runtimeID, PlanRevision: a.planRevision,
		UserID: actor.UserID, CorrelationID: requestID, Payload: payload,
	}
	return a.sendAndWait(ctx, requestID, webgateway.OperationGet, message)
}

func (a *RuntimeAdapter) newRequestIdentity(operation webgateway.Operation) (string, string, error) {
	if a == nil || a.ids == nil {
		return "", "", errRuntimeAdapterInternal
	}
	requestID, err := a.ids.New("web-request")
	if err != nil || strings.TrimSpace(requestID) == "" {
		return "", "", errRuntimeAdapterInternal
	}
	messageID := a.ids.Derive("message", string(webAdapterAddress), string(operation), requestID)
	if strings.TrimSpace(messageID) == "" {
		return "", "", errRuntimeAdapterInternal
	}
	return requestID, messageID, nil
}

func (a *RuntimeAdapter) sendAndWait(
	ctx context.Context,
	requestID string,
	operation webgateway.Operation,
	message contract.Message,
) (TaskView, error) {
	if ctx == nil {
		return TaskView{}, errRuntimeAdapterInternal
	}
	if err := ctx.Err(); err != nil {
		return TaskView{}, err
	}
	waiter := &runtimeWaiter{operation: operation, result: make(chan runtimeResult, 1)}
	if err := a.registerWaiter(requestID, waiter); err != nil {
		return TaskView{}, err
	}

	if err := a.ingress.Send(ctx, message); err != nil {
		removed := a.removeWaiter(requestID, waiter)
		if !removed {
			return receiveCompletedWaiter(waiter)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return TaskView{}, ctxErr
		}
		return TaskView{}, ErrRuntimeUnavailable
	}

	select {
	case result := <-waiter.result:
		return result.task, result.err
	case <-ctx.Done():
		if a.removeWaiter(requestID, waiter) {
			return TaskView{}, ctx.Err()
		}
		return receiveCompletedWaiter(waiter)
	}
}

func receiveCompletedWaiter(waiter *runtimeWaiter) (TaskView, error) {
	result, open := <-waiter.result
	if !open {
		return TaskView{}, ErrRuntimeUnavailable
	}
	return result.task, result.err
}

func (a *RuntimeAdapter) registerWaiter(requestID string, waiter *runtimeWaiter) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.availability != nil && !a.availability() {
		return ErrRuntimeUnavailable
	}
	if _, exists := a.waiters[requestID]; exists {
		return errRuntimeAdapterInternal
	}
	a.waiters[requestID] = waiter
	return nil
}

func (a *RuntimeAdapter) removeWaiter(requestID string, waiter *runtimeWaiter) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, exists := a.waiters[requestID]
	if !exists || current != waiter {
		return false
	}
	delete(a.waiters, requestID)
	return true
}

// Present implements webgateway.Presenter. Late results are acknowledged after
// deduplication even when their process-local HTTP waiter no longer exists.
func (a *RuntimeAdapter) Present(_ context.Context, presentation webgateway.Presentation) error {
	result := taskPresentationResult(presentation)
	key := "webgateway/" + presentation.PresentationID

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if a.seenLocked(key) {
		return nil
	}
	a.rememberLocked(key)
	waiter, exists := a.waiters[presentation.RequestID]
	if !exists {
		return nil
	}
	delete(a.waiters, presentation.RequestID)
	if waiter.operation != presentation.Operation {
		result = runtimeResult{err: errRuntimeAdapterInternal}
	}
	waiter.result <- result
	close(waiter.result)
	return nil
}

func taskPresentationResult(presentation webgateway.Presentation) runtimeResult {
	if presentation.Error != nil {
		switch presentation.Error.Code {
		case "web_task_not_found":
			return runtimeResult{err: ErrTaskNotFound}
		case "web_task_conflict":
			return runtimeResult{err: ErrTaskConflict}
		case "runtime_unavailable", "web_runtime_unavailable", "web_task_runtime_unavailable":
			return runtimeResult{err: ErrRuntimeUnavailable}
		default:
			return runtimeResult{err: errRuntimeAdapterInternal}
		}
	}
	switch presentation.Operation {
	case webgateway.OperationCreate:
		if presentation.Created == nil {
			return runtimeResult{err: errRuntimeAdapterInternal}
		}
		return runtimeResult{task: taskViewFromDTO(presentation.Created.Task)}
	case webgateway.OperationGet:
		if presentation.Found == nil {
			return runtimeResult{err: errRuntimeAdapterInternal}
		}
		return runtimeResult{task: taskViewFromDTO(presentation.Found.Task)}
	default:
		return runtimeResult{err: errRuntimeAdapterInternal}
	}
}

func taskViewFromDTO(value webgateway.TaskDTO) TaskView {
	result := TaskView{
		TaskID: value.TaskID, GoalID: value.GoalID, UserID: value.UserID, Title: value.Title, Input: value.Input,
		Phase: string(value.Phase), CreatedAt: value.CreatedAt.UTC(), UpdatedAt: value.UpdatedAt.UTC(),
	}
	if value.CompletedAt != nil {
		completed := value.CompletedAt.UTC()
		result.CompletedAt = &completed
	}
	return result
}

func (a *RuntimeAdapter) setAvailability(check func() bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.availability = check
	a.mu.Unlock()
}

// InteractionPresenter returns the interaction.Presenter view of this Hub.
// Go interfaces cannot overload Present for the two distinct presentation
// payload types, so module assembly uses this narrow typed view.
func (a *RuntimeAdapter) InteractionPresenter() interaction.Presenter {
	return interaction.PresenterFunc(a.PresentInteraction)
}

// PresentInteraction converts committed approval presentations into the Web
// DTO and fans them out without blocking the Effect worker.
func (a *RuntimeAdapter) PresentInteraction(_ context.Context, presentation interaction.Presentation) error {
	if presentation.Kind != interaction.PresentationApproval {
		return nil
	}
	if presentation.Approval == nil || strings.TrimSpace(presentation.ID) == "" {
		return errRuntimeAdapterInternal
	}
	value := presentation.Approval
	approval := ApprovalRequest{
		ApprovalID: value.ApprovalID, CallID: value.CallID, UserID: value.UserID,
		CapabilityRef: value.CapabilityRef, CapabilityVersion: value.CapabilityVersion,
		RiskSummary: value.RiskSummary, ArgumentsDigest: value.ArgumentsDigest,
		RequestedAt: value.RequestedAt.UTC(),
	}
	if value.ExpiresAt != nil {
		expires := value.ExpiresAt.UTC()
		approval.ExpiresAt = &expires
	}

	key := "interaction/" + presentation.ID
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if a.seenLocked(key) {
		return nil
	}
	a.rememberLocked(key)
	for id, subscription := range a.subscriptions[approval.UserID] {
		select {
		case <-subscription.ctx.Done():
			a.closeSubscriptionLocked(approval.UserID, id, subscription)
		default:
			select {
			case subscription.events <- approval:
			default:
				a.closeSubscriptionLocked(approval.UserID, id, subscription)
			}
		}
	}
	return nil
}

func (a *RuntimeAdapter) SubscribeApprovalRequests(ctx context.Context, actor Actor) (<-chan ApprovalRequest, error) {
	if ctx == nil || strings.TrimSpace(actor.UserID) == "" {
		return nil, errRuntimeAdapterInternal
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subscription := &approvalSubscription{
		ctx: ctx, events: make(chan ApprovalRequest, a.approvalSize), done: make(chan struct{}),
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ErrRuntimeUnavailable
	}
	a.nextSubID++
	subscription.id = a.nextSubID
	byUser := a.subscriptions[actor.UserID]
	if byUser == nil {
		byUser = make(map[uint64]*approvalSubscription)
		a.subscriptions[actor.UserID] = byUser
	}
	byUser[subscription.id] = subscription
	a.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			a.removeSubscription(actor.UserID, subscription)
		case <-subscription.done:
		}
	}()
	return subscription.events, nil
}

func (a *RuntimeAdapter) removeSubscription(userID string, subscription *approvalSubscription) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.subscriptions[userID][subscription.id]
	if current != subscription {
		return
	}
	a.closeSubscriptionLocked(userID, subscription.id, subscription)
}

func (a *RuntimeAdapter) closeSubscriptionLocked(userID string, id uint64, subscription *approvalSubscription) {
	byUser := a.subscriptions[userID]
	if byUser[id] != subscription {
		return
	}
	delete(byUser, id)
	if len(byUser) == 0 {
		delete(a.subscriptions, userID)
	}
	close(subscription.events)
	close(subscription.done)
}

func (a *RuntimeAdapter) seenLocked(id string) bool {
	_, exists := a.seen[id]
	return exists
}

func (a *RuntimeAdapter) rememberLocked(id string) {
	if a.dedupCapacity <= 0 || id == "" {
		return
	}
	if _, exists := a.seen[id]; exists {
		return
	}
	if len(a.seenOrder) == a.dedupCapacity {
		delete(a.seen, a.seenOrder[0])
		copy(a.seenOrder, a.seenOrder[1:])
		a.seenOrder[len(a.seenOrder)-1] = id
	} else {
		a.seenOrder = append(a.seenOrder, id)
	}
	a.seen[id] = struct{}{}
}

// Close marks Runtime ingress as unavailable, releases every waiter, and
// closes every approval stream. It is safe to call more than once.
func (a *RuntimeAdapter) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	for requestID, waiter := range a.waiters {
		delete(a.waiters, requestID)
		waiter.result <- runtimeResult{err: ErrRuntimeUnavailable}
		close(waiter.result)
	}
	for userID, byUser := range a.subscriptions {
		for id, subscription := range byUser {
			a.closeSubscriptionLocked(userID, id, subscription)
		}
	}
	return nil
}

var _ RuntimePort = (*RuntimeAdapter)(nil)
var _ webgateway.Presenter = (*RuntimeAdapter)(nil)
