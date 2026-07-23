package main

import (
	"agent/serviceruntime/contract"
	"agent/services/approval"
	"agent/services/interaction"
	"agent/services/webgateway"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type runtimeIngressFunc func(context.Context, contract.Message) error

func (f runtimeIngressFunc) Send(ctx context.Context, message contract.Message) error {
	return f(ctx, message)
}

type adapterTestIDs struct {
	mu   sync.Mutex
	next int
}

func (g *adapterTestIDs) New(kind string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("%s-%d", kind, g.next), nil
}

func (*adapterTestIDs) Derive(kind string, parts ...string) string {
	return kind + "-" + strings.Join(parts, "-")
}

func TestRuntimeAdapterCreateTaskRegistersWaiterBeforeSending(t *testing.T) {
	now := time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)
	var adapter *RuntimeAdapter
	var sent contract.Message
	ingress := runtimeIngressFunc(func(ctx context.Context, message contract.Message) error {
		sent = message.Clone()
		var request webgateway.CreateTaskRequest
		if err := json.Unmarshal(message.Payload, &request); err != nil {
			t.Fatal(err)
		}
		// Synchronous delivery proves the waiter already exists before Send.
		return adapter.Present(ctx, webgateway.Presentation{
			PresentationID: "presentation-create-1",
			RequestID:      request.RequestID,
			Operation:      webgateway.OperationCreate,
			Created: &webgateway.TaskCreatedPresentation{
				RequestID: request.RequestID,
				Task: webgateway.TaskDTO{
					TaskID: request.TaskID, GoalID: request.GoalID, UserID: "user-1", Title: request.Title,
					Input: request.Input, Phase: "created", CreatedAt: now, UpdatedAt: now,
				},
			},
		})
	})
	var err error
	adapter, err = newTestRuntimeAdapter(ingress, &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	task, err := adapter.CreateTask(context.Background(), Actor{UserID: "user-1"}, CreateTaskInput{
		GoalID: "goal-1", Title: "demo", Input: "do work",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.TaskID != "task-web-request-1" || task.GoalID != "goal-1" || task.UserID != "user-1" ||
		task.Title != "demo" || task.Input != "do work" || task.Phase != "created" {
		t.Fatalf("task=%#v", task)
	}
	if sent.ID != "message-web.adapter-create-web-request-1" ||
		sent.Kind != contract.MessageCommand ||
		sent.Type != webgateway.CreateTaskMessageType ||
		sent.Version != webgateway.ProtocolVersion ||
		sent.From != webAdapterAddress || sent.To != webgateway.DefaultAddress ||
		sent.RuntimeID != "runtime-test" || sent.PlanRevision != "revision-test" ||
		sent.UserID != "user-1" || sent.GoalID != "goal-1" ||
		sent.CorrelationID != "web-request-1" {
		t.Fatalf("message=%#v", sent)
	}
	var payload webgateway.CreateTaskRequest
	if err := json.Unmarshal(sent.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.RequestID != "web-request-1" || payload.TaskID != "task-web-request-1" ||
		payload.GoalID != "goal-1" || payload.Title != "demo" || payload.Input != "do work" {
		t.Fatalf("payload=%#v", payload)
	}
}

func TestRuntimeAdapterCreateFailureReturnsConfirmedTaskID(t *testing.T) {
	var adapter *RuntimeAdapter
	ingress := runtimeIngressFunc(func(ctx context.Context, message contract.Message) error {
		var request webgateway.CreateTaskRequest
		if err := json.Unmarshal(message.Payload, &request); err != nil {
			t.Fatal(err)
		}
		return adapter.Present(ctx, webgateway.Presentation{
			PresentationID: "presentation-create-failed",
			RequestID:      request.RequestID,
			Operation:      webgateway.OperationCreate,
			Error: &webgateway.ErrorDTO{
				Code: "web_task_request_failed", Message: "later saga stage failed", TaskID: request.TaskID,
			},
		})
	})
	var err error
	adapter, err = newTestRuntimeAdapter(ingress, &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	task, err := adapter.CreateTask(
		context.Background(),
		Actor{UserID: "user-1"},
		CreateTaskInput{Input: "do work"},
	)
	if !errors.Is(err, errRuntimeAdapterInternal) {
		t.Fatalf("err=%v", err)
	}
	if task.TaskID != "task-web-request-1" {
		t.Fatalf("confirmed task id was lost on create failure: %#v", task)
	}
}

func TestRuntimeAdapterGetTaskMapsPresentationAndStableErrors(t *testing.T) {
	now := time.Date(2026, 7, 23, 2, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		code    string
		wantErr error
	}{
		{name: "found"},
		{name: "not found", code: "web_task_not_found", wantErr: ErrTaskNotFound},
		{name: "conflict", code: "web_task_conflict", wantErr: ErrTaskConflict},
		{name: "unavailable", code: "runtime_unavailable", wantErr: ErrRuntimeUnavailable},
		{name: "internal", code: "web_task_request_failed", wantErr: errRuntimeAdapterInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var adapter *RuntimeAdapter
			ingress := runtimeIngressFunc(func(ctx context.Context, message contract.Message) error {
				if message.Kind != contract.MessageCommand || message.Type != webgateway.GetTaskMessageType ||
					message.From != webAdapterAddress || message.To != webgateway.DefaultAddress ||
					message.UserID != "user-1" || message.CorrelationID == "" {
					t.Fatalf("message=%#v", message)
				}
				var request webgateway.GetTaskRequest
				if err := json.Unmarshal(message.Payload, &request); err != nil {
					t.Fatal(err)
				}
				presentation := webgateway.Presentation{
					PresentationID: "presentation-" + test.name,
					RequestID:      request.RequestID,
					Operation:      webgateway.OperationGet,
				}
				if test.code == "" {
					presentation.Found = &webgateway.TaskFoundPresentation{
						RequestID: request.RequestID,
						Task: webgateway.TaskDTO{
							TaskID: request.TaskID, UserID: "user-1", Input: "input", Phase: "running",
							CreatedAt: now, UpdatedAt: now.Add(time.Minute),
						},
					}
				} else {
					presentation.Error = &webgateway.ErrorDTO{Code: test.code, Message: "not exposed"}
				}
				return adapter.Present(ctx, presentation)
			})
			var err error
			adapter, err = newTestRuntimeAdapter(ingress, &adapterTestIDs{}, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()

			task, err := adapter.GetTask(context.Background(), Actor{UserID: "user-1"}, "task-1")
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("err=%v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if task.TaskID != "task-1" || task.UserID != "user-1" || task.Input != "input" || task.Phase != "running" ||
				!task.CreatedAt.Equal(now) || !task.UpdatedAt.Equal(now.Add(time.Minute)) {
				t.Fatalf("task=%#v", task)
			}
		})
	}
}

func TestRuntimeAdapterCancellationLeavesDurableRequestAndAcceptsLatePresentation(t *testing.T) {
	sent := make(chan contract.Message, 1)
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(_ context.Context, message contract.Message) error {
		sent <- message.Clone()
		return nil
	}), &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, callErr := adapter.GetTask(ctx, Actor{UserID: "user-1"}, "task-1")
		result <- callErr
	}()
	message := <-sent
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}

	var request webgateway.GetTaskRequest
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		t.Fatal(err)
	}
	late := webgateway.Presentation{
		PresentationID: "late-presentation", RequestID: request.RequestID,
		Operation: webgateway.OperationGet,
		Found: &webgateway.TaskFoundPresentation{
			RequestID: request.RequestID,
			Task: webgateway.TaskDTO{
				TaskID: "task-1", UserID: "user-1", Input: "input", Phase: "created",
				CreatedAt: time.Now(), UpdatedAt: time.Now(),
			},
		},
	}
	if err := adapter.Present(context.Background(), late); err != nil {
		t.Fatalf("late presentation: %v", err)
	}
	if err := adapter.Present(context.Background(), late); err != nil {
		t.Fatalf("duplicate late presentation: %v", err)
	}
	if len(sent) != 0 {
		t.Fatal("cancellation sent an unexpected Runtime message")
	}
}

func TestRuntimeAdapterIngressFailureMapsToUnavailable(t *testing.T) {
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		return errors.New("transport details must not escape")
	}), &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	_, err = adapter.GetTask(context.Background(), Actor{UserID: "user-1"}, "task-1")
	if !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestRuntimeAdapterAvailabilityRejectsBeforeDurableSend(t *testing.T) {
	called := false
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		called = true
		return nil
	}), &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	adapter.setAvailability(func() bool { return false })

	_, err = adapter.GetTask(context.Background(), Actor{UserID: "user-1"}, "task-1")
	if !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if called {
		t.Fatal("unavailable adapter sent a durable Runtime message")
	}
}

func TestRuntimeAdapterApprovalIsolationMultipleSubscribersAndDeduplication(t *testing.T) {
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		return nil
	}), &adapterTestIDs{}, 32, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ctxOther, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	user1a, _ := adapter.SubscribeApprovalRequests(ctx1, Actor{UserID: "user-1"})
	user1b, _ := adapter.SubscribeApprovalRequests(ctx2, Actor{UserID: "user-1"})
	user2, _ := adapter.SubscribeApprovalRequests(ctxOther, Actor{UserID: "user-2"})

	presentation := approvalPresentation("approval/presentation-1", "approval-1", "user-1")
	if err := adapter.PresentInteraction(context.Background(), presentation); err != nil {
		t.Fatal(err)
	}
	if err := adapter.PresentInteraction(context.Background(), presentation); err != nil {
		t.Fatal(err)
	}
	for name, events := range map[string]<-chan ApprovalRequest{"first": user1a, "second": user1b} {
		select {
		case event := <-events:
			if event.ApprovalID != "approval-1" || event.UserID != "user-1" ||
				event.CapabilityRef != "workspace.write" || event.ArgumentsDigest != "sha256:test" {
				t.Fatalf("%s event=%#v", name, event)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s subscriber did not receive approval", name)
		}
		select {
		case duplicate := <-events:
			t.Fatalf("%s received duplicate %#v", name, duplicate)
		default:
		}
	}
	select {
	case event := <-user2:
		t.Fatalf("other user received %#v", event)
	default:
	}
}

func TestRuntimeAdapterSlowApprovalSubscriberIsClosedWithoutBlocking(t *testing.T) {
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		return nil
	}), &adapterTestIDs{}, 32, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	events, err := adapter.SubscribeApprovalRequests(context.Background(), Actor{UserID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}

	if err := adapter.PresentInteraction(context.Background(), approvalPresentation("approval/1", "approval-1", "user-1")); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := adapter.PresentInteraction(context.Background(), approvalPresentation("approval/2", "approval-2", "user-1")); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("slow subscriber blocked approval presentation")
	}
	first, open := <-events
	if !open || first.ApprovalID != "approval-1" {
		t.Fatalf("first=%#v open=%t", first, open)
	}
	if _, open := <-events; open {
		t.Fatal("overflowed subscription was not closed")
	}
}

func TestRuntimeAdapterSubscriptionCancellationClosesChannel(t *testing.T) {
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		return nil
	}), &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	ctx, cancel := context.WithCancel(context.Background())
	events, err := adapter.SubscribeApprovalRequests(ctx, Actor{UserID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, open := <-events:
		if open {
			t.Fatal("canceled subscription remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled subscription was not closed")
	}
}

func TestRuntimeAdapterCloseReleasesWaitersAndSubscribers(t *testing.T) {
	sent := make(chan struct{}, 1)
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		sent <- struct{}{}
		return nil
	}), &adapterTestIDs{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	events, err := adapter.SubscribeApprovalRequests(context.Background(), Actor{UserID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, callErr := adapter.CreateTask(context.Background(), Actor{UserID: "user-1"}, CreateTaskInput{Input: "work"})
		result <- callErr
	}()
	<-sent
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("waiter err=%v", err)
	}
	if _, open := <-events; open {
		t.Fatal("approval subscription remained open")
	}
	if _, err := adapter.SubscribeApprovalRequests(context.Background(), Actor{UserID: "user-1"}); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("subscribe err=%v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestRuntimeAdapterConcurrentApprovalDeliveryIsRaceFriendly(t *testing.T) {
	const count = 48
	adapter, err := newTestRuntimeAdapter(runtimeIngressFunc(func(context.Context, contract.Message) error {
		return nil
	}), &adapterTestIDs{}, count*2, count)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := adapter.SubscribeApprovalRequests(ctx, Actor{UserID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	wait.Add(count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			defer wait.Done()
			presentation := approvalPresentation(
				fmt.Sprintf("approval/presentation-%d", index),
				fmt.Sprintf("approval-%d", index),
				"user-1",
			)
			if presentErr := adapter.PresentInteraction(context.Background(), presentation); presentErr != nil {
				t.Errorf("present: %v", presentErr)
			}
		}()
	}
	wait.Wait()
	received := make(map[string]struct{}, count)
	for index := 0; index < count; index++ {
		select {
		case event := <-events:
			received[event.ApprovalID] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("received %d/%d approvals", len(received), count)
		}
	}
	if len(received) != count {
		t.Fatalf("unique approvals=%d, want %d", len(received), count)
	}
}

func newTestRuntimeAdapter(
	ingress RuntimeIngress,
	ids contract.IDGenerator,
	dedupCapacity int,
	approvalBuffer int,
) (*RuntimeAdapter, error) {
	return NewRuntimeAdapter(RuntimeAdapterOptions{
		Ingress: ingress, RuntimeID: "runtime-test", PlanRevision: "revision-test", IDs: ids,
		PresentationDedupCapacity: dedupCapacity, ApprovalSubscriberBuffer: approvalBuffer,
	})
}

func approvalPresentation(id, approvalID, userID string) interaction.Presentation {
	requestedAt := time.Date(2026, 7, 23, 3, 0, 0, 0, time.UTC)
	expiresAt := requestedAt.Add(time.Hour)
	return interaction.Presentation{
		ID: id, Kind: interaction.PresentationApproval,
		Approval: &approval.Requested{
			ApprovalID: approvalID, CallID: "call-" + approvalID, UserID: userID,
			CapabilityRef: "workspace.write", CapabilityVersion: "v1",
			RiskSummary: "write files", ArgumentsDigest: "sha256:test",
			RequestedAt: requestedAt, ExpiresAt: &expiresAt,
		},
	}
}
