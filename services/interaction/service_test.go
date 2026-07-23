package interaction

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestSubmitAndAgentCompletionAreEventSourced(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	target := &interactionService{address: DefaultAddress, agentAddress: agent.DefaultAddress, clock: fixedClock{now: now}}
	state, err := target.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	submitPayload, _ := json.Marshal(SubmitRequest{RequestID: "request-1", Input: "hello"})
	submit := contract.Message{
		ID: "submit-1", Kind: contract.MessageCommand, Type: SubmitMessageType, Version: ProtocolVersion,
		From: "cli.external", To: DefaultAddress, UserID: "user-1", RunID: "request-1",
		CorrelationID: "request-1", Payload: submitPayload,
	}
	decision, err := target.Handle(context.Background(), state, submit)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || len(decision.Outgoing) != 1 || len(decision.Effects) != 0 {
		t.Fatalf("submit decision=%#v", decision)
	}
	outgoing := decision.Outgoing[0]
	if outgoing.Kind != contract.MessageCommand || outgoing.Type != agent.ExecuteMessageType || outgoing.To != agent.DefaultAddress || outgoing.ReplyTo != DefaultAddress {
		t.Fatalf("agent outgoing=%#v", outgoing)
	}
	state, err = target.Apply(state, contract.StoredEvent{
		EventType: decision.Events[0].Type, EventVersion: decision.Events[0].Version, Payload: decision.Events[0].Payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	output := contract.ArtifactRef{Store: "test", Key: "answers/request-1.txt", ContentType: "text/plain"}
	completedPayload, _ := json.Marshal(agent.ExecuteResult{
		RunID: "request-1", Phase: agent.PhaseCompleted, Output: &output, Turns: 1,
	})
	completed := contract.Message{
		ID: "agent-completed-1", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: agent.DefaultAddress, To: DefaultAddress, UserID: "user-1", RunID: "request-1",
		CorrelationID: "request-1", Payload: completedPayload,
	}
	decision, err = target.Handle(context.Background(), state, completed)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || decision.Events[0].Type != requestCompletedEvent || len(decision.Effects) != 1 {
		t.Fatalf("completion decision=%#v", decision)
	}
	state, err = target.Apply(state, contract.StoredEvent{
		EventType: decision.Events[0].Type, EventVersion: decision.Events[0].Version, Payload: decision.Events[0].Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	request := restored.Requests["request-1"]
	if request.Phase != PhaseCompleted || request.Output == nil || request.Output.Key != output.Key || request.CompletedAt == nil {
		t.Fatalf("restored request=%#v", request)
	}
}

func TestAgentErrorProducesDurableFailureAndPresentation(t *testing.T) {
	now := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	target := &interactionService{address: DefaultAddress, agentAddress: agent.DefaultAddress, clock: fixedClock{now: now}}
	state, _ := target.InitialState(context.Background(), service.Init{})
	submitPayload, _ := json.Marshal(SubmitRequest{RequestID: "request-error", Input: "hello"})
	decision, err := target.Handle(context.Background(), state, contract.Message{
		ID: "submit-error", Kind: contract.MessageCommand, Type: SubmitMessageType, Version: ProtocolVersion,
		From: "cli.external", RunID: "request-error", Payload: submitPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err = target.Apply(state, contract.StoredEvent{
		EventType: decision.Events[0].Type, EventVersion: decision.Events[0].Version, Payload: decision.Events[0].Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	errorPayload, _ := json.Marshal(service.ReplyError{Code: "model_failed", Message: "model unavailable"})
	decision, err = target.Handle(context.Background(), state, contract.Message{
		ID: "agent-error", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: agent.DefaultAddress, RunID: "request-error", CorrelationID: "request-error", Payload: errorPayload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || decision.Events[0].Type != requestFailedEvent || len(decision.Effects) != 1 {
		t.Fatalf("failure decision=%#v", decision)
	}
	var presentation Presentation
	if err := json.Unmarshal(decision.Effects[0].Payload, &presentation); err != nil {
		t.Fatal(err)
	}
	if presentation.Kind != PresentationError || presentation.ErrorCode != "model_failed" || presentation.ErrorMessage != "model unavailable" {
		t.Fatalf("presentation=%#v", presentation)
	}
}

func TestTerminalProjectionRetainsLatestFivePersistedRequests(t *testing.T) {
	state, err := encodeState(initialState())
	if err != nil {
		t.Fatal(err)
	}
	target := &interactionService{}
	started := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for index := 0; index < RetainedTerminalRequests+2; index++ {
		requestID := fmt.Sprintf("request-%d", index)
		request := RequestState{
			RequestID: requestID, RunID: requestID, IdentityFingerprint: "fingerprint-" + requestID,
			Phase: PhaseRunning, StartedAt: started.Add(time.Duration(index) * time.Minute),
		}
		payload, _ := json.Marshal(request)
		state, err = target.Apply(state, contract.StoredEvent{
			EventType: requestSubmittedEvent, EventVersion: ProtocolVersion, Payload: payload,
		})
		if err != nil {
			t.Fatalf("submit %s: %v", requestID, err)
		}
		completedAt := request.StartedAt.Add(time.Second)
		request.Phase, request.CompletedAt = PhaseCompleted, &completedAt
		request.Output = &contract.ArtifactRef{Store: "test", Key: "answers/" + requestID + ".txt"}
		payload, _ = json.Marshal(request)
		state, err = target.Apply(state, contract.StoredEvent{
			EventType: requestCompletedEvent, EventVersion: ProtocolVersion, Payload: payload,
		})
		if err != nil {
			t.Fatalf("complete %s: %v", requestID, err)
		}
	}

	restored, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Requests) != RetainedTerminalRequests || len(restored.TerminalOrderIDs) != RetainedTerminalRequests {
		t.Fatalf("requests=%d order=%#v", len(restored.Requests), restored.TerminalOrderIDs)
	}
	if _, found := restored.Requests["request-0"]; found {
		t.Fatal("oldest terminal request was retained")
	}
	if _, found := restored.Requests["request-1"]; found {
		t.Fatal("second-oldest terminal request was retained")
	}
	if got := restored.TerminalOrderIDs[0]; got != "request-2" {
		t.Fatalf("oldest retained request=%q", got)
	}

	active := RequestState{
		RequestID: "request-active", RunID: "request-active", IdentityFingerprint: "fingerprint-active",
		Phase: PhaseRunning, StartedAt: started.Add(time.Hour),
	}
	payload, _ := json.Marshal(active)
	state, err = target.Apply(state, contract.StoredEvent{
		EventType: requestSubmittedEvent, EventVersion: ProtocolVersion, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored, err = decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Requests) != RetainedTerminalRequests+1 || restored.Requests["request-active"].Phase != PhaseRunning {
		t.Fatalf("active request was not retained: %#v", restored.Requests)
	}
}
