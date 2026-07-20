// Package workflow provides a durable, replay-based adapter for services that
// want to write synchronous-looking request code without publishing messages
// directly from Service.Handle.
package workflow

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	transitionEventType    contract.EventType = "$runtime.workflow.transition"
	transitionEventVersion                    = 1
	stateFormat                               = "serviceruntime.workflow/v1"
)

var ErrInvocationWaiting = errors.New("workflow invocation is waiting for a reply")

// WrapFactory decorates services created by delegate with durable workflow
// semantics. The wrapped service keeps its original component descriptor and
// business Apply implementation; Adapter owns only the workflow envelope.
func WrapFactory(delegate service.Factory) service.Factory {
	return service.FactoryFunc(func(ctx context.Context, create service.CreateRequest) (service.Service, error) {
		if delegate == nil {
			return nil, fmt.Errorf("workflow delegate factory is required")
		}
		delegateCreate := create
		delegateCreate.Requests = request.NewDriverClient()
		target, err := delegate.Create(ctx, delegateCreate)
		if err != nil {
			return nil, err
		}
		return NewAdapter(target, create.Address)
	})
}

// Adapter turns an unresolved request.Command/Query call into a Decision with
// an internal workflow event and an Outgoing message. When the durable Reply
// arrives, the original handler is replayed and the recorded result is returned
// at the same call site.
type Adapter struct {
	delegate service.Service
	address  contract.ServiceAddress
}

func NewAdapter(delegate service.Service, address contract.ServiceAddress) (*Adapter, error) {
	if delegate == nil {
		return nil, fmt.Errorf("workflow delegate service is required")
	}
	if address == "" {
		return nil, fmt.Errorf("workflow service address is required")
	}
	return &Adapter{delegate: delegate, address: address}, nil
}

func (a *Adapter) Descriptor() service.Descriptor {
	return a.delegate.Descriptor()
}

func (a *Adapter) InitialState(ctx context.Context, input service.Init) (service.State, error) {
	business, err := a.delegate.InitialState(ctx, input)
	if err != nil {
		return service.State{}, err
	}
	return encodeEnvelope(stateEnvelope{Business: business.Clone()})
}

func (a *Adapter) Handle(ctx context.Context, state service.State, message contract.Message) (service.Decision, error) {
	envelope, err := decodeEnvelope(state)
	if err != nil {
		return service.Decision{}, err
	}
	if envelope.Invocation == nil {
		invocation := &invocationState{ID: message.ID, Input: message.Clone()}
		return a.advance(ctx, envelope.Business, invocation, false)
	}

	invocation := envelope.Invocation.clone()
	if message.Kind != contract.MessageReply {
		return service.Decision{}, fault.Wrap(fault.Deferred, "workflow_waiting", fmt.Errorf("%w: invocation %q", ErrInvocationWaiting, invocation.ID))
	}
	pending := invocation.pendingCall()
	if pending == nil {
		return service.Decision{}, fmt.Errorf("workflow invocation %q has no pending call", invocation.ID)
	}
	if pending.Spec.To != "" && message.From != "" && pending.Spec.To != message.From {
		return service.Decision{}, fmt.Errorf("workflow call %q expected reply from %q, got %q", pending.Key, pending.Spec.To, message.From)
	}
	response, err := request.ResponseFromReply(message)
	if err != nil {
		return service.Decision{}, err
	}
	pending.Status = callCompleted
	pending.Response = &response
	return a.advance(ctx, envelope.Business, invocation, true)
}

func (a *Adapter) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	envelope, err := decodeEnvelope(state)
	if err != nil {
		return service.State{}, err
	}
	if event.EventType == transitionEventType {
		if event.EventVersion != transitionEventVersion {
			return service.State{}, fmt.Errorf("workflow transition version %d is not supported", event.EventVersion)
		}
		var transition transitionPayload
		if err := json.Unmarshal(event.Payload, &transition); err != nil {
			return service.State{}, fmt.Errorf("decode workflow transition: %w", err)
		}
		envelope.Invocation = transition.Invocation.clone()
		return encodeEnvelope(envelope)
	}
	business, err := a.delegate.Apply(envelope.Business.Clone(), event)
	if err != nil {
		return service.State{}, err
	}
	envelope.Business = business.Clone()
	return encodeEnvelope(envelope)
}

func (a *Adapter) advance(ctx context.Context, business service.State, invocation *invocationState, resuming bool) (service.Decision, error) {
	driver := &replayDriver{invocation: invocation.clone()}
	decision, suspended, err := execute(ctx, a.delegate, business, invocation.Input, driver)
	if err != nil {
		return service.Decision{}, err
	}
	if suspended {
		if invocation.Input.Kind == contract.MessageQuery {
			return service.Decision{}, fmt.Errorf("durable workflow suspension is not supported while handling a query")
		}
		pending := driver.invocation.pendingCall()
		if pending == nil {
			return service.Decision{}, fmt.Errorf("workflow suspended without a pending call")
		}
		return service.Decision{
			Events:   []service.NewEvent{transitionEvent(driver.invocation, "suspend")},
			Outgoing: []service.OutgoingMessage{outgoingCall(a.address, driver.invocation, pending)},
		}, nil
	}
	if driver.cursor != len(driver.invocation.Calls) {
		return service.Decision{}, fmt.Errorf("workflow replay consumed %d of %d recorded calls", driver.cursor, len(driver.invocation.Calls))
	}
	if !resuming && len(driver.invocation.Calls) == 0 {
		return decision, nil
	}
	if resuming && decision.Reply != nil {
		if invocation.Input.ReplyTo == "" {
			return service.Decision{}, fmt.Errorf("workflow handler produced a reply but original message has no reply_to")
		}
		outgoing, err := replyAsOutgoing(invocation.Input, decision.Reply)
		if err != nil {
			return service.Decision{}, err
		}
		decision.Outgoing = append(decision.Outgoing, outgoing)
		decision.Reply = nil
	}
	decision.Events = append([]service.NewEvent{transitionEvent(nil, "complete")}, decision.Events...)
	return decision, nil
}

func execute(ctx context.Context, delegate service.Service, business service.State, input contract.Message, driver *replayDriver) (decision service.Decision, suspended bool, err error) {
	if input.Deadline != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, *input.Deadline)
		defer cancel()
	}
	defer func() {
		value := recover()
		if value == nil {
			return
		}
		switch signal := value.(type) {
		case suspendSignal:
			suspended = true
			decision = service.Decision{}
			err = nil
		case nondeterminismSignal:
			decision = service.Decision{}
			err = signal.err
		default:
			panic(value)
		}
	}()
	// ServiceHost installs the currently claimed message in the context. On a
	// resume that message is the Reply, while business code is logically still
	// handling the original input. Restore the original message view as well as
	// its deadline before replaying the delegate.
	ctx = request.WithMessageContext(ctx, input)
	ctx = request.WithClient(ctx, request.NewDriverClient())
	ctx = request.WithAwaitDriver(ctx, driver)
	decision, err = delegate.Handle(ctx, business.Clone(), input.Clone())
	return decision, false, err
}

type stateEnvelope struct {
	Format     string           `json:"format"`
	Business   service.State    `json:"business"`
	Invocation *invocationState `json:"invocation,omitempty"`
}

type invocationState struct {
	ID    string           `json:"id"`
	Input contract.Message `json:"input"`
	Calls []callState      `json:"calls,omitempty"`
}

type callStatus string

const (
	callPending   callStatus = "pending"
	callCompleted callStatus = "completed"
)

type callState struct {
	Key         string            `json:"key"`
	Fingerprint string            `json:"fingerprint"`
	Spec        request.CallSpec  `json:"spec"`
	Status      callStatus        `json:"status"`
	Response    *request.Response `json:"response,omitempty"`
	Deadline    *time.Time        `json:"deadline,omitempty"`
}

type transitionPayload struct {
	Invocation *invocationState `json:"invocation,omitempty"`
}

func encodeEnvelope(envelope stateEnvelope) (service.State, error) {
	envelope.Format = stateFormat
	payload, err := json.Marshal(envelope)
	if err != nil {
		return service.State{}, fmt.Errorf("encode workflow state: %w", err)
	}
	return service.State{SchemaVersion: envelope.Business.SchemaVersion, Data: payload}, nil
}

func decodeEnvelope(state service.State) (stateEnvelope, error) {
	var envelope stateEnvelope
	if len(state.Data) == 0 {
		return envelope, fmt.Errorf("workflow state envelope is empty")
	}
	if err := json.Unmarshal(state.Data, &envelope); err != nil {
		// A service may be wrapped after it already has snapshots. Treat an
		// unmarked state as the delegate's legacy business state; the first
		// workflow transition upgrades it into an envelope.
		return stateEnvelope{Format: stateFormat, Business: state.Clone()}, nil
	}
	if envelope.Format == "" {
		return stateEnvelope{Format: stateFormat, Business: state.Clone()}, nil
	}
	if envelope.Format != stateFormat {
		return stateEnvelope{}, fmt.Errorf("workflow state format %q is not supported", envelope.Format)
	}
	if envelope.Business.SchemaVersion != state.SchemaVersion {
		return stateEnvelope{}, fmt.Errorf("workflow business state schema %d does not match envelope schema %d", envelope.Business.SchemaVersion, state.SchemaVersion)
	}
	return envelope, nil
}

func transitionEvent(invocation *invocationState, phase string) service.NewEvent {
	payload, _ := json.Marshal(transitionPayload{Invocation: invocation.clone()})
	id := "none"
	count := 0
	if invocation != nil {
		id = invocation.ID
		count = len(invocation.Calls)
	}
	return service.NewEvent{
		Key:     fmt.Sprintf("$workflow-state:%s:%s:%d", id, phase, count),
		Type:    transitionEventType,
		Version: transitionEventVersion,
		Payload: payload,
		Metadata: map[string]string{
			"runtime.workflow": "true",
		},
	}
}

func outgoingCall(source contract.ServiceAddress, invocation *invocationState, call *callState) service.OutgoingMessage {
	metadata := contract.CloneStrings(call.Spec.Metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["runtime.workflow.invocation_id"] = invocation.ID
	metadata["runtime.workflow.call_key"] = call.Key
	return service.OutgoingMessage{
		Key:     fmt.Sprintf("$workflow-call:%s:%d:%s", invocation.ID, len(invocation.Calls), call.Key),
		Kind:    call.Spec.Kind,
		Type:    call.Spec.Type,
		Version: call.Spec.Version,
		To:      call.Spec.To,
		ReplyTo: source,
		// Keep the awaited reply on its own ordered stream. Otherwise an
		// unrelated message from the original stream could be claimed first,
		// deferred by the active invocation, and then block the reply behind it.
		StreamID: contract.StreamID(fmt.Sprintf("$workflow/%s/%s/%s", source, invocation.ID, call.Key)),
		Deadline: cloneTime(call.Deadline),
		Payload:  contract.CloneRaw(call.Spec.Payload),
		Metadata: metadata,
	}
}

func replyAsOutgoing(original contract.Message, reply *service.Reply) (service.OutgoingMessage, error) {
	payload := contract.CloneRaw(reply.Payload)
	metadata := contract.CloneStrings(reply.Metadata)
	if reply.Error != nil {
		var err error
		payload, err = json.Marshal(reply.Error)
		if err != nil {
			return service.OutgoingMessage{}, err
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[contract.MetadataReplyError] = "true"
	}
	return service.OutgoingMessage{
		Key: reply.Key, Kind: contract.MessageReply, Type: reply.Type, Version: reply.Version,
		To: original.ReplyTo, CorrelationID: original.CorrelationID, CausationID: original.ID, StreamID: original.StreamID,
		Payload: payload, Metadata: metadata,
	}, nil
}

type replayDriver struct {
	invocation *invocationState
	cursor     int
}

type suspendSignal struct{}

type nondeterminismSignal struct{ err error }

func (d *replayDriver) AwaitRequest(ctx context.Context, spec request.CallSpec) (request.Response, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return request.Response{}, err
		}
	}
	index := d.cursor
	d.cursor++
	key := strings.TrimSpace(spec.Key)
	if key == "" {
		key = fmt.Sprintf("call-%d", index+1)
	}
	fingerprint, err := callFingerprint(spec)
	if err != nil {
		panic(nondeterminismSignal{err: err})
	}
	if index < len(d.invocation.Calls) {
		call := &d.invocation.Calls[index]
		if call.Key != key || call.Fingerprint != fingerprint {
			panic(nondeterminismSignal{err: fmt.Errorf("workflow call %d changed during replay: recorded key=%q fingerprint=%q, current key=%q fingerprint=%q", index+1, call.Key, call.Fingerprint, key, fingerprint)})
		}
		if call.Status == callCompleted && call.Response != nil {
			return cloneResponse(*call.Response), nil
		}
		panic(suspendSignal{})
	}
	for _, previous := range d.invocation.Calls {
		if previous.Key == key {
			panic(nondeterminismSignal{err: fmt.Errorf("workflow call key %q is duplicated", key)})
		}
	}
	call := callState{
		Key: key, Fingerprint: fingerprint, Spec: cloneCallSpec(spec), Status: callPending,
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			deadline = deadline.UTC()
			call.Deadline = &deadline
		}
	}
	d.invocation.Calls = append(d.invocation.Calls, call)
	panic(suspendSignal{})
}

func callFingerprint(spec request.CallSpec) (string, error) {
	value := struct {
		Kind     contract.MessageKind
		Type     contract.MessageType
		Version  int
		To       contract.ServiceAddress
		Payload  json.RawMessage
		Metadata map[string]string
	}{
		Kind: spec.Kind, Type: spec.Type, Version: spec.Version, To: spec.To,
		Payload: contract.CloneRaw(spec.Payload), Metadata: contract.CloneStrings(spec.Metadata),
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode workflow call fingerprint: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (i *invocationState) pendingCall() *callState {
	if i == nil {
		return nil
	}
	for index := len(i.Calls) - 1; index >= 0; index-- {
		if i.Calls[index].Status == callPending {
			return &i.Calls[index]
		}
	}
	return nil
}

func (i *invocationState) clone() *invocationState {
	if i == nil {
		return nil
	}
	cloned := &invocationState{ID: i.ID, Input: i.Input.Clone(), Calls: make([]callState, len(i.Calls))}
	for index, call := range i.Calls {
		call.Spec = cloneCallSpec(call.Spec)
		call.Deadline = cloneTime(call.Deadline)
		if call.Response != nil {
			response := cloneResponse(*call.Response)
			call.Response = &response
		}
		cloned.Calls[index] = call
	}
	return cloned
}

func cloneCallSpec(spec request.CallSpec) request.CallSpec {
	spec.Payload = contract.CloneRaw(spec.Payload)
	spec.Metadata = contract.CloneStrings(spec.Metadata)
	return spec
}

func cloneResponse(response request.Response) request.Response {
	response.Message = response.Message.Clone()
	if response.Error != nil {
		value := *response.Error
		value.Details = contract.CloneStrings(response.Error.Details)
		response.Error = &value
	}
	return response
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

var _ service.Service = (*Adapter)(nil)
var _ request.AwaitDriver = (*replayDriver)(nil)
