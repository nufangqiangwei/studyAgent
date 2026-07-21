package connection

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var (
	ErrAccessDenied = errors.New("connection belongs to another service")
	ErrNotFound     = errors.New("connection not found")
)

type aggregateState struct {
	Connections map[string]Record `json:"connections"`
}

type Manager struct {
	request    service.CreateRequest
	ids        contract.IDGenerator
	clock      contract.Clock
	supervisor *Supervisor
}

func newManager(request service.CreateRequest, ids contract.IDGenerator, clock contract.Clock, supervisor *Supervisor) *Manager {
	return &Manager{request: request, ids: ids, clock: clock, supervisor: supervisor}
}

func (m *Manager) Descriptor() service.Descriptor {
	return service.Descriptor{Component: ManagerComponent}
}

func (m *Manager) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(aggregateState{Connections: make(map[string]Record)})
}

func (m *Manager) Handle(_ context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	switch message.Type {
	case OpenMessageType:
		decision, output, handleErr := m.handleOpen(state, message)
		return operationResult(message, decision, output, handleErr)
	case SendMessageType:
		decision, output, handleErr := m.handleSend(state, message)
		return operationResult(message, decision, output, handleErr)
	case CloseMessageType:
		decision, output, handleErr := m.handleClose(state, message)
		return operationResult(message, decision, output, handleErr)
	case GetMessageType:
		output, handleErr := m.handleGet(state, message)
		return operationResult(message, service.Decision{}, output, handleErr)
	case ListMessageType:
		output, handleErr := m.handleList(state, message)
		return operationResult(message, service.Decision{}, output, handleErr)
	case DriverOpenedEventType, DriverFrameEventType, DriverClosedEventType, DriverErrorEventType:
		return m.handleDriverEvent(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported connection message %q", message.Type)
	}
}

func (m *Manager) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	switch event.EventType {
	case openRequestedStateEventType, closeRequestedStateEventType, openedStateEventType, closedStateEventType, failedStateEventType:
		var record Record
		if err := json.Unmarshal(event.Payload, &record); err != nil {
			return service.State{}, fmt.Errorf("decode connection state event %q: %w", event.EventType, err)
		}
		if strings.TrimSpace(record.ConnectionID) == "" {
			return service.State{}, fmt.Errorf("connection state event %q has no connection id", event.EventType)
		}
		state.Connections[record.ConnectionID] = record.Clone()
	default:
		return service.State{}, fmt.Errorf("unsupported connection state event %q", event.EventType)
	}
	return encodeState(state)
}

func (m *Manager) RestoreResources(_ context.Context, raw service.State) error {
	state, err := decodeState(raw)
	if err != nil {
		return err
	}
	records := make([]Record, 0, len(state.Connections))
	for _, record := range state.Connections {
		records = append(records, record.Clone())
	}
	return m.supervisor.Restore(records)
}

func (m *Manager) ReleaseResources(ctx context.Context) error {
	return m.supervisor.Release(ctx)
}

func (m *Manager) handleOpen(state aggregateState, message contract.Message) (service.Decision, Info, error) {
	if message.From == "" || message.From == m.request.Address {
		return service.Decision{}, Info{}, ErrAccessDenied
	}
	var input OpenRequest
	if err := decode(message.Payload, &input); err != nil {
		return service.Decision{}, Info{}, err
	}
	input.Key = strings.TrimSpace(input.Key)
	input.Driver = strings.TrimSpace(input.Driver)
	if input.Key == "" || input.Driver == "" {
		return service.Decision{}, Info{}, fmt.Errorf("connection key and driver are required")
	}
	connectionID := m.ids.Derive("connection", string(message.RuntimeID), string(message.PlanRevision), string(message.From), input.Key)
	record, found := state.Connections[connectionID]
	if found {
		if err := owns(record, message.From); err != nil {
			return service.Decision{}, Info{}, err
		}
		if record.Key != input.Key || record.Driver != input.Driver {
			return service.Decision{}, Info{}, fmt.Errorf("connection key %q is already bound to driver %q", input.Key, record.Driver)
		}
		if record.DesiredOpen && (record.Status == StatusOpening || record.Status == StatusOpen) {
			return service.Decision{}, infoFromRecord(record), nil
		}
	} else {
		record = Record{
			ConnectionID: connectionID, RuntimeID: message.RuntimeID, PlanRevision: message.PlanRevision,
			Manager: m.request.Address, OwnerAddress: message.From, Key: input.Key, Driver: input.Driver,
			CreatedAt: m.now(),
		}
	}
	record.Config = contract.CloneRaw(input.Config)
	record.Metadata = contract.CloneStrings(input.Metadata)
	record.DesiredOpen = true
	record.Status = StatusOpening
	record.Generation++
	record.LastError = ""
	record.UpdatedAt = m.now()
	record.ClosedAt = nil
	payload, err := json.Marshal(record)
	if err != nil {
		return service.Decision{}, Info{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{Key: "state:open:" + connectionID, Type: openRequestedStateEventType, Version: 1, Payload: payload}},
		Effects: []service.PlannedEffect{{
			Key: "effect:open:" + connectionID, Type: OpenEffectType, Version: 1, ExecutorRef: OpenExecutorRef,
			IdempotencyKey: fmt.Sprintf("%s/open/%d", connectionID, record.Generation), Payload: payload,
		}},
	}, infoFromRecord(record), nil
}

func (m *Manager) handleSend(state aggregateState, message contract.Message) (service.Decision, Info, error) {
	var input SendRequest
	if err := decode(message.Payload, &input); err != nil {
		return service.Decision{}, Info{}, err
	}
	record, err := ownedRecord(state, message.From, input.ConnectionID)
	if err != nil {
		return service.Decision{}, Info{}, err
	}
	if !record.DesiredOpen || record.Status != StatusOpen {
		return service.Decision{}, Info{}, fmt.Errorf("connection %q is not open", record.ConnectionID)
	}
	payload, err := json.Marshal(sendEffectPayload{
		ConnectionID: record.ConnectionID, Generation: record.Generation,
		Data: append([]byte(nil), input.Data...), Metadata: contract.CloneStrings(input.Metadata),
	})
	if err != nil {
		return service.Decision{}, Info{}, err
	}
	return service.Decision{Effects: []service.PlannedEffect{{
		Key: "effect:send:" + record.ConnectionID, Type: SendEffectType, Version: 1, ExecutorRef: SendExecutorRef,
		IdempotencyKey: message.ID, Payload: payload,
	}}}, infoFromRecord(record), nil
}

func (m *Manager) handleClose(state aggregateState, message contract.Message) (service.Decision, Info, error) {
	var input CloseRequest
	if err := decode(message.Payload, &input); err != nil {
		return service.Decision{}, Info{}, err
	}
	record, err := ownedRecord(state, message.From, input.ConnectionID)
	if err != nil {
		return service.Decision{}, Info{}, err
	}
	if !record.DesiredOpen && (record.Status == StatusClosing || record.Status == StatusClosed) {
		return service.Decision{}, infoFromRecord(record), nil
	}
	record.DesiredOpen = false
	record.Status = StatusClosing
	record.UpdatedAt = m.now()
	payload, err := json.Marshal(record)
	if err != nil {
		return service.Decision{}, Info{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{Key: "state:close:" + record.ConnectionID, Type: closeRequestedStateEventType, Version: 1, Payload: payload}},
		Effects: []service.PlannedEffect{{
			Key: "effect:close:" + record.ConnectionID, Type: CloseEffectType, Version: 1, ExecutorRef: CloseExecutorRef,
			IdempotencyKey: fmt.Sprintf("%s/close/%d", record.ConnectionID, record.Generation), Payload: payload,
		}},
	}, infoFromRecord(record), nil
}

func (m *Manager) handleGet(state aggregateState, message contract.Message) (Info, error) {
	var input GetRequest
	if err := decode(message.Payload, &input); err != nil {
		return Info{}, err
	}
	record, err := ownedRecord(state, message.From, input.ConnectionID)
	if err != nil {
		return Info{}, err
	}
	return infoFromRecord(record), nil
}

func (m *Manager) handleList(state aggregateState, message contract.Message) (ListResponse, error) {
	if message.From == "" || message.From == m.request.Address {
		return ListResponse{}, ErrAccessDenied
	}
	values := make([]Info, 0)
	for _, record := range state.Connections {
		if record.OwnerAddress == message.From {
			values = append(values, infoFromRecord(record))
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ConnectionID < values[j].ConnectionID })
	return ListResponse{Connections: values}, nil
}

func (m *Manager) handleDriverEvent(state aggregateState, message contract.Message) (service.Decision, error) {
	var input DriverEvent
	if err := decode(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	record, found := state.Connections[input.ConnectionID]
	if !found || input.Generation < record.Generation {
		return service.Decision{}, nil
	}
	if input.OccurredAt.IsZero() {
		input.OccurredAt = m.now()
	}
	if !driverEventMatchesMessage(input.Kind, message.Type) {
		return service.Decision{}, fmt.Errorf("driver event kind %q does not match message type %q", input.Kind, message.Type)
	}
	record.UpdatedAt = input.OccurredAt
	public := InboundEvent{
		ConnectionID: record.ConnectionID, ConnectionKey: record.Key, Generation: input.Generation,
		FrameID: input.FrameID, Kind: input.Kind, Data: append([]byte(nil), input.Data...),
		Error: input.Error, Metadata: contract.CloneStrings(input.Metadata), OccurredAt: input.OccurredAt,
	}
	publicPayload, err := json.Marshal(public)
	if err != nil {
		return service.Decision{}, err
	}
	outgoingType := MessageReceivedEventType
	decision := service.Decision{}
	switch message.Type {
	case DriverOpenedEventType:
		record.Status = StatusOpen
		record.Generation = input.Generation
		record.LastError = ""
		record.OpenedAt = cloneTime(&input.OccurredAt)
		record.ClosedAt = nil
		outgoingType = OpenedEventType
		decision.Events, err = stateEvent("state:opened:"+record.ConnectionID, openedStateEventType, record)
	case DriverFrameEventType:
		outgoingType = MessageReceivedEventType
	case DriverClosedEventType:
		record.Status = StatusClosed
		record.Generation = input.Generation
		record.ClosedAt = cloneTime(&input.OccurredAt)
		outgoingType = ClosedEventType
		decision.Events, err = stateEvent("state:closed:"+record.ConnectionID, closedStateEventType, record)
	case DriverErrorEventType:
		record.Status = StatusFailed
		record.Generation = input.Generation
		record.LastError = input.Error
		outgoingType = ErrorEventType
		decision.Events, err = stateEvent("state:failed:"+record.ConnectionID, failedStateEventType, record)
	}
	if err != nil {
		return service.Decision{}, err
	}
	decision.Outgoing = []service.OutgoingMessage{{
		Key:  "owner:" + string(outgoingType) + ":" + record.ConnectionID,
		Kind: contract.MessageEvent, Type: outgoingType, Version: 1, To: record.OwnerAddress,
		CorrelationID: record.ConnectionID, StreamID: contract.StreamID("connection/" + record.ConnectionID + "/inbound"),
		Payload: publicPayload,
	}}
	return decision, nil
}

func driverEventMatchesMessage(kind EventKind, messageType contract.MessageType) bool {
	switch kind {
	case EventOpened:
		return messageType == DriverOpenedEventType
	case EventData:
		return messageType == DriverFrameEventType
	case EventClosed:
		return messageType == DriverClosedEventType
	case EventError:
		return messageType == DriverErrorEventType
	default:
		return false
	}
}
func stateEvent(key string, eventType contract.EventType, record Record) ([]service.NewEvent, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return []service.NewEvent{{Key: key, Type: eventType, Version: 1, Payload: payload}}, nil
}

func decodeState(raw service.State) (aggregateState, error) {
	state := aggregateState{Connections: make(map[string]Record)}
	if len(raw.Data) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return aggregateState{}, fmt.Errorf("decode connection service state: %w", err)
	}
	if state.Connections == nil {
		state.Connections = make(map[string]Record)
	}
	return state, nil
}

func encodeState(state aggregateState) (service.State, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return service.State{}, fmt.Errorf("encode connection service state: %w", err)
	}
	return service.State{SchemaVersion: 1, Data: payload}, nil
}

func ownedRecord(state aggregateState, owner contract.ServiceAddress, connectionID string) (Record, error) {
	if strings.TrimSpace(connectionID) == "" {
		return Record{}, fmt.Errorf("connection id is required")
	}
	record, found := state.Connections[connectionID]
	if !found {
		return Record{}, ErrNotFound
	}
	if err := owns(record, owner); err != nil {
		return Record{}, err
	}
	return record.Clone(), nil
}

func owns(record Record, owner contract.ServiceAddress) error {
	if owner == "" || record.OwnerAddress != owner {
		return ErrAccessDenied
	}
	return nil
}

func decode(payload json.RawMessage, target any) error {
	if len(payload) == 0 {
		return fmt.Errorf("connection payload is required")
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode connection payload: %w", err)
	}
	return nil
}

func operationResult(message contract.Message, decision service.Decision, output any, cause error) (service.Decision, error) {
	if cause != nil {
		if message.ReplyTo == "" {
			return service.Decision{}, cause
		}
		decision.Reply = &service.Reply{
			Key: "error", Type: ReplyMessageType, Version: 1,
			Error: &service.ReplyError{Code: errorCode(cause), Message: cause.Error()},
		}
		return decision, nil
	}
	if message.ReplyTo == "" {
		return decision, nil
	}
	payload, err := json.Marshal(output)
	if err != nil {
		return service.Decision{}, err
	}
	decision.Reply = &service.Reply{Key: "result", Type: ReplyMessageType, Version: 1, Payload: payload}
	return decision, nil
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, ErrAccessDenied):
		return "connection_access_denied"
	case errors.Is(err, ErrNotFound):
		return "connection_not_found"
	default:
		return "connection_operation_failed"
	}
}

func infoFromRecord(record Record) Info {
	return Info{
		ConnectionID: record.ConnectionID, Key: record.Key, Driver: record.Driver,
		Status: record.Status, DesiredOpen: record.DesiredOpen, Generation: record.Generation,
		LastError: record.LastError, Metadata: contract.CloneStrings(record.Metadata),
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		OpenedAt: cloneTime(record.OpenedAt), ClosedAt: cloneTime(record.ClosedAt),
	}
}

func (m *Manager) now() time.Time {
	if m.clock == nil {
		return time.Now().UTC()
	}
	return m.clock.Now().UTC()
}

var _ service.Service = (*Manager)(nil)
var _ service.ActivationResource = (*Manager)(nil)
