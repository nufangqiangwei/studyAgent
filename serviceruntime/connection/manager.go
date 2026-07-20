package connection

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrAccessDenied = errors.New("connection belongs to another service")
	ErrNotFound     = errors.New("connection not found")
)

type Options struct {
	RuntimeID contract.RuntimeID
	Store     persistence.ConnectionStore
	Resolver  instance.AddressResolver
	Drivers   *Registry
	Sender    Sender
	IDs       contract.IDGenerator
	Clock     contract.Clock
	Observer  contract.RuntimeEventRecorder
}

type activeConnection struct {
	session Session
	cancel  context.CancelFunc
}

type Manager struct {
	runtimeID contract.RuntimeID
	store     persistence.ConnectionStore
	resolver  instance.AddressResolver
	drivers   *Registry
	sender    Sender
	ids       contract.IDGenerator
	clock     contract.Clock
	observer  contract.RuntimeEventRecorder

	opMu sync.Mutex
	mu   sync.RWMutex

	started bool
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
	active  map[string]activeConnection
}

type RecoveryReport struct {
	Attempted int
	Restored  int
	Failed    int
	Warnings  []string
}

func NewManager(options Options) (*Manager, error) {
	if options.RuntimeID == "" || options.Store == nil || options.Resolver == nil || options.Drivers == nil || options.Sender == nil || options.IDs == nil {
		return nil, fmt.Errorf("connection manager requires runtime, store, resolver, drivers, sender and id generator")
	}
	return &Manager{
		runtimeID: options.RuntimeID, store: options.Store, resolver: options.Resolver,
		drivers: options.Drivers, sender: options.Sender, ids: options.IDs, clock: options.Clock,
		observer: options.Observer,
		active:   make(map[string]activeConnection),
	}, nil
}

func (m *Manager) Descriptor() service.Descriptor {
	return service.Descriptor{Component: ManagerComponent}
}

func (m *Manager) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1}, nil
}

func (m *Manager) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state.Clone(), nil
}

func (m *Manager) Start(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("connection manager is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return fmt.Errorf("connection manager is closed")
	}
	if m.started {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.started = true
	return nil
}

func (m *Manager) Recover(ctx context.Context) (RecoveryReport, error) {
	if err := m.Start(ctx); err != nil {
		return RecoveryReport{}, err
	}
	records, err := m.store.List(ctx, m.runtimeID)
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{}
	for _, record := range records {
		if !record.DesiredOpen {
			continue
		}
		report.Attempted++
		if err := m.restore(ctx, record); err != nil {
			report.Failed++
			report.Warnings = append(report.Warnings, fmt.Sprintf("connection %s: %v", record.ConnectionID, err))
			continue
		}
		report.Restored++
	}
	return report, nil
}

func (m *Manager) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	owner, err := m.resolveOwner(ctx, message)
	if err != nil {
		return operationFailure(message, "connection_access_denied", err, false)
	}
	var output any
	switch message.Type {
	case OpenMessageType:
		var input OpenRequest
		if err = decode(message.Payload, &input); err == nil {
			var info Info
			info, err = m.open(ctx, owner, message, input)
			output = info
		}
	case SendMessageType:
		var input SendRequest
		if err = decode(message.Payload, &input); err == nil {
			err = m.send(ctx, owner, message, input)
		}
	case CloseMessageType:
		var input CloseRequest
		if err = decode(message.Payload, &input); err == nil {
			err = m.closeOwned(ctx, owner, input.ConnectionID)
		}
	case GetMessageType:
		var input GetRequest
		if err = decode(message.Payload, &input); err == nil {
			var info Info
			info, err = m.getOwned(ctx, owner, input.ConnectionID)
			output = info
		}
	case ListMessageType:
		var values []Info
		values, err = m.listOwned(ctx, owner)
		output = ListResponse{Connections: values}
	default:
		err = fmt.Errorf("unsupported connection manager message %q", message.Type)
	}
	if err != nil {
		return operationFailure(message, errorCode(err), err, !errors.Is(err, ErrAccessDenied) && !errors.Is(err, ErrNotFound))
	}
	return operationSuccess(message, output)
}

func (m *Manager) open(ctx context.Context, owner instance.DeliveryTarget, message contract.Message, input OpenRequest) (Info, error) {
	input.Key = strings.TrimSpace(input.Key)
	input.Driver = strings.TrimSpace(input.Driver)
	if input.Key == "" || input.Driver == "" {
		return Info{}, fmt.Errorf("connection key and driver are required")
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if err := m.available(); err != nil {
		return Info{}, err
	}
	connectionID := m.ids.Derive("connection", string(message.RuntimeID), string(message.PlanRevision), string(owner.InstanceID), input.Key)
	record, found, err := m.store.Get(ctx, message.RuntimeID, connectionID)
	if err != nil {
		return Info{}, err
	}
	now := m.now()
	if found {
		if err := owns(record, owner); err != nil {
			return Info{}, err
		}
		if record.Key != input.Key || record.Driver != input.Driver {
			return Info{}, fmt.Errorf("connection key %q is already bound to driver %q", input.Key, record.Driver)
		}
		if record.DesiredOpen && m.isActive(record.ConnectionID) {
			return infoFromRecord(record), nil
		}
		record.Config = contract.CloneRaw(input.Config)
		record.Metadata = contract.CloneStrings(input.Metadata)
		record.DesiredOpen = true
		record.Status = persistence.ConnectionOpening
		record.LastError = ""
		record.UpdatedAt = now
		record.ClosedAt = nil
		if err := m.store.Update(ctx, record); err != nil {
			return Info{}, err
		}
	} else {
		record = persistence.ConnectionRecord{
			ConnectionID: connectionID, RuntimeID: message.RuntimeID, PlanRevision: message.PlanRevision,
			OwnerInstanceID: owner.InstanceID, OwnerAddress: owner.Address,
			Key: input.Key, Driver: input.Driver, Config: contract.CloneRaw(input.Config), Metadata: contract.CloneStrings(input.Metadata),
			DesiredOpen: true, Status: persistence.ConnectionOpening, CreatedAt: now, UpdatedAt: now,
		}
		if err := m.store.Create(ctx, record); err != nil {
			return Info{}, err
		}
	}
	if err := m.openRecord(ctx, &record); err != nil {
		return Info{}, err
	}
	return infoFromRecord(record), nil
}

func (m *Manager) restore(ctx context.Context, record persistence.ConnectionRecord) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if err := m.available(); err != nil {
		return err
	}
	owner, err := m.resolver.ResolveAddress(ctx, record.RuntimeID, record.PlanRevision, record.OwnerAddress)
	if err != nil {
		return m.failRecord(ctx, &record, err)
	}
	if err := owns(record, owner); err != nil {
		return m.failRecord(ctx, &record, err)
	}
	if m.isActive(record.ConnectionID) {
		return nil
	}
	record.Status = persistence.ConnectionOpening
	record.LastError = ""
	record.UpdatedAt = m.now()
	if err := m.store.Update(ctx, record); err != nil {
		return err
	}
	return m.openRecord(ctx, &record)
}

func (m *Manager) openRecord(ctx context.Context, record *persistence.ConnectionRecord) error {
	driver, found := m.drivers.Resolve(record.Driver)
	if !found {
		return m.failRecord(ctx, record, fmt.Errorf("connection driver %q is not registered", record.Driver))
	}
	rootCtx := m.ctx
	connectionCtx, cancel := context.WithCancel(rootCtx)
	emitRecord := record.Clone()
	emit := func(_ context.Context, event Event) error {
		if err := connectionCtx.Err(); err != nil {
			return err
		}
		return m.emit(connectionCtx, emitRecord.Clone(), event)
	}
	session, err := driver.Open(connectionCtx, DriverOpenRequest{
		ConnectionID: record.ConnectionID, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		OwnerInstanceID: record.OwnerInstanceID, OwnerAddress: record.OwnerAddress,
		Config: contract.CloneRaw(record.Config), Metadata: contract.CloneStrings(record.Metadata),
	}, emit)
	if err != nil {
		cancel()
		return m.failRecord(ctx, record, err)
	}
	if session == nil {
		cancel()
		return m.failRecord(ctx, record, fmt.Errorf("connection driver %q returned a nil session", record.Driver))
	}
	m.mu.Lock()
	m.active[record.ConnectionID] = activeConnection{session: session, cancel: cancel}
	m.mu.Unlock()
	now := m.now()
	record.Status = persistence.ConnectionOpen
	record.LastError = ""
	record.UpdatedAt = now
	record.OpenedAt = &now
	record.ClosedAt = nil
	if err := m.store.Update(ctx, *record); err != nil {
		removed, found := m.removeActive(record.ConnectionID)
		if found {
			removed.cancel()
		}
		_ = session.Close(context.Background())
		return err
	}
	m.record(ctx, contract.RuntimeConnectionOpened, *record)
	return nil
}

func (m *Manager) send(ctx context.Context, owner instance.DeliveryTarget, message contract.Message, input SendRequest) error {
	record, err := m.ownedRecord(ctx, owner, message.RuntimeID, input.ConnectionID)
	if err != nil {
		return err
	}
	if !record.DesiredOpen {
		return fmt.Errorf("connection %q is closed", record.ConnectionID)
	}
	m.mu.RLock()
	active, found := m.active[record.ConnectionID]
	m.mu.RUnlock()
	if !found {
		return fmt.Errorf("connection %q is not active", record.ConnectionID)
	}
	return active.session.Send(ctx, Frame{ID: message.ID, Data: append([]byte(nil), input.Data...), Metadata: contract.CloneStrings(input.Metadata)})
}

func (m *Manager) closeOwned(ctx context.Context, owner instance.DeliveryTarget, connectionID string) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	record, err := m.ownedRecord(ctx, owner, owner.RuntimeID, connectionID)
	if err != nil {
		return err
	}
	active, found := m.removeActive(record.ConnectionID)
	var closeErr error
	if found {
		active.cancel()
		closeErr = active.session.Close(ctx)
	}
	now := m.now()
	record.DesiredOpen = false
	record.Status = persistence.ConnectionClosed
	record.UpdatedAt = now
	record.ClosedAt = &now
	if closeErr != nil {
		record.LastError = closeErr.Error()
	} else {
		record.LastError = ""
	}
	if err := m.store.Update(ctx, record); err != nil {
		return err
	}
	m.record(ctx, contract.RuntimeConnectionClosed, record)
	return closeErr
}

func (m *Manager) getOwned(ctx context.Context, owner instance.DeliveryTarget, connectionID string) (Info, error) {
	record, err := m.ownedRecord(ctx, owner, owner.RuntimeID, connectionID)
	if err != nil {
		return Info{}, err
	}
	return infoFromRecord(record), nil
}

func (m *Manager) listOwned(ctx context.Context, owner instance.DeliveryTarget) ([]Info, error) {
	records, err := m.store.List(ctx, owner.RuntimeID)
	if err != nil {
		return nil, err
	}
	result := make([]Info, 0)
	for _, record := range records {
		if record.PlanRevision == owner.PlanRevision && record.OwnerInstanceID == owner.InstanceID && record.OwnerAddress == owner.Address {
			result = append(result, infoFromRecord(record))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ConnectionID < result[j].ConnectionID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (m *Manager) ownedRecord(ctx context.Context, owner instance.DeliveryTarget, runtimeID contract.RuntimeID, connectionID string) (persistence.ConnectionRecord, error) {
	if strings.TrimSpace(connectionID) == "" {
		return persistence.ConnectionRecord{}, fmt.Errorf("connection id is required")
	}
	record, found, err := m.store.Get(ctx, runtimeID, connectionID)
	if err != nil {
		return persistence.ConnectionRecord{}, err
	}
	if !found {
		return persistence.ConnectionRecord{}, ErrNotFound
	}
	if err := owns(record, owner); err != nil {
		return persistence.ConnectionRecord{}, err
	}
	return record, nil
}

func (m *Manager) resolveOwner(ctx context.Context, message contract.Message) (instance.DeliveryTarget, error) {
	if message.RuntimeID != m.runtimeID || message.From == "" || message.From == ManagerAddress {
		return instance.DeliveryTarget{}, ErrAccessDenied
	}
	owner, err := m.resolver.ResolveAddress(ctx, message.RuntimeID, message.PlanRevision, message.From)
	if err != nil || owner.Address != message.From {
		return instance.DeliveryTarget{}, ErrAccessDenied
	}
	return owner, nil
}

func owns(record persistence.ConnectionRecord, owner instance.DeliveryTarget) error {
	if record.RuntimeID != owner.RuntimeID || record.PlanRevision != owner.PlanRevision || record.OwnerInstanceID != owner.InstanceID || record.OwnerAddress != owner.Address {
		return ErrAccessDenied
	}
	return nil
}

func (m *Manager) failRecord(ctx context.Context, record *persistence.ConnectionRecord, cause error) error {
	record.Status = persistence.ConnectionFailed
	record.LastError = cause.Error()
	record.UpdatedAt = m.now()
	if err := m.store.Update(ctx, *record); err != nil {
		return fmt.Errorf("%v; persist connection failure: %w", cause, err)
	}
	m.record(ctx, contract.RuntimeConnectionFailed, *record)
	return cause
}

func (m *Manager) emit(ctx context.Context, record persistence.ConnectionRecord, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	messageType := DataEventType
	switch event.Kind {
	case EventData:
	case EventClosed:
		messageType = ClosedEventType
	case EventError:
		messageType = ErrorEventType
	default:
		return fmt.Errorf("connection event kind %q is invalid", event.Kind)
	}
	payload, err := json.Marshal(InboundEvent{
		ConnectionID: record.ConnectionID, Kind: event.Kind, Data: append([]byte(nil), event.Data...),
		Error: event.Error, Metadata: contract.CloneStrings(event.Metadata),
	})
	if err != nil {
		return err
	}
	id, err := m.ids.New("connection-event")
	if err != nil {
		return err
	}
	return m.sender.Send(ctx, contract.Message{
		ID: id, Kind: contract.MessageEvent, Type: messageType, Version: 1,
		From: ManagerAddress, To: record.OwnerAddress,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		CorrelationID: record.ConnectionID, StreamID: contract.StreamID("connection/" + record.ConnectionID),
		Payload: payload,
	})
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.started = false
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Lock()
	active := m.active
	m.active = make(map[string]activeConnection)
	m.mu.Unlock()
	var first error
	for _, value := range active {
		value.cancel()
		if err := value.session.Close(context.Background()); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) available() error {
	if m.closed {
		return fmt.Errorf("connection manager is closed")
	}
	if !m.started {
		return fmt.Errorf("connection manager is not started")
	}
	return nil
}

func (m *Manager) isActive(connectionID string) bool {
	m.mu.RLock()
	_, found := m.active[connectionID]
	m.mu.RUnlock()
	return found
}

func (m *Manager) removeActive(connectionID string) (activeConnection, bool) {
	m.mu.Lock()
	value, found := m.active[connectionID]
	if found {
		delete(m.active, connectionID)
	}
	m.mu.Unlock()
	return value, found
}

func (m *Manager) now() time.Time {
	if m.clock == nil {
		return time.Now().UTC()
	}
	return m.clock.Now().UTC()
}

func (m *Manager) record(ctx context.Context, eventType contract.RuntimeEventType, record persistence.ConnectionRecord) {
	if m.observer == nil {
		return
	}
	id, err := m.ids.New("runtime-event")
	if err != nil {
		return
	}
	_ = m.observer.RecordRuntimeEvent(ctx, contract.RuntimeEvent{
		ID: id, Type: eventType, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.OwnerInstanceID, ServiceAddress: record.OwnerAddress,
		OccurredAt: m.now(), Attributes: map[string]string{
			"connection_id": record.ConnectionID, "driver": record.Driver, "status": string(record.Status),
		},
	})
}

func decode(payload json.RawMessage, target any) error {
	if len(payload) == 0 {
		return fmt.Errorf("connection request payload is required")
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode connection request: %w", err)
	}
	return nil
}

func operationSuccess(message contract.Message, output any) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, nil
	}
	payload, err := json.Marshal(output)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{Key: "result", Type: ReplyMessageType, Version: 1, Payload: payload}}, nil
}

func operationFailure(message contract.Message, code string, cause error, retryable bool) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, cause
	}
	return service.Decision{Reply: &service.Reply{
		Key: "error", Type: ReplyMessageType, Version: 1,
		Error: &service.ReplyError{Code: code, Message: cause.Error(), Retryable: retryable},
	}}, nil
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

func infoFromRecord(record persistence.ConnectionRecord) Info {
	return Info{
		ConnectionID: record.ConnectionID, Key: record.Key, Driver: record.Driver,
		Status: record.Status, DesiredOpen: record.DesiredOpen, LastError: record.LastError,
		Metadata: contract.CloneStrings(record.Metadata), CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		OpenedAt: cloneTime(record.OpenedAt), ClosedAt: cloneTime(record.ClosedAt),
	}
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
