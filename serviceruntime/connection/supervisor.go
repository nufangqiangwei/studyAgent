package connection

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type supervisorOptions struct {
	Request   service.CreateRequest
	Drivers   *Registry
	Ingress   assembly.MessageIngress
	IDs       contract.IDGenerator
	Clock     contract.Clock
	Resources *resourceDirectory
}

type activeConnection struct {
	record     Record
	generation uint64
	session    Session
	cancel     context.CancelFunc
}

type Supervisor struct {
	request   service.CreateRequest
	drivers   *Registry
	ingress   assembly.MessageIngress
	ids       contract.IDGenerator
	clock     contract.Clock
	resources *resourceDirectory

	opMu sync.Mutex
	mu   sync.RWMutex

	started    bool
	closed     bool
	rootCtx    context.Context
	rootCancel context.CancelFunc
	active     map[string]activeConnection
	receiveSeq map[string]uint64
}

func newSupervisor(options supervisorOptions) *Supervisor {
	return &Supervisor{
		request: options.Request, drivers: options.Drivers, ingress: options.Ingress,
		ids: options.IDs, clock: options.Clock, resources: options.Resources,
		active: make(map[string]activeConnection), receiveSeq: make(map[string]uint64),
	}
}

func (s *Supervisor) Restore(records []Record) error {
	if s == nil {
		return fmt.Errorf("connection supervisor is nil")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed {
		return fmt.Errorf("connection supervisor is closed")
	}
	if !s.started {
		s.rootCtx, s.rootCancel = context.WithCancel(context.Background())
		s.started = true
		if err := s.resources.Register(s.request.InstanceID, s); err != nil {
			s.started = false
			s.rootCancel()
			return err
		}
	}
	for _, persisted := range records {
		if !persisted.DesiredOpen {
			continue
		}
		record := persisted.Clone()
		record.Generation++
		if err := s.openLocked(context.Background(), record, "restore"); err != nil {
			// Resource restoration is best effort. openLocked emits a durable
			// DriverError event so the Service state observes the failure.
			continue
		}
	}
	return nil
}

func (s *Supervisor) Open(ctx context.Context, record Record, sourceID string) error {
	if s == nil {
		return fmt.Errorf("connection supervisor is nil")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	return s.openLocked(ctx, record.Clone(), sourceID)
}

func (s *Supervisor) openLocked(ctx context.Context, record Record, sourceID string) error {
	if err := s.available(); err != nil {
		return err
	}
	if record.Generation == 0 {
		record.Generation = 1
	}
	s.mu.RLock()
	existing, found := s.active[record.ConnectionID]
	s.mu.RUnlock()
	if found && existing.generation > record.Generation {
		return nil
	}
	if found && existing.generation == record.Generation {
		return s.emit(ctx, existing.record, existing.generation, Event{ID: "opened:" + sourceID, Kind: EventOpened})
	}
	if found {
		s.removeAndClose(ctx, record.ConnectionID)
	}
	driver, found := s.drivers.Resolve(record.Driver)
	if !found {
		err := fmt.Errorf("connection driver %q is not registered", record.Driver)
		_ = s.emit(ctx, record, record.Generation, Event{ID: "open-error:" + sourceID, Kind: EventError, Error: err.Error()})
		return err
	}
	connectionCtx, cancel := context.WithCancel(s.rootCtx)
	emit := func(emitCtx context.Context, event Event) error {
		if err := connectionCtx.Err(); err != nil {
			return err
		}
		if emitCtx == nil {
			emitCtx = connectionCtx
		}
		if event.Kind == EventClosed {
			s.removeActive(record.ConnectionID, record.Generation)
		}
		return s.emit(emitCtx, record, record.Generation, event)
	}
	session, err := driver.Open(connectionCtx, DriverOpenRequest{
		ConnectionID: record.ConnectionID, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		OwnerAddress: record.OwnerAddress, Config: contract.CloneRaw(record.Config), Metadata: contract.CloneStrings(record.Metadata),
	}, emit)
	if err != nil {
		cancel()
		_ = s.emit(ctx, record, record.Generation, Event{ID: "open-error:" + sourceID, Kind: EventError, Error: err.Error()})
		return err
	}
	if session == nil {
		cancel()
		err = fmt.Errorf("connection driver %q returned a nil session", record.Driver)
		_ = s.emit(ctx, record, record.Generation, Event{ID: "open-error:" + sourceID, Kind: EventError, Error: err.Error()})
		return err
	}
	s.mu.Lock()
	s.active[record.ConnectionID] = activeConnection{record: record.Clone(), generation: record.Generation, session: session, cancel: cancel}
	s.mu.Unlock()
	return s.emit(ctx, record, record.Generation, Event{ID: "opened:" + sourceID, Kind: EventOpened})
}

func (s *Supervisor) Send(ctx context.Context, input sendEffectPayload, effectID string) error {
	s.mu.RLock()
	active, found := s.active[input.ConnectionID]
	s.mu.RUnlock()
	if !found || active.generation < input.Generation {
		return fmt.Errorf("connection %q is not active", input.ConnectionID)
	}
	return active.session.Send(ctx, Frame{ID: effectID, Data: append([]byte(nil), input.Data...), Metadata: contract.CloneStrings(input.Metadata)})
}

func (s *Supervisor) Close(ctx context.Context, record Record, sourceID string) error {
	if s == nil {
		return fmt.Errorf("connection supervisor is nil")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.available(); err != nil {
		return err
	}
	closeErr := s.removeAndClose(ctx, record.ConnectionID)
	emitErr := s.emit(ctx, record, record.Generation, Event{ID: "closed:" + sourceID, Kind: EventClosed, Error: errorText(closeErr)})
	if closeErr != nil {
		return closeErr
	}
	return emitErr
}

func (s *Supervisor) IsActive(connectionID string, generation uint64) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	active, found := s.active[connectionID]
	s.mu.RUnlock()
	return found && active.generation >= generation
}

func (s *Supervisor) Release(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.started = false
	if s.rootCancel != nil {
		s.rootCancel()
	}
	s.resources.Remove(s.request.InstanceID, s)
	s.mu.Lock()
	active := s.active
	s.active = make(map[string]activeConnection)
	s.mu.Unlock()
	var first error
	for _, connection := range active {
		connection.cancel()
		if err := connection.session.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *Supervisor) emit(ctx context.Context, record Record, generation uint64, event Event) error {
	if event.Kind != EventOpened && event.Kind != EventData && event.Kind != EventClosed && event.Kind != EventError {
		return fmt.Errorf("connection event kind %q is invalid", event.Kind)
	}
	frameID := event.ID
	if frameID == "" {
		s.mu.Lock()
		s.receiveSeq[record.ConnectionID]++
		frameID = strconv.FormatUint(s.receiveSeq[record.ConnectionID], 10)
		s.mu.Unlock()
	}
	occurredAt := s.now()
	payload, err := json.Marshal(DriverEvent{
		ConnectionID: record.ConnectionID, Generation: generation, FrameID: frameID,
		Kind: event.Kind, Data: append([]byte(nil), event.Data...), Error: event.Error,
		Metadata: contract.CloneStrings(event.Metadata), OccurredAt: occurredAt,
	})
	if err != nil {
		return err
	}
	messageType := DriverFrameEventType
	switch event.Kind {
	case EventOpened:
		messageType = DriverOpenedEventType
	case EventClosed:
		messageType = DriverClosedEventType
	case EventError:
		messageType = DriverErrorEventType
	}
	messageID := s.ids.Derive("connection-inbound", record.ConnectionID, strconv.FormatUint(generation, 10), frameID, string(messageType))
	return s.ingress.Send(ctx, contract.Message{
		ID: messageID, Kind: contract.MessageEvent, Type: messageType, Version: 1,
		From: record.Manager, To: record.Manager,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		CorrelationID: record.ConnectionID, StreamID: contract.StreamID("connection/" + record.ConnectionID + "/driver"),
		Payload: payload,
	})
}

func (s *Supervisor) removeAndClose(ctx context.Context, connectionID string) error {
	s.mu.Lock()
	active, found := s.active[connectionID]
	if found {
		delete(s.active, connectionID)
	}
	s.mu.Unlock()
	if !found {
		return nil
	}
	active.cancel()
	return active.session.Close(ctx)
}

func (s *Supervisor) removeActive(connectionID string, generation uint64) {
	s.mu.Lock()
	active, found := s.active[connectionID]
	if found && active.generation == generation {
		delete(s.active, connectionID)
		active.cancel()
	}
	s.mu.Unlock()
}

func (s *Supervisor) available() error {
	if s.closed {
		return fmt.Errorf("connection supervisor is closed")
	}
	if !s.started {
		return fmt.Errorf("connection supervisor is not activated")
	}
	return nil
}

func (s *Supervisor) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

type resourceDirectory struct {
	mu     sync.RWMutex
	values map[contract.ServiceInstanceID]*Supervisor
}

func newResourceDirectory() *resourceDirectory {
	return &resourceDirectory{values: make(map[contract.ServiceInstanceID]*Supervisor)}
}

func (d *resourceDirectory) Register(instanceID contract.ServiceInstanceID, supervisor *Supervisor) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if current, found := d.values[instanceID]; found && current != supervisor {
		return fmt.Errorf("connection resources for service instance %q are already active", instanceID)
	}
	d.values[instanceID] = supervisor
	return nil
}

func (d *resourceDirectory) Resolve(instanceID contract.ServiceInstanceID) (*Supervisor, bool) {
	d.mu.RLock()
	value, found := d.values[instanceID]
	d.mu.RUnlock()
	return value, found
}

func (d *resourceDirectory) Remove(instanceID contract.ServiceInstanceID, supervisor *Supervisor) {
	d.mu.Lock()
	if d.values[instanceID] == supervisor {
		delete(d.values, instanceID)
	}
	d.mu.Unlock()
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
