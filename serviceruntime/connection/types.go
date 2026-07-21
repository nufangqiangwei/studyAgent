package connection

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	DefaultAddress contract.ServiceAddress = "connection.manager"
	ManagerAddress                         = DefaultAddress

	OpenMessageType  contract.MessageType = "connection.open"
	SendMessageType  contract.MessageType = "connection.send"
	CloseMessageType contract.MessageType = "connection.close"
	GetMessageType   contract.MessageType = "connection.get"
	ListMessageType  contract.MessageType = "connection.list"
	ReplyMessageType contract.MessageType = "connection.reply"

	DriverOpenedEventType contract.MessageType = "connection.driver.opened"
	DriverFrameEventType  contract.MessageType = "connection.driver.frame"
	DriverClosedEventType contract.MessageType = "connection.driver.closed"
	DriverErrorEventType  contract.MessageType = "connection.driver.error"

	OpenedEventType          contract.MessageType = "connection.opened"
	MessageReceivedEventType contract.MessageType = "connection.message_received"
	DataEventType                                 = MessageReceivedEventType
	ClosedEventType          contract.MessageType = "connection.closed"
	ErrorEventType           contract.MessageType = "connection.error"
)

const (
	openRequestedStateEventType  contract.EventType = "connection.open_requested"
	closeRequestedStateEventType contract.EventType = "connection.close_requested"
	openedStateEventType         contract.EventType = "connection.opened"
	closedStateEventType         contract.EventType = "connection.closed"
	failedStateEventType         contract.EventType = "connection.failed"
)

var ManagerComponent = contract.ComponentRef{Type: "connection.manager", Version: "v1"}

type Status string

const (
	StatusOpening Status = "opening"
	StatusOpen    Status = "open"
	StatusClosing Status = "closing"
	StatusClosed  Status = "closed"
	StatusFailed  Status = "failed"
)

func (s Status) Valid() bool {
	switch s {
	case StatusOpening, StatusOpen, StatusClosing, StatusClosed, StatusFailed:
		return true
	default:
		return false
	}
}

type EventKind string

const (
	EventOpened EventKind = "opened"
	EventData   EventKind = "data"
	EventClosed EventKind = "closed"
	EventError  EventKind = "error"
)

type Frame struct {
	// ID is stable across a retried effect. Drivers that support protocol-level
	// idempotency should propagate or deduplicate this value.
	ID       string            `json:"id,omitempty"`
	Data     []byte            `json:"data,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Event struct {
	// ID should be stable when a Driver may retry delivery of the same remote
	// frame. When omitted, the Supervisor assigns a process-local sequence.
	ID       string            `json:"id,omitempty"`
	Kind     EventKind         `json:"kind"`
	Data     []byte            `json:"data,omitempty"`
	Error    string            `json:"error,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type EmitFunc func(ctx context.Context, event Event) error

type DriverOpenRequest struct {
	ConnectionID string
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	OwnerAddress contract.ServiceAddress
	Config       json.RawMessage
	Metadata     map[string]string
}

type Session interface {
	Send(ctx context.Context, frame Frame) error
	Close(ctx context.Context) error
}

type Driver interface {
	Open(ctx context.Context, request DriverOpenRequest, emit EmitFunc) (Session, error)
}

type DriverFunc func(ctx context.Context, request DriverOpenRequest, emit EmitFunc) (Session, error)

func (f DriverFunc) Open(ctx context.Context, request DriverOpenRequest, emit EmitFunc) (Session, error) {
	return f(ctx, request, emit)
}

type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

func NewRegistry() *Registry { return &Registry{drivers: make(map[string]Driver)} }

func (r *Registry) Register(name string, driver Driver) error {
	if r == nil {
		return fmt.Errorf("connection driver registry is nil")
	}
	name = strings.TrimSpace(name)
	if name == "" || driver == nil {
		return fmt.Errorf("connection driver name and implementation are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.drivers[name]; exists {
		return fmt.Errorf("connection driver %q is already registered", name)
	}
	r.drivers[name] = driver
	return nil
}

func (r *Registry) Resolve(name string) (Driver, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	driver, found := r.drivers[strings.TrimSpace(name)]
	r.mu.RUnlock()
	return driver, found
}

type OpenRequest struct {
	Key      string            `json:"key"`
	Driver   string            `json:"driver"`
	Config   json.RawMessage   `json:"config,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type SendRequest struct {
	ConnectionID string            `json:"connection_id"`
	Data         []byte            `json:"data,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type CloseRequest struct {
	ConnectionID string `json:"connection_id"`
}
type GetRequest struct {
	ConnectionID string `json:"connection_id"`
}
type ListRequest struct{}

type Record struct {
	ConnectionID string                  `json:"connection_id"`
	RuntimeID    contract.RuntimeID      `json:"runtime_id"`
	PlanRevision contract.PlanRevision   `json:"plan_revision"`
	Manager      contract.ServiceAddress `json:"manager"`
	OwnerAddress contract.ServiceAddress `json:"owner_address"`
	Key          string                  `json:"key"`
	Driver       string                  `json:"driver"`
	Config       json.RawMessage         `json:"config,omitempty"`
	Metadata     map[string]string       `json:"metadata,omitempty"`
	DesiredOpen  bool                    `json:"desired_open"`
	Status       Status                  `json:"status"`
	Generation   uint64                  `json:"generation"`
	LastError    string                  `json:"last_error,omitempty"`
	CreatedAt    time.Time               `json:"created_at,omitempty"`
	UpdatedAt    time.Time               `json:"updated_at,omitempty"`
	OpenedAt     *time.Time              `json:"opened_at,omitempty"`
	ClosedAt     *time.Time              `json:"closed_at,omitempty"`
}

func (r Record) Clone() Record {
	r.Config = contract.CloneRaw(r.Config)
	r.Metadata = contract.CloneStrings(r.Metadata)
	r.OpenedAt = cloneTime(r.OpenedAt)
	r.ClosedAt = cloneTime(r.ClosedAt)
	return r
}

type Info struct {
	ConnectionID string            `json:"connection_id"`
	Key          string            `json:"key"`
	Driver       string            `json:"driver"`
	Status       Status            `json:"status"`
	DesiredOpen  bool              `json:"desired_open"`
	Generation   uint64            `json:"generation"`
	LastError    string            `json:"last_error,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at,omitempty"`
	UpdatedAt    time.Time         `json:"updated_at,omitempty"`
	OpenedAt     *time.Time        `json:"opened_at,omitempty"`
	ClosedAt     *time.Time        `json:"closed_at,omitempty"`
}

type ListResponse struct {
	Connections []Info `json:"connections"`
}

// DriverEvent is the private, durable ingress protocol from a Driver/Supervisor
// to the Connection Service.
type DriverEvent struct {
	ConnectionID string            `json:"connection_id"`
	Generation   uint64            `json:"generation"`
	FrameID      string            `json:"frame_id,omitempty"`
	Kind         EventKind         `json:"kind"`
	Data         []byte            `json:"data,omitempty"`
	Error        string            `json:"error,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	OccurredAt   time.Time         `json:"occurred_at"`
}

// InboundEvent is the public event delivered to the Service that owns the
// connection. It is intentionally transported as a normal Runtime Event.
type InboundEvent struct {
	ConnectionID  string            `json:"connection_id"`
	ConnectionKey string            `json:"connection_key"`
	Generation    uint64            `json:"generation"`
	FrameID       string            `json:"frame_id,omitempty"`
	Kind          EventKind         `json:"kind"`
	Data          []byte            `json:"data,omitempty"`
	Error         string            `json:"error,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
