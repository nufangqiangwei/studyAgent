package connection

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/request"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	ManagerAddress contract.ServiceAddress = "$runtime.connections"

	OpenMessageType  contract.MessageType = "runtime.connection.open"
	SendMessageType  contract.MessageType = "runtime.connection.send"
	CloseMessageType contract.MessageType = "runtime.connection.close"
	GetMessageType   contract.MessageType = "runtime.connection.get"
	ListMessageType  contract.MessageType = "runtime.connection.list"
	ReplyMessageType contract.MessageType = "runtime.connection.reply"

	DataEventType   contract.MessageType = "runtime.connection.data"
	ClosedEventType contract.MessageType = "runtime.connection.closed"
	ErrorEventType  contract.MessageType = "runtime.connection.error"
)

var ManagerComponent = contract.ComponentRef{Type: "$runtime.connection-manager", Version: "v1"}

type EventKind string

const (
	EventData   EventKind = "data"
	EventClosed EventKind = "closed"
	EventError  EventKind = "error"
)

type Frame struct {
	// ID is stable across a retried service message. Drivers that need
	// exactly-once delivery can use it as their protocol idempotency key.
	ID       string            `json:"id,omitempty"`
	Data     []byte            `json:"data,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Event struct {
	Kind     EventKind         `json:"kind"`
	Data     []byte            `json:"data,omitempty"`
	Error    string            `json:"error,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type EmitFunc func(ctx context.Context, event Event) error

type DriverOpenRequest struct {
	ConnectionID    string
	RuntimeID       contract.RuntimeID
	PlanRevision    contract.PlanRevision
	OwnerInstanceID contract.ServiceInstanceID
	OwnerAddress    contract.ServiceAddress
	Config          json.RawMessage
	Metadata        map[string]string
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
	// Key is stable and unique within the owning service instance. It is used
	// to derive the durable connection ID and to make a retried open idempotent.
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

type Info struct {
	ConnectionID string                       `json:"connection_id"`
	Key          string                       `json:"key"`
	Driver       string                       `json:"driver"`
	Status       persistence.ConnectionStatus `json:"status"`
	DesiredOpen  bool                         `json:"desired_open"`
	LastError    string                       `json:"last_error,omitempty"`
	Metadata     map[string]string            `json:"metadata,omitempty"`
	CreatedAt    time.Time                    `json:"created_at"`
	UpdatedAt    time.Time                    `json:"updated_at"`
	OpenedAt     *time.Time                   `json:"opened_at,omitempty"`
	ClosedAt     *time.Time                   `json:"closed_at,omitempty"`
}

type ListResponse struct {
	Connections []Info `json:"connections"`
}

type InboundEvent struct {
	ConnectionID string            `json:"connection_id"`
	Kind         EventKind         `json:"kind"`
	Data         []byte            `json:"data,omitempty"`
	Error        string            `json:"error,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func Open(ctx context.Context, input OpenRequest) (Info, error) {
	var output Info
	err := request.Command(ctx, ManagerAddress, OpenMessageType, input, &output)
	return output, err
}

func OpenKey(ctx context.Context, callKey string, input OpenRequest) (Info, error) {
	var output Info
	err := request.CommandKey(ctx, callKey, ManagerAddress, OpenMessageType, input, &output)
	return output, err
}

func Send(ctx context.Context, input SendRequest) error {
	return request.Command(ctx, ManagerAddress, SendMessageType, input, nil)
}

func SendKey(ctx context.Context, callKey string, input SendRequest) error {
	return request.CommandKey(ctx, callKey, ManagerAddress, SendMessageType, input, nil)
}

func Close(ctx context.Context, input CloseRequest) error {
	return request.Command(ctx, ManagerAddress, CloseMessageType, input, nil)
}

func CloseKey(ctx context.Context, callKey string, input CloseRequest) error {
	return request.CommandKey(ctx, callKey, ManagerAddress, CloseMessageType, input, nil)
}

func Get(ctx context.Context, input GetRequest) (Info, error) {
	var output Info
	err := request.Query(ctx, ManagerAddress, GetMessageType, input, &output)
	return output, err
}

func List(ctx context.Context) ([]Info, error) {
	var output ListResponse
	err := request.Query(ctx, ManagerAddress, ListMessageType, ListRequest{}, &output)
	return output.Connections, err
}
