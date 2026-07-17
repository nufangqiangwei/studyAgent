package request

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Sender interface {
	Send(ctx context.Context, message contract.Message) error
}

type SenderFunc func(ctx context.Context, message contract.Message) error

func (f SenderFunc) Send(ctx context.Context, message contract.Message) error {
	return f(ctx, message)
}

type ClientFactoryOptions struct {
	RuntimeID      contract.RuntimeID
	PlanRevision   contract.PlanRevision
	ReplyAddress   contract.ServiceAddress
	IDs            contract.IDGenerator
	Sender         Sender
	Broker         *Broker
	DefaultTimeout time.Duration
}

type ClientFactory struct {
	runtimeID      contract.RuntimeID
	planRevision   contract.PlanRevision
	replyAddress   contract.ServiceAddress
	ids            contract.IDGenerator
	sender         Sender
	broker         *Broker
	defaultTimeout time.Duration
}

func NewClientFactory(options ClientFactoryOptions) (*ClientFactory, error) {
	if options.RuntimeID == "" || options.PlanRevision == "" || options.IDs == nil || options.Sender == nil || options.Broker == nil {
		return nil, fmt.Errorf("request client factory requires runtime, plan revision, ids, sender and broker")
	}
	if options.ReplyAddress == "" {
		options.ReplyAddress = options.Broker.Address()
	}
	if options.ReplyAddress != options.Broker.Address() {
		return nil, fmt.Errorf("request reply address %q does not match broker %q", options.ReplyAddress, options.Broker.Address())
	}
	if options.DefaultTimeout <= 0 {
		options.DefaultTimeout = 30 * time.Second
	}
	return &ClientFactory{
		runtimeID: options.RuntimeID, planRevision: options.PlanRevision,
		replyAddress: options.ReplyAddress, ids: options.IDs,
		sender: options.Sender, broker: options.Broker,
		defaultTimeout: options.DefaultTimeout,
	}, nil
}

func (f *ClientFactory) ForSource(source contract.ServiceAddress) *Client {
	if f == nil {
		return nil
	}
	return &Client{
		runtimeID: f.runtimeID, planRevision: f.planRevision,
		source: source, replyAddress: f.replyAddress,
		ids: f.ids, sender: f.sender, broker: f.broker,
		defaultTimeout: f.defaultTimeout,
	}
}

type Client struct {
	runtimeID      contract.RuntimeID
	planRevision   contract.PlanRevision
	source         contract.ServiceAddress
	replyAddress   contract.ServiceAddress
	ids            contract.IDGenerator
	sender         Sender
	broker         *Broker
	defaultTimeout time.Duration
}

type CallSpec struct {
	Kind     contract.MessageKind
	Type     contract.MessageType
	Version  int
	To       contract.ServiceAddress
	Payload  json.RawMessage
	Metadata map[string]string
}

// Query sends a version-1 query and blocks until its correlated reply arrives.
func (c *Client) Query(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, input, output interface{}) error {
	return c.callValue(ctx, contract.MessageQuery, target, messageType, 1, input, output)
}

// Command sends a version-1 command and blocks until its correlated reply arrives.
func (c *Client) Command(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, input, output interface{}) error {
	return c.callValue(ctx, contract.MessageCommand, target, messageType, 1, input, output)
}

func (c *Client) QueryVersion(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, version int, input, output interface{}) error {
	return c.callValue(ctx, contract.MessageQuery, target, messageType, version, input, output)
}

func (c *Client) CommandVersion(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, version int, input, output interface{}) error {
	return c.callValue(ctx, contract.MessageCommand, target, messageType, version, input, output)
}

// Call sends a prepared command/query and waits for a reply.
func (c *Client) Call(ctx context.Context, spec CallSpec, output interface{}) error {
	if spec.Kind != contract.MessageCommand && spec.Kind != contract.MessageQuery {
		return fmt.Errorf("synchronous request kind must be command or query")
	}
	ctx, cancel := c.callContext(ctx)
	defer cancel()
	message, err := c.newMessage(ctx, spec)
	if err != nil {
		return err
	}
	waiter, err := c.broker.Register(message.ID)
	if err != nil {
		return err
	}
	defer c.broker.Cancel(message.ID)
	if err := c.sender.Send(ctx, message); err != nil {
		return err
	}
	select {
	case response := <-waiter:
		if response.Error != nil {
			return response.Error
		}
		return decodePayload(response.Message.Payload, output)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Event dispatches a version-1 event through the runtime's event routes.
func (c *Client) Event(ctx context.Context, messageType contract.MessageType, input interface{}) error {
	return c.dispatchValue(ctx, contract.MessageEvent, "", messageType, 1, input)
}

func (c *Client) EventVersion(ctx context.Context, messageType contract.MessageType, version int, input interface{}) error {
	return c.dispatchValue(ctx, contract.MessageEvent, "", messageType, version, input)
}

// Reply dispatches a version-1 reply correlated to the supplied request.
// Service.Handle should normally return service.Decision.Reply so the reply is
// committed atomically; this method is intended for explicit immediate replies.
func (c *Client) Reply(ctx context.Context, original contract.Message, messageType contract.MessageType, input interface{}) error {
	return c.ReplyVersion(ctx, original, messageType, 1, input)
}

func (c *Client) ReplyVersion(ctx context.Context, original contract.Message, messageType contract.MessageType, version int, input interface{}) error {
	payload, err := encodePayload(input)
	if err != nil {
		return err
	}
	return c.dispatchReply(ctx, original, messageType, version, payload)
}

// Dispatch sends a prepared command, query, or event without waiting.
func (c *Client) Dispatch(ctx context.Context, spec CallSpec) error {
	if spec.Kind == contract.MessageReply || !spec.Kind.Valid() {
		return fmt.Errorf("dispatch kind must be command, query or event")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	message, err := c.newMessage(ctx, spec)
	if err != nil {
		return err
	}
	message.ReplyTo = ""
	return c.sender.Send(ctx, message)
}

func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline || c == nil || c.defaultTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.defaultTimeout)
}

func (c *Client) callValue(ctx context.Context, kind contract.MessageKind, target contract.ServiceAddress, messageType contract.MessageType, version int, input, output interface{}) error {
	payload, err := encodePayload(input)
	if err != nil {
		return err
	}
	return c.Call(ctx, CallSpec{Kind: kind, Type: messageType, Version: version, To: target, Payload: payload}, output)
}

func (c *Client) dispatchValue(ctx context.Context, kind contract.MessageKind, target contract.ServiceAddress, messageType contract.MessageType, version int, input interface{}) error {
	payload, err := encodePayload(input)
	if err != nil {
		return err
	}
	return c.Dispatch(ctx, CallSpec{Kind: kind, Type: messageType, Version: version, To: target, Payload: payload})
}

func (c *Client) dispatchReply(ctx context.Context, original contract.Message, messageType contract.MessageType, version int, payload json.RawMessage) error {
	if c == nil || c.ids == nil || c.sender == nil {
		return fmt.Errorf("request client is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if original.ID == "" || original.ReplyTo == "" {
		return fmt.Errorf("reply requires an original message id and reply_to address")
	}
	if messageType == "" || version <= 0 {
		return fmt.Errorf("reply type and positive version are required")
	}
	id, err := c.ids.New("reply")
	if err != nil {
		return err
	}
	correlationID := original.CorrelationID
	if correlationID == "" {
		correlationID = original.ID
	}
	message := contract.Message{
		ID: id, Kind: contract.MessageReply, Type: messageType, Version: version,
		From: c.source, To: original.ReplyTo,
		RuntimeID: c.runtimeID, PlanRevision: c.planRevision,
		UserID: original.UserID, GoalID: original.GoalID, RunID: original.RunID,
		CorrelationID: correlationID, CausationID: original.ID, StreamID: original.StreamID,
		Payload: contract.CloneRaw(payload),
	}
	return c.sender.Send(ctx, message)
}

func (c *Client) newMessage(ctx context.Context, spec CallSpec) (contract.Message, error) {
	if c == nil || c.ids == nil || c.sender == nil || c.broker == nil {
		return contract.Message{}, fmt.Errorf("request client is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return contract.Message{}, err
	}
	if !spec.Kind.Valid() || spec.Kind == contract.MessageReply || spec.Type == "" || spec.Version <= 0 {
		return contract.Message{}, fmt.Errorf("request kind, type and positive version are required")
	}
	if spec.Kind != contract.MessageEvent && spec.To == "" {
		return contract.Message{}, fmt.Errorf("%s request requires a target", spec.Kind)
	}
	id, err := c.ids.New("request")
	if err != nil {
		return contract.Message{}, err
	}
	message := contract.Message{
		ID: id, Kind: spec.Kind, Type: spec.Type, Version: spec.Version,
		From: c.source, To: spec.To, ReplyTo: c.replyAddress,
		RuntimeID: c.runtimeID, PlanRevision: c.planRevision,
		CorrelationID: id, Payload: contract.CloneRaw(spec.Payload), Metadata: contract.CloneStrings(spec.Metadata),
	}
	if deadline, ok := ctx.Deadline(); ok {
		deadline = deadline.UTC()
		message.Deadline = &deadline
	}
	if parent, ok := messageFromContext(ctx); ok {
		message.UserID = parent.UserID
		message.GoalID = parent.GoalID
		message.RunID = parent.RunID
		message.StreamID = parent.StreamID
		message.CausationID = parent.ID
		if parent.CorrelationID != "" {
			message.CorrelationID = parent.CorrelationID
		}
	}
	return message, nil
}

func encodePayload(value interface{}) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	if raw, ok := value.(json.RawMessage); ok {
		return contract.CloneRaw(raw), nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode request payload: %w", err)
	}
	return payload, nil
}

func decodePayload(payload json.RawMessage, output interface{}) error {
	if output == nil {
		return nil
	}
	if raw, ok := output.(*json.RawMessage); ok {
		*raw = contract.CloneRaw(payload)
		return nil
	}
	if len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, output); err != nil {
		return fmt.Errorf("decode response payload: %w", err)
	}
	return nil
}

type messageContextKey struct{}

type clientContextKey struct{}

func WithClient(ctx context.Context, client *Client) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, clientContextKey{}, client)
}

func FromContext(ctx context.Context) (*Client, bool) {
	if ctx == nil {
		return nil, false
	}
	client, ok := ctx.Value(clientContextKey{}).(*Client)
	return client, ok && client != nil
}

func Query(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, input, output interface{}) error {
	client, ok := FromContext(ctx)
	if !ok {
		return fmt.Errorf("request client is not available in the service context")
	}
	return client.Query(ctx, target, messageType, input, output)
}

func Command(ctx context.Context, target contract.ServiceAddress, messageType contract.MessageType, input, output interface{}) error {
	client, ok := FromContext(ctx)
	if !ok {
		return fmt.Errorf("request client is not available in the service context")
	}
	return client.Command(ctx, target, messageType, input, output)
}

func Event(ctx context.Context, messageType contract.MessageType, input interface{}) error {
	client, ok := FromContext(ctx)
	if !ok {
		return fmt.Errorf("request client is not available in the service context")
	}
	return client.Event(ctx, messageType, input)
}

func Reply(ctx context.Context, original contract.Message, messageType contract.MessageType, input interface{}) error {
	client, ok := FromContext(ctx)
	if !ok {
		return fmt.Errorf("request client is not available in the service context")
	}
	return client.Reply(ctx, original, messageType, input)
}

// WithMessageContext lets nested requests inherit identity and correlation from
// the message currently handled by a service.
func WithMessageContext(ctx context.Context, message contract.Message) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, messageContextKey{}, message.Clone())
}

func messageFromContext(ctx context.Context) (contract.Message, bool) {
	if ctx == nil {
		return contract.Message{}, false
	}
	message, ok := ctx.Value(messageContextKey{}).(contract.Message)
	return message, ok
}
