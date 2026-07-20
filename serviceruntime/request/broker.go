package request

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

const DefaultReplyAddress contract.ServiceAddress = "$runtime.replies"

// ResponseError is returned to a synchronous caller when a service replies with
// a structured service.ReplyError.
type ResponseError struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

func (e *ResponseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type Response struct {
	RequestID string
	Message   contract.Message
	Error     *ResponseError
}

// Broker is the in-process reply endpoint used by synchronous request calls.
// It deliberately is not a Service: replies are intercepted by the transport
// and delivered directly to the waiting caller.
type Broker struct {
	address contract.ServiceAddress

	mu      sync.Mutex
	waiters map[string]chan Response
}

func NewBroker(address contract.ServiceAddress) (*Broker, error) {
	if address == "" {
		address = DefaultReplyAddress
	}
	return &Broker{address: address, waiters: make(map[string]chan Response)}, nil
}

func (b *Broker) Address() contract.ServiceAddress {
	if b == nil {
		return ""
	}
	return b.address
}

func (b *Broker) Register(requestID string) (<-chan Response, error) {
	if b == nil {
		return nil, fmt.Errorf("request broker is nil")
	}
	if requestID == "" {
		return nil, fmt.Errorf("request id is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.waiters[requestID]; exists {
		return nil, fmt.Errorf("request %q is already waiting for a reply", requestID)
	}
	waiter := make(chan Response, 1)
	b.waiters[requestID] = waiter
	return waiter, nil
}

func (b *Broker) Cancel(requestID string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.waiters, requestID)
	b.mu.Unlock()
}

// DeliverReply implements the transport reply-sink contract. Unknown and late
// replies are accepted and discarded so an outbox does not retry forever after
// its caller has timed out.
func (b *Broker) DeliverReply(ctx context.Context, message contract.Message) error {
	if b == nil {
		return fmt.Errorf("request broker is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if message.Kind != contract.MessageReply {
		return fmt.Errorf("request broker only accepts reply messages")
	}
	if message.To != b.address {
		return fmt.Errorf("reply target %q does not match broker %q", message.To, b.address)
	}
	response, err := ResponseFromReply(message)
	if err != nil {
		return err
	}
	requestID := response.RequestID

	b.mu.Lock()
	waiter, exists := b.waiters[requestID]
	if exists {
		delete(b.waiters, requestID)
	}
	b.mu.Unlock()
	if exists {
		waiter <- response
	}
	return nil
}

// ResponseFromReply normalizes a durable reply for both the in-process broker
// and workflow replay adapters.
func ResponseFromReply(message contract.Message) (Response, error) {
	if message.Kind != contract.MessageReply {
		return Response{}, fmt.Errorf("message %q is not a reply", message.ID)
	}
	requestID := message.CausationID
	if requestID == "" {
		return Response{}, fmt.Errorf("reply %q has no causation id", message.ID)
	}
	response := Response{RequestID: requestID, Message: message.Clone()}
	if message.Metadata[contract.MetadataReplyError] == "true" {
		var responseErr ResponseError
		if err := json.Unmarshal(message.Payload, &responseErr); err != nil {
			responseErr = ResponseError{Code: "invalid_reply_error", Message: err.Error()}
		}
		response.Error = &responseErr
	}
	return response, nil
}
