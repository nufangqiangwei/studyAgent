package request

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

type testIDs struct{ next int }

func (i *testIDs) New(kind string) (string, error) {
	i.next++
	return fmt.Sprintf("%s-%d", kind, i.next), nil
}

func (*testIDs) Derive(kind string, parts ...string) string { return kind }

func TestBrokerCorrelatesStructuredErrorReplyByCausationID(t *testing.T) {
	broker, err := NewBroker("")
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := broker.Register("request-1")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(ResponseError{Code: "not_found", Message: "value is missing"})
	if err := broker.DeliverReply(context.Background(), contract.Message{
		ID: "reply-1", Kind: contract.MessageReply, Type: "value.failed", Version: 1,
		To: broker.Address(), RuntimeID: "runtime", PlanRevision: "v1",
		CausationID: "request-1", Payload: payload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	}); err != nil {
		t.Fatal(err)
	}
	response := <-waiter
	var responseErr *ResponseError
	if !errors.As(response.Error, &responseErr) {
		t.Fatalf("response error = %#v, want ResponseError", response.Error)
	}
	if response.RequestID != "request-1" || responseErr.Code != "not_found" || responseErr.Message != "value is missing" {
		t.Fatalf("response = %#v", response)
	}

	// A caller may have timed out before its durable reply arrives. The broker
	// accepts that late reply so the service outbox can still be acknowledged.
	if err := broker.DeliverReply(context.Background(), contract.Message{
		ID: "reply-late", Kind: contract.MessageReply, Type: "value.result", Version: 1,
		To: broker.Address(), RuntimeID: "runtime", PlanRevision: "v1", CausationID: "request-late",
	}); err != nil {
		t.Fatalf("late reply: %v", err)
	}
}

func TestClientConvenienceMethodsMatchMessageKinds(t *testing.T) {
	broker, err := NewBroker("")
	if err != nil {
		t.Fatal(err)
	}
	var sent []contract.Message
	factory, err := NewClientFactory(ClientFactoryOptions{
		RuntimeID: "runtime", PlanRevision: "v1", IDs: &testIDs{}, Broker: broker,
		Sender: SenderFunc(func(ctx context.Context, message contract.Message) error {
			sent = append(sent, message.Clone())
			if message.Kind == contract.MessageCommand || message.Kind == contract.MessageQuery {
				return broker.DeliverReply(ctx, contract.Message{
					ID: "reply-" + message.ID, Kind: contract.MessageReply, Type: "test.result", Version: 1,
					To: broker.Address(), RuntimeID: "runtime", PlanRevision: "v1",
					CausationID: message.ID, CorrelationID: message.CorrelationID,
				})
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	client := factory.ForSource("source")
	ctx := context.Background()
	if err := client.Command(ctx, "target", "test.command", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Query(ctx, "target", "test.query", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Event(ctx, "test.event", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Reply(ctx, contract.Message{ID: "original", ReplyTo: "reply.target"}, "test.reply", nil); err != nil {
		t.Fatal(err)
	}
	want := []contract.MessageKind{
		contract.MessageCommand, contract.MessageQuery, contract.MessageEvent, contract.MessageReply,
	}
	if len(sent) != len(want) {
		t.Fatalf("sent message count = %d, want %d", len(sent), len(want))
	}
	for index, kind := range want {
		if sent[index].Kind != kind {
			t.Fatalf("sent[%d].kind = %q, want %q", index, sent[index].Kind, kind)
		}
	}
	if sent[3].CausationID != "original" || sent[3].To != "reply.target" {
		t.Fatalf("reply correlation = %#v", sent[3])
	}
}
