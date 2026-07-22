package llmClient

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "model.default"

	CompleteMessageType  contract.MessageType = "model.complete"
	CompletedMessageType contract.MessageType = "model.completed"

	CompleteEffectType  contract.EffectType = "model.complete"
	CompleteExecutorRef                     = "model.complete@v1"

	ProtocolVersion = 1
)

var Component = contract.ComponentRef{Type: "model.provider", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "model.provider.state", Version: 1}
