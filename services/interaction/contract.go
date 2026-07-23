package interaction

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "interaction.main"

	SubmitMessageType contract.MessageType = "interaction.submit"

	PresentEffectType  contract.EffectType = "interaction.presentation.deliver"
	PresentExecutorRef                     = "interaction.presentation.deliver@v1"

	ProtocolVersion = 1
)

var Component = contract.ComponentRef{Type: "interaction.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "interaction.service.state", Version: 1}

const (
	requestSubmittedEvent contract.EventType = "interaction.request.submitted"
	requestCompletedEvent contract.EventType = "interaction.request.completed"
	requestFailedEvent    contract.EventType = "interaction.request.failed"
)
