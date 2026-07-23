package webgateway

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "web.gateway"

	CreateTaskMessageType contract.MessageType = "web.task.create"
	GetTaskMessageType    contract.MessageType = "web.task.get"

	PresentationEffectType  contract.EffectType = "web.task.presentation.deliver"
	PresentationExecutorRef                     = "web.task.presentation.deliver@v1"

	ProtocolVersion = 1
)

var Component = contract.ComponentRef{Type: "webgateway.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "webgateway.service.state", Version: 1}

const (
	requestRecordedEvent          contract.EventType = "webgateway.request.recorded"
	taskDeclarationCompletedEvent contract.EventType = "webgateway.task.declaration_completed"
	requestSucceededEvent         contract.EventType = "webgateway.request.succeeded"
	requestFailedEvent            contract.EventType = "webgateway.request.failed"
	maxInlineTaskInputBytes                          = 16 << 10
	RetainedTerminalRequests                         = 128
)
