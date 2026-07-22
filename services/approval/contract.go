package approval

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "approval.main"

	RequestMessageType     contract.MessageType = "approval.request"
	ResolveMessageType     contract.MessageType = "approval.resolve"
	CancelMessageType      contract.MessageType = "approval.cancel"
	ExpireMessageType      contract.MessageType = "approval.expire"
	GetMessageType         contract.MessageType = "approval.get"
	ListPendingMessageType contract.MessageType = "approval.list_pending"
	ResponseMessageType    contract.MessageType = "approval.response"

	RequestedEventType contract.MessageType = "approval.requested"
	ResolvedEventType  contract.MessageType = "approval.resolved"
	CancelledEventType contract.MessageType = "approval.cancelled"
	ExpiredEventType   contract.MessageType = "approval.expired"

	ProtocolVersion = 1

	InteractionDependency = "interaction"
	SchedulerDependency   = "scheduler"
)

var Component = contract.ComponentRef{Type: "approval.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "approval.service.state", Version: 1}

const (
	approvalRequestedStateEvent contract.EventType = "approval.requested"
	approvalResolvedStateEvent  contract.EventType = "approval.resolved"
	approvalCancelledStateEvent contract.EventType = "approval.cancelled"
	approvalExpiredStateEvent   contract.EventType = "approval.expired"
)
