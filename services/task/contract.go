package task

import "agent/serviceruntime/contract"

const (
	CreateMessageType      contract.MessageType = "task.create"
	MarkReadyMessageType   contract.MessageType = "task.mark_ready"
	AssignMessageType      contract.MessageType = "task.assign"
	StartMessageType       contract.MessageType = "task.start"
	SuspendMessageType     contract.MessageType = "task.suspend"
	ResumeMessageType      contract.MessageType = "task.resume"
	RetryMessageType       contract.MessageType = "task.retry"
	CancelMessageType      contract.MessageType = "task.cancel"
	GetMessageType         contract.MessageType = "task.get"
	StatusMessageType      contract.MessageType = "task.status"
	StatusChangedEventType contract.MessageType = "task.status.changed"
	CompletedEventType     contract.MessageType = "task.completed"
	FailedEventType        contract.MessageType = "task.failed"
	CancelledEventType     contract.MessageType = "task.cancelled"

	ExecutionWaitingMessageType contract.MessageType = "task.execution.waiting"
	ExecutionResumedMessageType contract.MessageType = "task.execution.resumed"

	ProtocolVersion = 1
)

var Component = contract.ComponentRef{Type: "task.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "task.service.state", Version: 1}

const (
	taskCreatedEvent         contract.EventType = "task.state.created"
	taskReadyEvent           contract.EventType = "task.state.ready"
	taskAssignedEvent        contract.EventType = "task.state.assigned"
	taskStartedEvent         contract.EventType = "task.state.started"
	taskWaitingEvent         contract.EventType = "task.state.waiting"
	taskResumedEvent         contract.EventType = "task.state.resumed"
	taskSuspendedEvent       contract.EventType = "task.state.suspended"
	taskRetryRequestedEvent  contract.EventType = "task.state.retry_requested"
	taskCancelRequestedEvent contract.EventType = "task.state.cancel_requested"
	taskCompletedEvent       contract.EventType = "task.state.completed"
	taskFailedEvent          contract.EventType = "task.state.failed"
	taskCancelledEvent       contract.EventType = "task.state.cancelled"
)

const maxInlineTaskInputBytes = 16 << 10
