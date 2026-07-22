package capability

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "capability.main"

	InvokeMessageType contract.MessageType = "capability.invoke"
	CancelMessageType contract.MessageType = "capability.cancel"
	GetMessageType    contract.MessageType = "capability.get"
	ListMessageType   contract.MessageType = "capability.list"
	PruneMessageType  contract.MessageType = "capability.prune"
	ResultMessageType contract.MessageType = "capability.result"

	ExecutionCompletedMessageType contract.MessageType = "capability.execution.completed"
	ExecutionFailedMessageType    contract.MessageType = "capability.execution.failed"

	ProtocolVersion = 1

	ApprovalDependency  = "approval"
	SchedulerDependency = "scheduler"
)

var Component = contract.ComponentRef{Type: "capability.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "capability.service.state", Version: 1}

const (
	callReceivedStateEvent      contract.EventType = "capability.call.received"
	callAuthorizationStateEvent contract.EventType = "capability.call.authorization_decided"
	callExecutionStateEvent     contract.EventType = "capability.call.execution_started"
	callTerminalStateEvent      contract.EventType = "capability.call.terminal"
	callLateOutcomeStateEvent   contract.EventType = "capability.call.late_outcome_observed"
	callCompactedStateEvent     contract.EventType = "capability.call.compacted"
	tombstoneRemovedStateEvent  contract.EventType = "capability.call.tombstone_removed"
)
