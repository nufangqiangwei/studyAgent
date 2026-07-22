package agent

import "agent/serviceruntime/contract"

const (
	DefaultAddress contract.ServiceAddress = "agent.main"

	ExecuteMessageType   contract.MessageType = "agent.execute"
	CancelMessageType    contract.MessageType = "agent.cancel"
	GetMessageType       contract.MessageType = "agent.get"
	CompletedMessageType contract.MessageType = "agent.completed"
	StatusMessageType    contract.MessageType = "agent.status"

	ArtifactPreparedMessageType contract.MessageType = "agent.artifact.prepared"
	ArtifactFailedMessageType   contract.MessageType = "agent.artifact.failed"

	PrepareArtifactEffectType  contract.EffectType = "agent.artifact.prepare"
	PrepareArtifactExecutorRef                     = "agent.artifact.prepare@v1"

	ProtocolVersion = 1

	ModelDependency      = "model"
	CapabilityDependency = "capability"
)

var Component = contract.ComponentRef{Type: "agent.service", Version: "v1"}

var StateSchema = contract.SchemaRef{Name: "agent.service.state", Version: 1}

const (
	runStartedEvent               contract.EventType = "agent.run.started"
	capabilitiesResolvedEvent     contract.EventType = "agent.run.capabilities_resolved"
	promptRequestedEvent          contract.EventType = "agent.run.prompt_requested"
	promptPreparedEvent           contract.EventType = "agent.run.prompt_prepared"
	modelRequestedEvent           contract.EventType = "agent.run.model_requested"
	modelRejectedEvent            contract.EventType = "agent.run.model_response_rejected"
	capabilityRequestedEvent      contract.EventType = "agent.run.capability_requested"
	capabilityResultObservedEvent contract.EventType = "agent.run.capability_result_observed"
	outputRequestedEvent          contract.EventType = "agent.run.output_requested"
	runCompletedEvent             contract.EventType = "agent.run.completed"
	runFailedEvent                contract.EventType = "agent.run.failed"
	runCancelledEvent             contract.EventType = "agent.run.cancelled"
)

type artifactOperation string

const (
	preparePrompt artifactOperation = "prompt"
	prepareOutput artifactOperation = "output"
)
