package event

const (
	EventRunStarted                   Type = "RunStarted"
	EventRunResumed                   Type = "RunResumed"
	EventRunCompleted                 Type = "RunCompleted"
	EventRunFailed                    Type = "RunFailed"
	EventRunCancelled                 Type = "RunCancelled"
	EventStateChanged                 Type = "StateChanged"
	EventWaitStarted                  Type = "WaitStarted"
	EventWaitEnded                    Type = "WaitEnded"
	EventModelRequestCreated          Type = "ModelRequestCreated"
	EventModelResponseReceived        Type = "ModelResponseReceived"
	EventModelResponseFailed          Type = "ModelResponseFailed"
	EventToolCallRequested            Type = "ToolCallRequested"
	EventToolCallDispatched           Type = "ToolCallDispatched"
	EventToolCallCompleted            Type = "ToolCallCompleted"
	EventToolCallFailed               Type = "ToolCallFailed"
	EventUserApprovalRequired         Type = "UserApprovalRequired"
	EventUserApprovalReceived         Type = "UserApprovalReceived"
	EventExternalCallbackReceived     Type = "ExternalCallbackReceived"
	EventScheduledResumeDue           Type = "ScheduledResumeDue"
	EventContextPersisted             Type = "ContextPersisted"
	EventContextCompressed            Type = "ContextCompressed"
	EventEffectStarted                Type = "EffectStarted"
	EventEffectSucceeded              Type = "EffectSucceeded"
	EventEffectFailed                 Type = "EffectFailed"
	EventPolicyDecision               Type = "PolicyDecision"
	EventPolicyRejectedAction         Type = "PolicyRejectedAction"
	EventTaskCompleted                Type = "TaskCompleted"
	EventTaskFailed                   Type = "TaskFailed"
	EventStepLimitReached             Type = "StepLimitReached"
	EventAlternativeStrategyRequested Type = "AlternativeStrategyRequested"
)

func BuiltinDefinitions() []Definition {
	definitions := []Definition{
		mustReach(EventRunStarted, "starts a resumable run"),
		mustReach(EventRunResumed, "resumes a persisted run"),
		mustReach(EventRunCompleted, "completes a run"),
		mustReach(EventRunFailed, "fails a run"),
		mustReach(EventRunCancelled, "cancels a run"),
		mustReach(EventWaitStarted, "marks a run as waiting"),
		mustReach(EventWaitEnded, "marks a waiting run as resumed"),
		mustReach(EventModelResponseReceived, "continues a run after a model response"),
		mustReach(EventModelResponseFailed, "continues a run after a model failure"),
		mustReach(EventToolCallCompleted, "continues a run after a tool result"),
		mustReach(EventToolCallFailed, "continues a run after a tool failure"),
		mustReach(EventUserApprovalReceived, "continues a run after user approval"),
		mustReach(EventExternalCallbackReceived, "continues a run after an external callback"),
		mustReach(EventScheduledResumeDue, "continues a run after a scheduled resume"),
		mustReach(EventStepLimitReached, "marks a run as failed after reaching its step limit"),

		canIntercept(EventStateChanged, "observes a llm state transition"),
		canIntercept(EventModelRequestCreated, "observes a model request before dispatch"),
		canIntercept(EventToolCallRequested, "observes a requested tool call"),
		canIntercept(EventToolCallDispatched, "observes a dispatched tool call"),
		canIntercept(EventUserApprovalRequired, "observes a required user approval"),
		canIntercept(EventContextPersisted, "observes persisted run context"),
		canIntercept(EventContextCompressed, "observes context compression"),
		canIntercept(EventEffectStarted, "observes an effect before execution"),
		canIntercept(EventEffectSucceeded, "observes a completed effect"),
		canIntercept(EventEffectFailed, "observes a failed effect"),
		canIntercept(EventPolicyDecision, "observes a policy decision"),
		canIntercept(EventPolicyRejectedAction, "observes a rejected policy action"),
		canIntercept(EventTaskCompleted, "observes a completed task"),
		canIntercept(EventTaskFailed, "observes a failed task"),
		canIntercept(EventAlternativeStrategyRequested, "observes a request for a higher-level strategy change"),
	}
	return append([]Definition(nil), definitions...)
}

func canIntercept(eventType Type, description string) Definition {
	return Definition{
		Type:        eventType,
		Description: description,
		Delivery:    DeliveryCanBeIntercepted,
	}
}

func mustReach(eventType Type, description string) Definition {
	return Definition{
		Type:        eventType,
		Description: description,
		Delivery:    DeliveryMustReachStateMachine,
	}
}
