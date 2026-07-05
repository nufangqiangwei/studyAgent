package statemachine

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
)

const CodeAgentName = "code"

const (
	CodePhaseInspectRepo AgentPhase = "InspectRepo"
	CodePhaseEditCode    AgentPhase = "EditCode"
	CodePhaseRunTests    AgentPhase = "RunTests"
	CodePhaseFixErrors   AgentPhase = "FixErrors"
	CodePhaseReport      AgentPhase = "Report"
)

const (
	EventCodeInspectionCompleted eventbus.EventType = "code.inspection_completed"
	EventCodeEditCompleted       eventbus.EventType = "code.edit_completed"
	EventCodeTestsPassed         eventbus.EventType = "code.tests_passed"
	EventCodeTestsFailed         eventbus.EventType = "code.tests_failed"
	EventCodeFixCompleted        eventbus.EventType = "code.fix_completed"
)

func NewCodeAgentFlow() (*ConfiguredAgentFlow, error) {
	resume := NewEffectTemplate(reactor.EffectAgentResume, nil)
	return NewConfiguredAgentFlow(AgentFlowDefinition{
		Initial: CodePhaseInspectRepo,
		Transitions: []AgentTransition{
			{From: CodePhaseInspectRepo, EventType: EventCodeInspectionCompleted, To: CodePhaseEditCode, Effects: []EffectTemplate{resume}},
			{From: CodePhaseEditCode, EventType: EventCodeEditCompleted, To: CodePhaseRunTests, Effects: []EffectTemplate{resume}},
			{From: CodePhaseRunTests, EventType: EventCodeTestsFailed, To: CodePhaseFixErrors, Effects: []EffectTemplate{resume}},
			{From: CodePhaseFixErrors, EventType: EventCodeFixCompleted, To: CodePhaseRunTests, Effects: []EffectTemplate{resume}},
			{From: CodePhaseRunTests, EventType: EventCodeTestsPassed, To: CodePhaseReport, Effects: []EffectTemplate{resume}},
		},
	})
}
