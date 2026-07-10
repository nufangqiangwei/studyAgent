package orchestrator

import (
	"agent/internal/runtime/agents"
	"context"
	"fmt"
	"strings"
)

type Orchestrator struct {
	registry AgentRegistry
	planner  Planner
}

func New(registry AgentRegistry, planner Planner) (*Orchestrator, error) {
	if registry == nil {
		return nil, fmt.Errorf("orchestrator: agent registry is required")
	}
	if planner == nil {
		return nil, fmt.Errorf("orchestrator: planner is required")
	}
	return &Orchestrator{registry: registry, planner: planner}, nil
}

func (o *Orchestrator) Advance(ctx context.Context, request AdvanceRequest) (AdvanceResult, error) {
	if o == nil {
		return AdvanceResult{}, fmt.Errorf("orchestrator is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	decision, err := o.planner.Plan(ctx, PlanRequest{
		Goal:     request.Goal,
		TaskID:   request.TaskID,
		Agents:   o.agentDescriptors(),
		History:  cloneStepRecords(request.History),
		Metadata: cloneStringMap(request.Metadata),
	})
	if err != nil {
		return AdvanceResult{}, err
	}
	if err := decision.Validate(); err != nil {
		return AdvanceResult{}, err
	}

	switch decision.Action {
	case ActionStartAgent:
		result, err := o.startAgent(ctx, decision.Work, request)
		if err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{Status: StatusDispatched, Decision: decision, AgentResult: &result}, nil
	case ActionResumeAgent:
		result, err := o.resumeAgent(ctx, decision.Work, request)
		if err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{Status: StatusDispatched, Decision: decision, AgentResult: &result}, nil
	case ActionWait:
		return AdvanceResult{Status: StatusWaiting, Decision: decision}, nil
	case ActionComplete:
		return AdvanceResult{Status: StatusCompleted, Decision: decision}, nil
	case ActionFail:
		return AdvanceResult{Status: StatusFailed, Decision: decision}, nil
	default:
		return AdvanceResult{}, fmt.Errorf("unsupported orchestrator action %q", decision.Action)
	}
}

func (o *Orchestrator) startAgent(ctx context.Context, work *AgentWork, request AdvanceRequest) (agents.AgentResult, error) {
	agent, err := o.lookupAgent(work.Agent)
	if err != nil {
		return agents.AgentResult{}, err
	}
	taskID := strings.TrimSpace(work.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(request.TaskID)
	}
	return agent.Start(ctx, agents.AgentStartInput{
		TaskID:   taskID,
		Input:    work.Input,
		Metadata: mergeMetadata(request.Metadata, work.Metadata),
	})
}

func (o *Orchestrator) resumeAgent(ctx context.Context, work *AgentWork, request AdvanceRequest) (agents.AgentResult, error) {
	agent, err := o.lookupAgent(work.Agent)
	if err != nil {
		return agents.AgentResult{}, err
	}
	return agent.Resume(ctx, agents.AgentResumeInput{
		TaskID:   strings.TrimSpace(work.TaskID),
		Payload:  cloneRaw(work.Payload),
		Metadata: mergeMetadata(request.Metadata, work.Metadata),
	})
}

func (o *Orchestrator) lookupAgent(name string) (agents.Agent, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("orchestrator: agent name is required")
	}
	agent, ok := o.registry.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("orchestrator: agent %q not found", name)
	}
	return agent, nil
}

func (o *Orchestrator) agentDescriptors() []AgentDescriptor {
	names := o.registry.ListNames()
	descriptors := make([]AgentDescriptor, 0, len(names))
	for _, name := range names {
		descriptors = append(descriptors, AgentDescriptor{Name: name})
	}
	return descriptors
}
