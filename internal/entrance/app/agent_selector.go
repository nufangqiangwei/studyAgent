package app

import (
	"agent/internal/agent"
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

type agentSelector struct {
	ctx      context.Context
	registry *agent.Registry
	opts     agent.CreatAgentOptions
	current  agent.Agent
}

func newAgentSelector(ctx context.Context, registry *agent.Registry, initialName string, opts agent.CreatAgentOptions) (*agentSelector, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if registry == nil {
		return nil, fmt.Errorf("agent selector: registry is not configured")
	}
	if strings.TrimSpace(initialName) == "" {
		return nil, fmt.Errorf("agent selector: initial agent name is required")
	}

	selector := &agentSelector{
		ctx:      ctx,
		registry: registry,
		opts:     opts,
	}
	if err := selector.SelectAgent(initialName); err != nil {
		return nil, err
	}
	return selector, nil
}

func (s *agentSelector) Run(ctx context.Context, task string) error {
	if s == nil || s.current == nil {
		return fmt.Errorf("agent selector: active agent is not configured")
	}
	return s.current.Run(ctx, task)
}

func (s *agentSelector) Submit(ctx context.Context, task string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.Submit(ctx, task)
}

func (s *agentSelector) Recover(ctx context.Context) (content.AsyncRecoverResult, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRecoverResult{}, err
	}
	return async.Recover(ctx)
}

func (s *agentSelector) Work(ctx context.Context) (content.AsyncWorkResult, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	return async.Work(ctx)
}

func (s *agentSelector) Advance(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.Advance(ctx, runID)
}

func (s *agentSelector) DispatchNextEffect(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.DispatchNextEffect(ctx, runID)
}

func (s *agentSelector) SubmitUserInput(ctx context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.SubmitUserInput(ctx, runID, answer)
}

func (s *agentSelector) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.SubmitUserApproval(ctx, runID, approved, reason)
}

func (s *agentSelector) Result(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	async, err := s.asyncAgent()
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return async.Result(ctx, runID)
}

func (s *agentSelector) ActiveAgentName() string {
	if s == nil || s.current == nil {
		return ""
	}
	return s.current.Name()
}

func (s *agentSelector) ListAgentNames() []string {
	if s == nil || s.registry == nil {
		return nil
	}
	return s.registry.ListAgentNames()
}

func (s *agentSelector) SelectAgent(name string) error {
	if s == nil || s.registry == nil {
		return fmt.Errorf("agent selector: registry is not configured")
	}
	canonicalName, err := s.canonicalAgentName(name)
	if err != nil {
		return err
	}
	factory, err := s.registry.SelectAgent(canonicalName)
	if err != nil {
		return err
	}
	nextAgent, err := factory(s.ctx, s.opts)
	if err != nil {
		return err
	}
	s.current = nextAgent
	return nil
}

func (s *agentSelector) asyncAgent() (content.AsyncAgentRunner, error) {
	if s == nil || s.current == nil {
		return nil, fmt.Errorf("agent selector: active agent is not configured")
	}
	async, ok := s.current.(content.AsyncAgentRunner)
	if !ok {
		return nil, fmt.Errorf("agent selector: active agent %q does not support async runtime", s.current.Name())
	}
	return async, nil
}

func (s *agentSelector) canonicalAgentName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("agent selector: agent name is required")
	}
	for _, candidate := range s.registry.ListAgentNames() {
		if strings.EqualFold(candidate, name) {
			return candidate, nil
		}
	}
	return name, nil
}
