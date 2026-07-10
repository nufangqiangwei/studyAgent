package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"context"
)

type BuildFirstMessageHook func(ctx context.Context, input agents2.AgentStartInput) ([]agents2.Message, error)

// AgentRuntimeHooks is the extension point reserved for agentRuntime lifecycle
// customization. Hook methods should be added here as runtime hook points are
// introduced.
type AgentRuntimeHooks struct {
	BuildSystemPrompt BuildFirstMessageHook
}
