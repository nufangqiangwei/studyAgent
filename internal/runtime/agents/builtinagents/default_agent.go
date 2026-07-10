package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents/prompt"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

const DefaultAgentName = "default"

type DefaultAgentOption = AgentOption

type DefaultAgent struct {
	runtime *agentRuntime
}

func NewDefaultAgent(options ...AgentOption) (*DefaultAgent, error) {
	agent := &DefaultAgent{}
	base := []AgentOption{
		WithAgentSource("agent.default"),
	}
	base = append(base, options...)
	base = append(base,
		withAgentName(DefaultAgentName),
		WithSystemPrompt(prompt.DefaultSystemPrompt),
		WithAgentRuntimeHooks(AgentRuntimeHooks{BuildSystemPrompt: agent.BuildSystemPrompt}),
	)
	runtime, err := newAgentRuntime(agentRuntimeDefaults{
		name:        DefaultAgentName,
		source:      "agent.default",
		errorPrefix: "default agent",
	}, base...)
	if err != nil {
		return nil, err
	}
	agent.runtime = runtime
	return agent, nil
}

func (a *DefaultAgent) Name() string {
	if a == nil || a.runtime == nil {
		return DefaultAgentName
	}
	return a.runtime.agentName()
}

func (a *DefaultAgent) Start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.start(ctx, input)
}

func (a *DefaultAgent) Resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.resume(ctx, input)
}

func (a *DefaultAgent) Snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentSnapshot{}, false, err
	}
	return runtime.snapshot(ctx, taskID)
}

func (a *DefaultAgent) core() (*agentRuntime, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("default agent is nil")
	}
	return a.runtime, nil
}

func (a *DefaultAgent) BuildSystemPrompt(ctx context.Context, input agents2.AgentStartInput) ([]agents2.Message, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("default agent is nil")
	}
	systemPrompt := strings.Join([]string{
		prompt.DefaultSystemPrompt,
		prompt.DefaultUserHabitContext,
		a.buildTaskContext(input),
	}, "\n\n")
	return []agents2.Message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: input.Input},
	}, nil
}

func (a *DefaultAgent) buildTaskContext(input agents2.AgentStartInput) string {
	now := a.runtime.now().Format(time.RFC3339)
	return strings.NewReplacer(
		"当前时间：", "当前时间："+now,
		"当前地点：", "当前地点：未提供",
		"用户原始输入：", "用户原始输入："+strings.TrimSpace(input.Input),
		"当前可用工具：", "当前可用工具："+formatDefaultTaskTools(a.runtime.tools),
		"本轮已知事实：", "本轮已知事实："+formatDefaultKnownFacts(input),
		"本轮缺失事实：", "本轮缺失事实："+formatDefaultMissingFacts(),
	).Replace(prompt.TaskContext)
}

func formatDefaultTaskTools(tools []agents2.ToolSpec) string {
	if len(tools) == 0 {
		return "无"
	}
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "无"
	}
	return "\n- " + strings.Join(names, "\n- ")
}

func formatDefaultKnownFacts(input agents2.AgentStartInput) string {
	facts := []string{
		"任务 ID：" + strings.TrimSpace(input.TaskID),
		"用户原始输入：" + strings.TrimSpace(input.Input),
	}
	if len(input.Metadata) > 0 {
		keys := make([]string, 0, len(input.Metadata))
		for key := range input.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := input.Metadata[key]
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if key != "" || value != "" {
				facts = append(facts, key+"："+value)
			}
		}
	}
	return "\n- " + strings.Join(facts, "\n- ")
}

func formatDefaultMissingFacts() string {
	return "\n- 当前地点：未提供\n- 相关用户习惯：当前没有可用数据\n- 其他任务细节：由模型根据任务风险判断是否需要继续补全"
}
