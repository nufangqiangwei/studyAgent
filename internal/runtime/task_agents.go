package runtime

import (
	"agent/internal/runtime/agents"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type taskAgentRegistry struct {
	mu     sync.RWMutex
	agents map[string]agents.Agent
}

func newTaskAgentRegistry() *taskAgentRegistry {
	return &taskAgentRegistry{agents: make(map[string]agents.Agent)}
}

func (r *taskAgentRegistry) Register(taskID string, agent agents.Agent) error {
	if r == nil {
		return fmt.Errorf("task agent registry is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task agent registry: task_id is required")
	}
	if agent == nil {
		return fmt.Errorf("task agent registry %q: agent is required", taskID)
	}
	agentName := strings.TrimSpace(agent.Name())
	if agentName == "" {
		return fmt.Errorf("task agent registry %q: agent name is required", taskID)
	}

	key := taskAgentKey(taskID, agentName)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.agents[key]; exists {
		return fmt.Errorf("agent %q for task %q already exists", agentName, taskID)
	}
	r.agents[key] = agent
	return nil
}

func (r *taskAgentRegistry) Unregister(taskID string, agentName string) bool {
	if r == nil {
		return false
	}
	key := taskAgentKey(taskID, agentName)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.agents[key]; !exists {
		return false
	}
	delete(r.agents, key)
	return true
}

func (r *taskAgentRegistry) Lookup(taskID string, agentName string) (agents.Agent, bool) {
	if r == nil {
		return nil, false
	}
	taskID = strings.TrimSpace(taskID)
	agentName = strings.TrimSpace(agentName)
	if taskID == "" || agentName == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	agent, ok := r.agents[taskAgentKey(taskID, agentName)]
	return agent, ok
}

func (r *taskAgentRegistry) List() []TaskAgentRegistration {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TaskAgentRegistration, 0, len(r.agents))
	for key, agent := range r.agents {
		taskID, agentName := splitTaskAgentKey(key)
		out = append(out, TaskAgentRegistration{
			TaskID: taskID,
			Agent:  agentName,
			agent:  agent,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TaskID == out[j].TaskID {
			return out[i].Agent < out[j].Agent
		}
		return out[i].TaskID < out[j].TaskID
	})
	return out
}

type TaskAgentRegistration struct {
	TaskID string `json:"task_id"`
	Agent  string `json:"agent"`
	agent  agents.Agent
}

func taskAgentKey(taskID string, agentName string) string {
	return strings.TrimSpace(taskID) + "\x00" + strings.TrimSpace(agentName)
}

func splitTaskAgentKey(key string) (string, string) {
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) != 2 {
		return key, ""
	}
	return parts[0], parts[1]
}
