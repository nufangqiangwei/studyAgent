package builtinagents

import agents2 "agent/internal/runtime/agents"

func cloneMessages(messages []agents2.Message) []agents2.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]agents2.Message, 0, len(messages))
	for _, message := range messages {
		cloned = append(cloned, message.Clone())
	}
	return cloned
}

func cloneToolSpecs(tools []agents2.ToolSpec) []agents2.ToolSpec {
	if len(tools) == 0 {
		return nil
	}
	cloned := make([]agents2.ToolSpec, 0, len(tools))
	for _, tool := range tools {
		cloned = append(cloned, tool.Clone())
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
