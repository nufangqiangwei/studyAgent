package agents

func cloneMessages(messages []Message) []Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]Message, 0, len(messages))
	for _, message := range messages {
		cloned = append(cloned, message.Clone())
	}
	return cloned
}

func cloneToolSpecs(tools []ToolSpec) []ToolSpec {
	if len(tools) == 0 {
		return nil
	}
	cloned := make([]ToolSpec, 0, len(tools))
	for _, tool := range tools {
		cloned = append(cloned, tool.Clone())
	}
	return cloned
}
