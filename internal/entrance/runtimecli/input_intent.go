package runtimecli

import (
	"context"
	"strings"
	"unicode/utf8"
)

type InputIntentAnalyzer interface {
	AnalyzeInputIntent(ctx context.Context, request InputIntentRequest) (InputIntentDecision, error)
}

type InputIntentRequest struct {
	Input              string
	WorkDir            string
	Model              string
	HasCurrentTask     bool
	CurrentTaskID      string
	CurrentTaskPhase   string
	PendingPreprocess  bool
	PendingUserRequest bool
}

type InputIntentRoute string

const (
	InputIntentAppendCurrent InputIntentRoute = "append_current"
	InputIntentStartNewTask  InputIntentRoute = "start_new_task"
)

type InputTaskComplexity string

const (
	InputTaskSimple  InputTaskComplexity = "simple"
	InputTaskComplex InputTaskComplexity = "complex"
)

type InputIntentDecision struct {
	Route             InputIntentRoute
	NewTaskComplexity InputTaskComplexity
	Confidence        float64
	Reason            string
}

type RuleInputIntentAnalyzer struct{}

func NewRuleInputIntentAnalyzer() RuleInputIntentAnalyzer {
	return RuleInputIntentAnalyzer{}
}

func (RuleInputIntentAnalyzer) AnalyzeInputIntent(ctx context.Context, request InputIntentRequest) (InputIntentDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return InputIntentDecision{}, err
	}

	input := strings.TrimSpace(request.Input)
	decision := InputIntentDecision{
		Route:             InputIntentStartNewTask,
		NewTaskComplexity: classifyNewTaskComplexity(input),
		Confidence:        0.85,
		Reason:            "no current task is active",
	}
	if !request.HasCurrentTask {
		return normalizeInputIntentDecision(decision, request), nil
	}

	decision = InputIntentDecision{
		Route:             InputIntentAppendCurrent,
		NewTaskComplexity: classifyNewTaskComplexity(input),
		Confidence:        0.6,
		Reason:            "current task is active and the input has no explicit new-task marker",
	}

	switch {
	case hasExplicitNewTaskMarker(input):
		decision.Route = InputIntentStartNewTask
		decision.Confidence = 0.9
		decision.Reason = "input explicitly asks to start a new task"
	case hasExplicitAppendMarker(input):
		decision.Route = InputIntentAppendCurrent
		decision.Confidence = 0.9
		decision.Reason = "input explicitly refers to the current task"
	}

	return normalizeInputIntentDecision(decision, request), nil
}

func normalizeInputIntentDecision(decision InputIntentDecision, request InputIntentRequest) InputIntentDecision {
	switch decision.Route {
	case InputIntentAppendCurrent, InputIntentStartNewTask:
	default:
		if request.HasCurrentTask {
			decision.Route = InputIntentAppendCurrent
			decision.Reason = "invalid analyzer route; defaulted to current task"
		} else {
			decision.Route = InputIntentStartNewTask
			decision.Reason = "invalid analyzer route; defaulted to new task"
		}
	}
	switch decision.NewTaskComplexity {
	case InputTaskSimple, InputTaskComplex:
	default:
		decision.NewTaskComplexity = classifyNewTaskComplexity(request.Input)
	}
	if decision.Confidence < 0 {
		decision.Confidence = 0
	}
	if decision.Confidence > 1 {
		decision.Confidence = 1
	}
	return decision
}

func hasExplicitNewTaskMarker(input string) bool {
	text := normalizedIntentText(input)
	if startsWithAny(text,
		"\u65b0\u4efb\u52a1", "\u65b0\u5efa\u4efb\u52a1", "\u521b\u5efa\u65b0\u4efb\u52a1",
		"\u5f00\u65b0\u4efb\u52a1", "\u53e6\u4e00\u4e2a\u4efb\u52a1", "\u53e6\u5916\u4e00\u4e2a\u4efb\u52a1",
		"\u4e0b\u4e00\u4e2a\u4efb\u52a1", "\u6362\u4e2a\u4efb\u52a1", "\u91cd\u65b0\u5f00\u59cb",
		"\u53e6\u8d77\u4e00\u4e2a\u4efb\u52a1",
		"new task", "start a new task", "create a new task", "another task",
		"next task", "switch task", "restart with",
	) {
		return true
	}
	return containsAny(text,
		"\u5e2e\u6211\u5f00\u4e00\u4e2a\u65b0\u4efb\u52a1",
		"\u5e2e\u6211\u521b\u5efa\u4e00\u4e2a\u65b0\u4efb\u52a1",
		"\u5148\u505a\u53e6\u4e00\u4e2a\u4efb\u52a1",
		"start another task", "open another task",
	)
}

func hasExplicitAppendMarker(input string) bool {
	text := normalizedIntentText(input)
	if startsWithAny(text,
		"\u8865\u5145", "\u8ffd\u52a0", "\u7ee7\u7eed", "\u63a5\u7740", "\u5f53\u524d\u4efb\u52a1",
		"\u5bf9\u5f53\u524d\u4efb\u52a1", "\u8fd9\u4e2a\u4efb\u52a1", "\u4e0a\u9762\u8fd9\u4e2a",
		"\u521a\u624d\u90a3\u4e2a", "\u524d\u9762\u90a3\u4e2a", "\u6539\u6210", "\u6539\u4e3a",
		"\u4e0d\u8981", "\u53ea\u9700\u8981",
		"continue", "also", "for the current task", "for this task",
		"update that", "change it", "make it", "instead",
	) {
		return true
	}
	return containsAny(text,
		"\u5bf9\u4e0a\u9762\u7684\u4efb\u52a1", "\u5728\u5f53\u524d\u4efb\u52a1\u91cc",
		"\u57fa\u4e8e\u521a\u624d", "\u521a\u624d\u7684\u4efb\u52a1", "\u4e0a\u4e00\u4e2a\u4efb\u52a1",
		"same task", "current task", "previous task", "the thing above",
	)
}

func classifyNewTaskComplexity(input string) InputTaskComplexity {
	text := normalizedIntentText(input)
	if text == "" {
		return InputTaskSimple
	}

	score := 0
	length := utf8.RuneCountInString(text)
	switch {
	case length >= 120:
		score += 2
	case length >= 70:
		score++
	}
	if containsAny(text,
		"\u91cd\u6784", "\u67b6\u6784", "\u8bbe\u8ba1", "\u8fc1\u79fb", "\u96c6\u6210",
		"\u72b6\u6001\u673a", "\u5de5\u4f5c\u6d41", "\u591a agent", "\u591a\u6a21\u578b",
		"\u53ef\u6062\u590d", "\u5f02\u6b65", "\u6301\u4e45\u5316", "\u7aef\u5230\u7aef",
		"\u6d4b\u8bd5\u8986\u76d6", "\u62c6\u5206",
		"refactor", "architecture", "design", "migration", "integrate", "workflow",
		"state machine", "multi-agent", "resumable", "async", "persistence", "e2e",
	) {
		score++
	}
	if containsAny(text,
		"\u5e76\u4e14", "\u540c\u65f6", "\u7136\u540e", "\u6700\u540e", "\u4ee5\u53ca",
		"\u987a\u4fbf", "\u5206\u6b65\u9aa4", "\u62c6\u6210", "\u591a\u4e2a",
		"and then", "at the same time", "also", "finally", "step by step",
	) {
		score++
	}
	if containsAny(text, "1.", "2.", "3.", "\u4e00\u3001", "\u4e8c\u3001", "- ") {
		score++
	}
	if score >= 2 {
		return InputTaskComplex
	}
	return InputTaskSimple
}

func normalizedIntentText(input string) string {
	text := strings.TrimSpace(input)
	text = strings.ToLower(text)
	text = strings.TrimLeft(text, " \t\r\n:\uff1a-\u2014*#")
	return text
}

func startsWithAny(text string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		prefix = strings.ToLower(strings.TrimSpace(prefix))
		if prefix != "" && strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func containsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" && strings.Contains(text, needle) {
			return true
		}
	}
	return false
}
