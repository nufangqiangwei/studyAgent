package runtimecli

import (
	"context"
	"testing"
)

func TestRuleInputIntentAnalyzerDefaultsToNewTaskWithoutCurrentTask(t *testing.T) {
	analyzer := NewRuleInputIntentAnalyzer()

	decision, err := analyzer.AnalyzeInputIntent(context.Background(), InputIntentRequest{
		Input: "\u89e3\u91ca\u4e00\u4e0b internal/runtime \u7684\u72b6\u6001\u673a",
	})
	if err != nil {
		t.Fatalf("AnalyzeInputIntent returned error: %v", err)
	}

	if decision.Route != InputIntentStartNewTask {
		t.Fatalf("route = %q, want new task", decision.Route)
	}
	if decision.NewTaskComplexity != InputTaskSimple {
		t.Fatalf("complexity = %q, want simple", decision.NewTaskComplexity)
	}
}

func TestRuleInputIntentAnalyzerDefaultsToAppendWithCurrentTask(t *testing.T) {
	analyzer := NewRuleInputIntentAnalyzer()

	decision, err := analyzer.AnalyzeInputIntent(context.Background(), InputIntentRequest{
		Input:          "\u8fd9\u91cc\u4f18\u5148\u4fdd\u6301\u63a5\u53e3\u8fb9\u754c\u6e05\u6670",
		HasCurrentTask: true,
	})
	if err != nil {
		t.Fatalf("AnalyzeInputIntent returned error: %v", err)
	}

	if decision.Route != InputIntentAppendCurrent {
		t.Fatalf("route = %q, want append current", decision.Route)
	}
	if decision.Confidence <= 0 || decision.Confidence >= 1 {
		t.Fatalf("confidence = %v, want normalized value", decision.Confidence)
	}
}

func TestRuleInputIntentAnalyzerRecognizesExplicitRoutes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  InputIntentRoute
	}{
		{
			name:  "chinese new task",
			input: "\u65b0\u4efb\u52a1\uff1a\u7ed9 runtimecli \u52a0\u4e00\u4e2a\u8f93\u5165\u610f\u56fe\u5206\u7c7b\u5668",
			want:  InputIntentStartNewTask,
		},
		{
			name:  "english new task",
			input: "new task: add input routing tests",
			want:  InputIntentStartNewTask,
		},
		{
			name:  "chinese append",
			input: "\u8865\u5145\uff1a\u5f53\u524d\u4efb\u52a1\u91cc\u4e0d\u8981\u6539 NativeLoop",
			want:  InputIntentAppendCurrent,
		},
		{
			name:  "english append",
			input: "for the current task, keep the runtime generic",
			want:  InputIntentAppendCurrent,
		},
	}

	analyzer := NewRuleInputIntentAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := analyzer.AnalyzeInputIntent(context.Background(), InputIntentRequest{
				Input:          tt.input,
				HasCurrentTask: true,
			})
			if err != nil {
				t.Fatalf("AnalyzeInputIntent returned error: %v", err)
			}
			if decision.Route != tt.want {
				t.Fatalf("route = %q, want %q", decision.Route, tt.want)
			}
		})
	}
}

func TestRuleInputIntentAnalyzerClassifiesComplexNewTask(t *testing.T) {
	analyzer := NewRuleInputIntentAnalyzer()

	decision, err := analyzer.AnalyzeInputIntent(context.Background(), InputIntentRequest{
		Input: "\u65b0\u4efb\u52a1\uff1a\u91cd\u6784 runtimecli \u7684\u8f93\u5165\u8def\u7531\uff0c\u5e76\u4e14\u628a\u610f\u56fe\u5206\u6790\u3001\u4efb\u52a1\u9884\u5904\u7406\u3001\u72b6\u6001\u673a\u4e8b\u4ef6\u8bb0\u5f55\u62c6\u6210\u6e05\u6670\u7684\u63a5\u53e3\u548c\u6d4b\u8bd5",
	})
	if err != nil {
		t.Fatalf("AnalyzeInputIntent returned error: %v", err)
	}

	if decision.NewTaskComplexity != InputTaskComplex {
		t.Fatalf("complexity = %q, want complex", decision.NewTaskComplexity)
	}
}

func TestNormalizeInputIntentDecisionFallsBackConservatively(t *testing.T) {
	decision := normalizeInputIntentDecision(InputIntentDecision{
		Route:             "unknown",
		NewTaskComplexity: "unknown",
		Confidence:        2,
	}, InputIntentRequest{
		Input:          "\u7ee7\u7eed\u6cbf\u7528\u73b0\u5728\u7684\u63a5\u53e3",
		HasCurrentTask: true,
	})

	if decision.Route != InputIntentAppendCurrent {
		t.Fatalf("route = %q, want append current", decision.Route)
	}
	if decision.NewTaskComplexity != InputTaskSimple {
		t.Fatalf("complexity = %q, want simple", decision.NewTaskComplexity)
	}
	if decision.Confidence != 1 {
		t.Fatalf("confidence = %v, want clamped value", decision.Confidence)
	}
}
