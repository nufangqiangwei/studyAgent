package taskpreprocess

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ActionProceed Action = "proceed"
	ActionAskUser Action = "ask_user"

	defaultMaxQuestions = 1
)

type Action string

type Request struct {
	Input          string
	WorkDir        string
	Model          string
	MaxQuestions   int
	Clarifications []Clarification
}

type Clarification struct {
	QuestionID string `json:"question_id,omitempty"`
	Question   string `json:"question"`
	Answer     string `json:"answer"`
}

type Result struct {
	Action             Action          `json:"action"`
	OriginalInput      string          `json:"original_input,omitempty"`
	NormalizedTask     string          `json:"normalized_task,omitempty"`
	Summary            string          `json:"summary,omitempty"`
	Steps              []Step          `json:"steps,omitempty"`
	Questions          []Question      `json:"questions,omitempty"`
	MissingInformation []string        `json:"missing_information,omitempty"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
}

type Step struct {
	ID   string `json:"id,omitempty"`
	Goal string `json:"goal"`
}

type Question struct {
	ID     string `json:"id,omitempty"`
	Prompt string `json:"prompt"`
}

type Analyzer interface {
	Analyze(ctx context.Context, request Request) (Result, error)
}

type Processor struct {
	analyzer Analyzer
}

func NewProcessor(analyzer Analyzer) (*Processor, error) {
	if analyzer == nil {
		return nil, fmt.Errorf("task preprocess: analyzer is required")
	}
	return &Processor{analyzer: analyzer}, nil
}

func (p *Processor) Preprocess(ctx context.Context, request Request) (Result, error) {
	if p == nil || p.analyzer == nil {
		return Result{}, fmt.Errorf("task preprocess: processor is not configured")
	}
	request.Input = strings.TrimSpace(request.Input)
	request.WorkDir = strings.TrimSpace(request.WorkDir)
	request.Model = strings.TrimSpace(request.Model)
	request.MaxQuestions = normalizeMaxQuestions(request.MaxQuestions)
	request.Clarifications = cleanClarifications(request.Clarifications)
	if request.Input == "" {
		return Result{}, fmt.Errorf("task preprocess: input is required")
	}

	result, err := p.analyzer.Analyze(ctx, request)
	if err != nil {
		return Result{}, err
	}
	return normalizeResult(result, request)
}

func (r Result) AgentTask() string {
	original := strings.TrimSpace(r.OriginalInput)
	normalized := strings.TrimSpace(r.NormalizedTask)
	if normalized == "" {
		normalized = original
	}
	if len(r.Steps) == 0 && strings.TrimSpace(r.Summary) == "" && original == normalized {
		return normalized
	}

	var b strings.Builder
	b.WriteString("Task preprocessing result:\n")
	if original != "" {
		b.WriteString("Original input:\n")
		b.WriteString(original)
		b.WriteString("\n\n")
	}
	if normalized != "" {
		b.WriteString("Normalized task:\n")
		b.WriteString(normalized)
		b.WriteString("\n\n")
	}
	if summary := strings.TrimSpace(r.Summary); summary != "" {
		b.WriteString("Summary:\n")
		b.WriteString(summary)
		b.WriteString("\n\n")
	}
	if len(r.Steps) > 0 {
		b.WriteString("Decomposed steps:\n")
		for i, step := range r.Steps {
			goal := strings.TrimSpace(step.Goal)
			if goal == "" {
				continue
			}
			id := strings.TrimSpace(step.ID)
			if id == "" {
				id = fmt.Sprintf("%d", i+1)
			}
			b.WriteString(id)
			b.WriteString(". ")
			b.WriteString(goal)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("Instruction: Do not invent missing requirements. If more required information is discovered during execution, ask the user before proceeding.")
	return strings.TrimSpace(b.String())
}

type LLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

type ModelAnalyzer struct {
	client      LLMClient
	model       string
	source      string
	temperature float64
}

type ModelAnalyzerOption func(*ModelAnalyzer)

func WithSource(source string) ModelAnalyzerOption {
	return func(a *ModelAnalyzer) {
		a.source = strings.TrimSpace(source)
	}
}

func WithTemperature(temperature float64) ModelAnalyzerOption {
	return func(a *ModelAnalyzer) {
		a.temperature = temperature
	}
}

func NewModelAnalyzer(client LLMClient, model string, options ...ModelAnalyzerOption) (*ModelAnalyzer, error) {
	if client == nil {
		return nil, fmt.Errorf("task preprocess model analyzer: client is required")
	}
	analyzer := &ModelAnalyzer{
		client: client,
		model:  strings.TrimSpace(model),
		source: "task_preprocess",
	}
	for _, option := range options {
		if option != nil {
			option(analyzer)
		}
	}
	if analyzer.source == "" {
		analyzer.source = "task_preprocess"
	}
	return analyzer, nil
}

func (a *ModelAnalyzer) Analyze(ctx context.Context, request Request) (Result, error) {
	if a == nil || a.client == nil {
		return Result{}, fmt.Errorf("task preprocess model analyzer is not configured")
	}
	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = a.model
	}
	if model == "" {
		return Result{}, fmt.Errorf("task preprocess model analyzer: model is required")
	}

	payload := modelPromptPayload{
		OriginalInput:    request.Input,
		WorkDir:          request.WorkDir,
		MaxQuestions:     normalizeMaxQuestions(request.MaxQuestions),
		Clarifications:   cleanClarifications(request.Clarifications),
		ResponseContract: responseContract(),
	}
	userPayload, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("task preprocess: marshal prompt payload: %w", err)
	}

	response, err := a.client.Complete(ctx, llmClient.Request{
		Model: model,
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: systemPrompt(payload.MaxQuestions)},
			{Role: llmClient.RoleUser, Content: string(userPayload)},
		},
		Temperature: a.temperature,
		Metadata: map[string]string{
			"purpose": "task_preprocess",
			"source":  a.source,
		},
	})
	if err != nil {
		return Result{}, fmt.Errorf("task preprocess model call: %w", err)
	}

	result, err := decodeModelResult(response.Content)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

type modelPromptPayload struct {
	OriginalInput    string          `json:"original_input"`
	WorkDir          string          `json:"work_dir,omitempty"`
	MaxQuestions     int             `json:"max_questions"`
	Clarifications   []Clarification `json:"clarifications,omitempty"`
	ResponseContract map[string]any  `json:"response_contract"`
}

func systemPrompt(maxQuestions int) string {
	return strings.TrimSpace(fmt.Sprintf(`You are a task preprocessor for an interactive CLI coding agent.
Analyze the user's non-command input before the main agent sees it.
Your job is to understand the task, decompose it, and decide whether the task can safely start.

Return exactly one JSON object. Do not include markdown outside JSON.

Rules:
- If the task can start with the available information, use action "proceed".
- If critical information is missing and the main agent would need to guess the user's intent, use action "ask_user".
- Ask at most %d concise question(s).
- Ask only for information that cannot reasonably be discovered from the repository or tools.
- Do not ask for implementation details unless the user must choose between materially different outcomes.
- Do not invent project facts, user preferences, targets, constraints, or acceptance criteria.
- For "proceed", provide a normalized task and short decomposed steps when useful.
- For "ask_user", include the missing information and the exact question to show the user.`, normalizeMaxQuestions(maxQuestions)))
}

func responseContract() map[string]any {
	return map[string]any{
		"proceed": map[string]any{
			"action":          string(ActionProceed),
			"normalized_task": "clear task text for the main agent",
			"summary":         "brief understanding of the task",
			"steps":           []map[string]string{{"id": "1", "goal": "first concrete step"}},
		},
		"ask_user": map[string]any{
			"action":              string(ActionAskUser),
			"missing_information": []string{"what is missing"},
			"questions":           []map[string]string{{"id": "q1", "prompt": "concise question"}},
		},
	}
}

func decodeModelResult(content string) (Result, error) {
	content = stripJSONFence(strings.TrimSpace(content))
	if content == "" {
		return Result{}, fmt.Errorf("task preprocess: model returned empty response")
	}

	var result Result
	if err := json.Unmarshal([]byte(content), &result); err == nil && result.Action != "" {
		return result, nil
	}

	var envelope struct {
		Result     Result `json:"result"`
		Preprocess Result `json:"preprocess"`
		Decision   Result `json:"decision"`
	}
	if err := json.Unmarshal([]byte(content), &envelope); err != nil {
		return Result{}, fmt.Errorf("task preprocess: decode model response: %w", err)
	}
	switch {
	case envelope.Result.Action != "":
		return envelope.Result, nil
	case envelope.Preprocess.Action != "":
		return envelope.Preprocess, nil
	case envelope.Decision.Action != "":
		return envelope.Decision, nil
	default:
		return Result{}, fmt.Errorf("task preprocess: model response action is required")
	}
}

func stripJSONFence(content string) string {
	content = strings.TrimSpace(content)
	if !strings.HasPrefix(content, "```") {
		return content
	}
	lines := strings.Split(content, "\n")
	if len(lines) < 2 {
		return content
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func normalizeResult(result Result, request Request) (Result, error) {
	result.Action = Action(strings.TrimSpace(string(result.Action)))
	result.OriginalInput = strings.TrimSpace(result.OriginalInput)
	if result.OriginalInput == "" {
		result.OriginalInput = request.Input
	}
	result.NormalizedTask = strings.TrimSpace(result.NormalizedTask)
	result.Summary = strings.TrimSpace(result.Summary)
	result.Steps = cleanSteps(result.Steps)
	result.Questions = cleanQuestions(result.Questions, request.MaxQuestions)
	result.MissingInformation = cleanStrings(result.MissingInformation)

	switch result.Action {
	case ActionProceed:
		if result.NormalizedTask == "" {
			result.NormalizedTask = request.Input
		}
		if result.NormalizedTask == "" {
			return Result{}, fmt.Errorf("task preprocess: normalized task is required")
		}
		result.Questions = nil
		result.MissingInformation = nil
	case ActionAskUser:
		if len(result.Questions) == 0 {
			return Result{}, fmt.Errorf("task preprocess: ask_user requires at least one question")
		}
		result.NormalizedTask = ""
		result.Steps = nil
	default:
		return Result{}, fmt.Errorf("task preprocess: unsupported action %q", result.Action)
	}
	return result, nil
}

func normalizeMaxQuestions(maxQuestions int) int {
	if maxQuestions <= 0 {
		return defaultMaxQuestions
	}
	if maxQuestions > 3 {
		return 3
	}
	return maxQuestions
}

func cleanClarifications(values []Clarification) []Clarification {
	if len(values) == 0 {
		return nil
	}
	out := make([]Clarification, 0, len(values))
	for _, value := range values {
		question := strings.TrimSpace(value.Question)
		answer := strings.TrimSpace(value.Answer)
		if question == "" || answer == "" {
			continue
		}
		out = append(out, Clarification{
			QuestionID: strings.TrimSpace(value.QuestionID),
			Question:   question,
			Answer:     answer,
		})
	}
	return out
}

func cleanSteps(values []Step) []Step {
	if len(values) == 0 {
		return nil
	}
	out := make([]Step, 0, len(values))
	for _, value := range values {
		goal := strings.TrimSpace(value.Goal)
		if goal == "" {
			continue
		}
		out = append(out, Step{ID: strings.TrimSpace(value.ID), Goal: goal})
	}
	return out
}

func cleanQuestions(values []Question, maxQuestions int) []Question {
	if len(values) == 0 {
		return nil
	}
	maxQuestions = normalizeMaxQuestions(maxQuestions)
	out := make([]Question, 0, minInt(len(values), maxQuestions))
	for _, value := range values {
		prompt := strings.TrimSpace(value.Prompt)
		if prompt == "" {
			continue
		}
		out = append(out, Question{ID: strings.TrimSpace(value.ID), Prompt: prompt})
		if len(out) >= maxQuestions {
			break
		}
	}
	return out
}

func cleanStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
