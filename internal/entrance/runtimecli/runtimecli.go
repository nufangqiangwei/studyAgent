package runtimecli

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/entrance/cli"
	"agent/internal/foundation/llmClient"
	"agent/internal/foundation/policy"
	"agent/internal/runtime"
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents"
	eventbus2 "agent/internal/runtime/eventbus"
	reactor2 "agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	runtimetools "agent/internal/runtime/tools"
	"agent/internal/taskpreprocess"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

type LLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

type Options struct {
	LLM                 LLMClient
	Policy              policy.Checker
	TaskID              string
	Source              string
	Sync                bool
	Preprocessor        TaskPreprocessor
	InputIntentAnalyzer InputIntentAnalyzer
}

type TaskPreprocessor interface {
	Preprocess(ctx context.Context, request taskpreprocess.Request) (taskpreprocess.Result, error)
}

func Run(ctx context.Context, env content.Env, registry *command.Registry, options Options) error {
	session, err := NewSession(ctx, env, options)
	if err != nil {
		return err
	}
	defer session.Close()
	return cli.Run(ctx, env, registry, session.HandlePlainInput)
}

type Session struct {
	env               content.Env
	runtime           *runtime.Runtime
	toolSpecs         []agents2.ToolSpec
	preprocessor      TaskPreprocessor
	intentAnalyzer    InputIntentAnalyzer
	baseTaskID        string
	source            string
	taskSeq           int
	taskRuntime       *runtime.TaskRuntime
	pendingPreprocess *pendingPreprocess
	taskIDs           map[string]struct{}
	taskIDMu          sync.RWMutex
	mu                sync.Mutex
	outputMu          sync.Mutex
}

type pendingPreprocess struct {
	originalInput  string
	clarifications []taskpreprocess.Clarification
	question       taskpreprocess.Question
}

func NewSession(ctx context.Context, env content.Env, options Options) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	source := strings.TrimSpace(options.Source)
	if source == "" {
		source = "runtimecli"
	}
	toolAdapter, err := runtimetools.NewDefault(
		runtimetools.WithPolicy(options.Policy),
		runtimetools.WithWorkDir(env.Config.WorkDir),
		runtimetools.WithEnv(env),
		runtimetools.WithSource(source+".tools"),
	)
	if err != nil {
		return nil, err
	}
	modelExecutor, err := agents2.NewModelExecutor(&modelAdapter{client: options.LLM}, agents2.WithModelExecutorSource(source+".model"))
	if err != nil {
		return nil, err
	}

	runtimeOptions := []runtime.Option{
		runtime.WithSource(source),
		runtime.WithEffectExecutor(reactor2.EffectModelCall, modelExecutor),
		runtime.WithEffectExecutor(reactor2.EffectToolDispatch, toolAdapter),
		runtime.WithEffectExecutor(reactor2.EffectUserInputRequest, userInputRequestExecutor{}),
	}
	if options.Sync {
		runtimeOptions = append(runtimeOptions,
			runtime.WithSyncEffects(),
			runtime.WithResultDelivery(eventbus2.DeliverySync),
		)
	}
	rt, err := runtime.New(runtimeOptions...)
	if err != nil {
		return nil, err
	}
	preprocessor := options.Preprocessor
	if preprocessor == nil && options.LLM != nil {
		analyzer, err := taskpreprocess.NewModelAnalyzer(options.LLM, env.Config.Model, taskpreprocess.WithSource(source+".preprocess"))
		if err != nil {
			_ = rt.Close()
			return nil, err
		}
		preprocessor, err = taskpreprocess.NewProcessor(analyzer)
		if err != nil {
			_ = rt.Close()
			return nil, err
		}
	}

	session := &Session{
		env:            env,
		runtime:        rt,
		toolSpecs:      toolAdapter.Specs(),
		preprocessor:   preprocessor,
		intentAnalyzer: defaultInputIntentAnalyzer(options.InputIntentAnalyzer),
		baseTaskID:     strings.TrimSpace(options.TaskID),
		source:         source,
		taskIDs:        make(map[string]struct{}),
	}
	if err := session.subscribeOutput(); err != nil {
		_ = rt.Close()
		return nil, err
	}
	return session, nil
}

func (s *Session) Close() error {
	if s == nil || s.runtime == nil {
		return nil
	}
	return s.runtime.Close()
}

func (s *Session) HandlePlainInput(ctx context.Context, env content.Env, line string) error {
	if s == nil {
		return fmt.Errorf("runtime cli session is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	input := strings.TrimSpace(line)
	if input == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.env = env

	if s.pendingPreprocess != nil {
		return s.continuePreprocessLocked(ctx, input)
	}

	if s.taskRuntime == nil {
		return s.preprocessAndStartTaskLocked(ctx, input)
	}
	state, ok, err := s.taskRuntime.State(ctx)
	if err != nil {
		return err
	}
	if !ok || state.Phase == statemachine2.PhaseCreated {
		return s.preprocessAndStartTaskLocked(ctx, input)
	}
	if state.IsTerminal() {
		s.taskRuntime = nil
		return s.preprocessAndStartTaskLocked(ctx, input)
	}

	switch state.Phase {
	case statemachine2.PhaseRunning:
		return s.handleRunningInputLocked(ctx, state, input)
	case statemachine2.PhaseWaitingUserInput:
		requestID := ""
		if state.PendingUserInput != nil {
			requestID = state.PendingUserInput.RequestID
		}
		return s.publishUserInputLocked(ctx, requestID, input)
	case statemachine2.PhaseWaitingModel, statemachine2.PhaseWaitingTool, statemachine2.PhaseWaitingSubAgent:
		return fmt.Errorf("task %s is %s; wait for the current step to finish", state.TaskID, state.Phase)
	default:
		return fmt.Errorf("task %s cannot accept user input in phase %s", state.TaskID, state.Phase)
	}
}

func (s *Session) handleRunningInputLocked(ctx context.Context, state statemachine2.TaskState, input string) error {
	decision, err := s.analyzeInputIntentLocked(ctx, input, state)
	if err != nil {
		return err
	}
	if decision.Route != InputIntentStartNewTask {
		return s.publishUserInputLocked(ctx, "", input)
	}

	previousTask := s.taskRuntime
	s.taskRuntime = nil
	if err := s.preprocessAndStartTaskLocked(ctx, input); err != nil {
		s.taskRuntime = previousTask
		return err
	}
	return nil
}

func (s *Session) analyzeInputIntentLocked(ctx context.Context, input string, state statemachine2.TaskState) (InputIntentDecision, error) {
	analyzer := defaultInputIntentAnalyzer(s.intentAnalyzer)
	return analyzer.AnalyzeInputIntent(ctx, InputIntentRequest{
		Input:            input,
		WorkDir:          s.env.Config.WorkDir,
		Model:            s.env.Config.Model,
		HasCurrentTask:   true,
		CurrentTaskID:    state.TaskID,
		CurrentTaskPhase: string(state.Phase),
	})
}

func defaultInputIntentAnalyzer(analyzer InputIntentAnalyzer) InputIntentAnalyzer {
	if analyzer != nil {
		return analyzer
	}
	defaultAnalyzer := NewRuleInputIntentAnalyzer()
	return defaultAnalyzer
}

func (s *Session) preprocessAndStartTaskLocked(ctx context.Context, input string) error {
	result, err := s.preprocessLocked(ctx, input, nil)
	if err != nil {
		return err
	}
	if result.Action == taskpreprocess.ActionAskUser {
		return s.setPendingPreprocessLocked(input, nil, result)
	}
	if err := s.ensureTaskLocked(ctx); err != nil {
		return err
	}
	return s.startTaskLocked(ctx, result.AgentTask())
}

func (s *Session) continuePreprocessLocked(ctx context.Context, answer string) error {
	pending := s.pendingPreprocess
	if pending == nil {
		return nil
	}
	clarifications := append([]taskpreprocess.Clarification(nil), pending.clarifications...)
	clarifications = append(clarifications, taskpreprocess.Clarification{
		QuestionID: pending.question.ID,
		Question:   pending.question.Prompt,
		Answer:     answer,
	})
	s.pendingPreprocess = nil

	result, err := s.preprocessLocked(ctx, pending.originalInput, clarifications)
	if err != nil {
		return err
	}
	if result.Action == taskpreprocess.ActionAskUser {
		return s.setPendingPreprocessLocked(pending.originalInput, clarifications, result)
	}
	if err := s.ensureTaskLocked(ctx); err != nil {
		return err
	}
	return s.startTaskLocked(ctx, result.AgentTask())
}

func (s *Session) preprocessLocked(ctx context.Context, input string, clarifications []taskpreprocess.Clarification) (taskpreprocess.Result, error) {
	input = strings.TrimSpace(input)
	if s.preprocessor == nil {
		return taskpreprocess.Result{
			Action:         taskpreprocess.ActionProceed,
			OriginalInput:  input,
			NormalizedTask: input,
		}, nil
	}
	return s.preprocessor.Preprocess(ctx, taskpreprocess.Request{
		Input:          input,
		WorkDir:        s.env.Config.WorkDir,
		Model:          s.env.Config.Model,
		MaxQuestions:   1,
		Clarifications: clarifications,
	})
}

func (s *Session) setPendingPreprocessLocked(input string, clarifications []taskpreprocess.Clarification, result taskpreprocess.Result) error {
	if len(result.Questions) == 0 {
		return fmt.Errorf("task preprocess requested user input without a question")
	}
	question := result.Questions[0]
	s.pendingPreprocess = &pendingPreprocess{
		originalInput:  strings.TrimSpace(input),
		clarifications: append([]taskpreprocess.Clarification(nil), clarifications...),
		question:       question,
	}
	return s.println("? " + question.Prompt)
}

func (s *Session) ensureTaskLocked(ctx context.Context) error {
	if s.taskRuntime != nil {
		return nil
	}
	taskID, err := s.nextTaskIDLocked()
	if err != nil {
		return err
	}
	agent, err := builtinagents.NewAnalyzeAgent(
		builtinagents.WithModelName(s.env.Config.Model),
		builtinagents.WithSnapshotStore(s.runtime.SnapshotStore()),
		builtinagents.WithTools(s.toolSpecs),
		builtinagents.WithSystemPrompt(systemPrompt(s.env.Config.WorkDir)),
		builtinagents.WithAgentSource(s.source+".agent"),
	)
	if err != nil {
		return err
	}
	taskRuntime, err := s.runtime.CreateTaskRuntime(ctx, taskID, agent)
	if err != nil {
		return err
	}
	s.taskRuntime = taskRuntime
	s.taskIDMu.Lock()
	s.taskIDs[taskID] = struct{}{}
	s.taskIDMu.Unlock()
	return nil
}

func (s *Session) nextTaskIDLocked() (string, error) {
	s.taskSeq++
	if s.baseTaskID != "" {
		if s.taskSeq == 1 {
			return s.baseTaskID, nil
		}
		return fmt.Sprintf("%s_%d", s.baseTaskID, s.taskSeq), nil
	}
	return newID("task")
}

func (s *Session) startTaskLocked(ctx context.Context, input string) error {
	_, err := s.taskRuntime.Start(ctx, runtime.TaskStart{
		Input: input,
		Metadata: map[string]string{
			"entry": "runtimecli",
		},
	})
	return err
}

func (s *Session) publishUserInputLocked(ctx context.Context, requestID string, answer string) error {
	if strings.TrimSpace(requestID) == "" {
		id, err := newID("input")
		if err != nil {
			return err
		}
		requestID = id
	}
	event, err := eventbus2.NewEvent(statemachine2.TopicTask, statemachine2.EventUserInputReceived, statemachine2.UserInputPayload{
		RequestID: requestID,
		Answer:    answer,
	}, eventbus2.WithTaskID(s.taskRuntime.TaskID()), eventbus2.WithSource(s.source))
	if err != nil {
		return err
	}
	_, err = s.taskRuntime.Publish(ctx, event)
	return err
}

func (s *Session) subscribeOutput() error {
	if s == nil || s.runtime == nil || s.runtime.EventBus() == nil {
		return fmt.Errorf("runtime cli event bus is not configured")
	}
	if _, err := s.runtime.EventBus().SubscribeReadOnlyFunc(eventbus2.Filter{
		Topic: runtime.TopicTaskResult,
	}, s.handleResultEvent, eventbus2.WithSubscriptionID(s.source+".result")); err != nil {
		return err
	}
	if _, err := s.runtime.EventBus().SubscribeReadOnlyFunc(eventbus2.Filter{
		Topic: statemachine2.TopicTask,
		Type:  statemachine2.EventAgentUserInputRequested,
	}, s.handleUserInputRequestEvent, eventbus2.WithSubscriptionID(s.source+".user_input")); err != nil {
		return err
	}
	return nil
}

func (s *Session) handleResultEvent(_ context.Context, event eventbus2.Event) error {
	if !s.ownsTask(event.TaskID) {
		return nil
	}
	var payload statemachine2.TaskTerminalPayload
	if len(event.Payload) > 0 {
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
	}
	switch event.Type {
	case runtime.EventTaskCompleted:
		text := resultText(payload.Result)
		if strings.TrimSpace(text) == "" {
			return nil
		}
		return s.println(text)
	case runtime.EventTaskFailed:
		message := "task failed"
		if payload.Error != nil && payload.Error.Message != "" {
			message = payload.Error.Message
		}
		return s.println("error: " + message)
	case runtime.EventTaskCancelled:
		return s.println("task cancelled")
	default:
		return nil
	}
}

func (s *Session) handleUserInputRequestEvent(_ context.Context, event eventbus2.Event) error {
	if !s.ownsTask(event.TaskID) {
		return nil
	}
	var payload statemachine2.UserInputPayload
	if len(event.Payload) > 0 {
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
	}
	prompt := strings.TrimSpace(payload.Prompt)
	if prompt == "" {
		prompt = "Input requested"
	}
	return s.println("? " + prompt)
}

func (s *Session) ownsTask(taskID string) bool {
	if s == nil {
		return false
	}
	s.taskIDMu.RLock()
	defer s.taskIDMu.RUnlock()
	_, ok := s.taskIDs[taskID]
	return ok
}

func (s *Session) println(text string) error {
	if s == nil || s.env.IO.Out == nil {
		return nil
	}
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	_, err := fmt.Fprintln(s.env.IO.Out, text)
	return err
}

type modelAdapter struct {
	client LLMClient
}

func (m *modelAdapter) Complete(ctx context.Context, request agents2.ModelRequest) (agents2.ModelResponse, error) {
	if m == nil || m.client == nil {
		return agents2.ModelResponse{}, fmt.Errorf("runtime cli model client is not configured")
	}
	response, err := m.client.Complete(ctx, llmClient.Request{
		Model:       request.Model,
		Messages:    llmMessages(request.Messages),
		Tools:       llmTools(request.Tools),
		Temperature: request.Temperature,
		Metadata:    cloneStringMap(request.Metadata),
	})
	if err != nil {
		return agents2.ModelResponse{}, err
	}
	metadata := map[string]string{
		"provider": response.Provider,
		"model":    response.Model,
	}
	if len(response.ToolCalls) > 0 {
		call := response.ToolCalls[0]
		arguments := append(json.RawMessage(nil), call.Input...)
		if strings.TrimSpace(string(arguments)) == "" {
			arguments = json.RawMessage(`{}`)
		}
		return agents2.ModelResponse{
			Content: strings.TrimSpace(response.Content),
			Decision: &agents2.Decision{
				Action: agents2.ActionUseTool,
				Tool: &agents2.ToolIntent{
					ToolCallID: call.ID,
					ToolName:   call.Name,
					Arguments:  arguments,
				},
			},
			Metadata: metadata,
		}, nil
	}

	content := strings.TrimSpace(response.Content)
	if content == "" {
		return agents2.ModelResponse{
			Decision: &agents2.Decision{Action: agents2.ActionComplete},
			Metadata: metadata,
		}, nil
	}
	modelResponse := agents2.ModelResponse{Content: content, Metadata: metadata}
	if _, err := modelResponse.ResolveDecision(); err == nil {
		return modelResponse, nil
	}
	modelResponse.Decision = &agents2.Decision{
		Action:      agents2.ActionComplete,
		FinalAnswer: content,
	}
	return modelResponse, nil
}

type userInputRequestExecutor struct{}

func (userInputRequestExecutor) ExecuteEffect(_ context.Context, _ reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if effect.Type != reactor2.EffectUserInputRequest {
		return reactor2.EffectResult{}, fmt.Errorf("user input executor: unsupported effect type %q", effect.Type)
	}
	return reactor2.EffectResult{}, nil
}

func llmMessages(messages []agents2.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		content := message.Content
		if strings.TrimSpace(content) == "" && len(message.Data) > 0 {
			content = string(message.Data)
		}
		role := llmRole(message.Role)
		if strings.TrimSpace(message.Role) == "tool" {
			role = llmClient.RoleUser
			content = toolObservationContent(message)
		}
		out = append(out, llmClient.Message{
			Role:    role,
			Content: content,
		})
	}
	return out
}

func llmRole(role string) llmClient.Role {
	switch strings.TrimSpace(role) {
	case string(llmClient.RoleSystem):
		return llmClient.RoleSystem
	case string(llmClient.RoleAssistant):
		return llmClient.RoleAssistant
	case string(llmClient.RoleTool):
		return llmClient.RoleTool
	default:
		return llmClient.RoleUser
	}
}

func toolObservationContent(message agents2.Message) string {
	var payload statemachine2.ToolCallPayload
	if len(message.Data) > 0 && json.Unmarshal(message.Data, &payload) == nil && payload.ToolCallID != "" {
		var b strings.Builder
		b.WriteString("Tool result")
		if payload.ToolName != "" {
			b.WriteString(" from ")
			b.WriteString(payload.ToolName)
		}
		b.WriteString(":\n")
		if payload.Error != "" {
			b.WriteString("error: ")
			b.WriteString(payload.Error)
			return b.String()
		}
		if len(payload.Result) > 0 {
			b.WriteString(string(payload.Result))
			return b.String()
		}
		b.WriteString("{}")
		return b.String()
	}
	if strings.TrimSpace(message.Content) != "" {
		return message.Content
	}
	if len(message.Data) > 0 {
		return "Tool result:\n" + string(message.Data)
	}
	return "Tool result: {}"
}

func llmTools(specs []agents2.ToolSpec) []llmClient.ToolDefinition {
	if len(specs) == 0 {
		return nil
	}
	out := make([]llmClient.ToolDefinition, 0, len(specs))
	for _, spec := range specs {
		out = append(out, llmClient.ToolDefinition{
			Name:        spec.Name,
			Description: spec.Description,
			InputSchema: append(json.RawMessage(nil), spec.InputSchema...),
		})
	}
	return out
}

func resultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var envelope struct {
		Answer      string `json:"answer"`
		FinalAnswer string `json:"final_answer"`
		Summary     string `json:"summary"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil {
		switch {
		case strings.TrimSpace(envelope.Answer) != "":
			return envelope.Answer
		case strings.TrimSpace(envelope.FinalAnswer) != "":
			return envelope.FinalAnswer
		case strings.TrimSpace(envelope.Summary) != "":
			return envelope.Summary
		}
	}
	var pretty any
	if err := json.Unmarshal(raw, &pretty); err == nil {
		formatted, err := json.MarshalIndent(pretty, "", "  ")
		if err == nil {
			return string(formatted)
		}
	}
	return string(raw)
}

func systemPrompt(workDir string) string {
	return strings.TrimSpace(fmt.Sprintf(`You are an interactive CLI coding agent.
The user is talking to one runtime task and may provide follow-up guidance.
Workspace: %s

Return exactly one JSON object matching this decision protocol:
- To answer: {"action":"complete","final_answer":"..."}
- To use a tool: {"action":"use_tool","tool":{"tool_name":"workspace.read","arguments":{"path":"go.mod"}}}
- To ask the user: {"action":"ask_user","user_input":{"prompt":"..."}}
- To fail: {"action":"fail","error":"..."}

Use tools when repository context is needed. Ask the user only when required information is missing.
Do not include markdown outside the JSON object. Do not expose hidden reasoning or chain-of-thought.`, strings.TrimSpace(workDir)))
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

func newID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("runtime cli: generate id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	id := fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	)
	if strings.TrimSpace(prefix) == "" {
		return id, nil
	}
	return prefix + "_" + id, nil
}
