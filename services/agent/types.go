package agent

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/services/capability"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	defaultMaxTurns                       = 32
	defaultMaxArtifactBytes         int64 = 16 << 20
	defaultMaxPromptBytes           int64 = 32 << 20
	defaultMaxInlineCapabilityBytes       = 32 << 10
	maxInlineAgentSpecBytes               = 24 << 10
	maxInlineAgentInputBytes              = 16 << 10
	maxInlineAgentEffectBytes             = 48 << 10
)

type CapabilityPrompt struct {
	Ref             string          `json:"ref"`
	Version         string          `json:"version"`
	Description     string          `json:"description,omitempty"`
	ArgumentsSchema json.RawMessage `json:"arguments_schema,omitempty"`
}

func (p CapabilityPrompt) clone() CapabilityPrompt {
	p.ArgumentsSchema = contract.CloneRaw(p.ArgumentsSchema)
	return p
}

type AgentSpec struct {
	Ref          string `json:"ref"`
	Version      string `json:"version"`
	SystemPrompt string `json:"system_prompt"`

	Capabilities []CapabilityPrompt `json:"capabilities,omitempty"`
	MaxTurns     int                `json:"max_turns,omitempty"`
	Temperature  *float64           `json:"temperature,omitempty"`
	MaxTokens    int                `json:"max_tokens,omitempty"`

	MaxArtifactBytes               int64 `json:"max_artifact_bytes,omitempty"`
	MaxPromptBytes                 int64 `json:"max_prompt_bytes,omitempty"`
	MaxInlineCapabilityResultBytes int   `json:"max_inline_capability_result_bytes,omitempty"`
}

func (s AgentSpec) withDefaults() AgentSpec {
	s.Ref, s.Version, s.SystemPrompt = strings.TrimSpace(s.Ref), strings.TrimSpace(s.Version), strings.TrimSpace(s.SystemPrompt)
	s.Capabilities = append([]CapabilityPrompt(nil), s.Capabilities...)
	for index := range s.Capabilities {
		s.Capabilities[index] = s.Capabilities[index].clone()
		s.Capabilities[index].Ref = strings.TrimSpace(s.Capabilities[index].Ref)
		s.Capabilities[index].Version = strings.TrimSpace(s.Capabilities[index].Version)
		s.Capabilities[index].Description = strings.TrimSpace(s.Capabilities[index].Description)
	}
	if s.MaxTurns <= 0 {
		s.MaxTurns = defaultMaxTurns
	}
	if s.MaxArtifactBytes <= 0 {
		s.MaxArtifactBytes = defaultMaxArtifactBytes
	}
	if s.MaxPromptBytes <= 0 {
		s.MaxPromptBytes = defaultMaxPromptBytes
	}
	if s.MaxInlineCapabilityResultBytes <= 0 {
		s.MaxInlineCapabilityResultBytes = defaultMaxInlineCapabilityBytes
	}
	if s.Temperature != nil {
		value := *s.Temperature
		s.Temperature = &value
	}
	return s
}

func (s AgentSpec) validate() error {
	if s.Ref == "" || s.Version == "" || s.SystemPrompt == "" {
		return fmt.Errorf("agent ref, version, and system prompt are required")
	}
	if s.MaxTurns <= 0 || s.MaxArtifactBytes <= 0 || s.MaxPromptBytes <= 0 || s.MaxInlineCapabilityResultBytes <= 0 {
		return fmt.Errorf("agent limits must be positive")
	}
	if s.MaxInlineCapabilityResultBytes > defaultMaxInlineCapabilityBytes {
		return fmt.Errorf("agent inline capability result limit cannot exceed %d bytes", defaultMaxInlineCapabilityBytes)
	}
	if s.MaxTokens < 0 {
		return fmt.Errorf("agent max tokens cannot be negative")
	}
	if s.Temperature != nil && (*s.Temperature < 0 || *s.Temperature > 2) {
		return fmt.Errorf("agent temperature must be between 0 and 2")
	}
	seen := make(map[string]struct{}, len(s.Capabilities))
	for _, prompt := range s.Capabilities {
		if prompt.Ref == "" || prompt.Version == "" {
			return fmt.Errorf("agent capability ref and version are required")
		}
		if len(prompt.ArgumentsSchema) > 0 && !json.Valid(prompt.ArgumentsSchema) {
			return fmt.Errorf("agent capability %q arguments schema is invalid JSON", prompt.Ref+"@"+prompt.Version)
		}
		key := prompt.Ref + "@" + prompt.Version
		if _, exists := seen[key]; exists {
			return fmt.Errorf("agent capability %q is duplicated", key)
		}
		seen[key] = struct{}{}
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode agent spec: %w", err)
	}
	if len(encoded) > maxInlineAgentSpecBytes {
		return fmt.Errorf("agent spec exceeds %d inline bytes", maxInlineAgentSpecBytes)
	}
	return nil
}

type ExecuteRequest struct {
	RunID         string                `json:"run_id,omitempty"`
	Input         string                `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`
}

type CancelRequest struct {
	RunID      string `json:"run_id"`
	ReasonCode string `json:"reason_code,omitempty"`
}

type GetRequest struct {
	RunID string `json:"run_id"`
}

type RunPhase string

const (
	PhaseDiscoveringCapabilities RunPhase = "discovering_capabilities"
	PhasePreparingPrompt         RunPhase = "preparing_prompt"
	PhaseWaitingModel            RunPhase = "waiting_model"
	PhaseWaitingCapability       RunPhase = "waiting_capability"
	PhaseFinalizing              RunPhase = "finalizing"
	PhaseCompleted               RunPhase = "completed"
	PhaseFailed                  RunPhase = "failed"
	PhaseCancelled               RunPhase = "cancelled"
)

func (p RunPhase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseCancelled
}

type ResolvedCapability struct {
	Descriptor      capability.CapabilityDescriptor `json:"descriptor"`
	Description     string                          `json:"description,omitempty"`
	ArgumentsSchema json.RawMessage                 `json:"arguments_schema,omitempty"`
}

func (r ResolvedCapability) clone() ResolvedCapability {
	r.Descriptor = r.Descriptor.Clone()
	r.ArgumentsSchema = contract.CloneRaw(r.ArgumentsSchema)
	return r
}

type ModelAction struct {
	Action            string          `json:"action"`
	Answer            string          `json:"answer,omitempty"`
	CapabilityRef     string          `json:"capability_ref,omitempty"`
	CapabilityVersion string          `json:"capability_version,omitempty"`
	Arguments         json.RawMessage `json:"arguments,omitempty"`
}

func (a ModelAction) clone() ModelAction {
	a.Arguments = contract.CloneRaw(a.Arguments)
	return a
}

type CapabilityOutcome struct {
	CallID       string                `json:"call_id"`
	Phase        capability.CallPhase  `json:"phase"`
	ResultRef    *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result       json.RawMessage       `json:"result,omitempty"`
	ErrorCode    string                `json:"error_code,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
}

func (o CapabilityOutcome) clone() CapabilityOutcome {
	o.Result = contract.CloneRaw(o.Result)
	if o.ResultRef != nil {
		value := *o.ResultRef
		o.ResultRef = &value
	}
	return o
}

type TurnRecord struct {
	Number           int                   `json:"number"`
	PromptRef        *contract.ArtifactRef `json:"prompt_ref,omitempty"`
	ModelRequestID   string                `json:"model_request_id,omitempty"`
	ModelResponseRef *contract.ArtifactRef `json:"model_response_ref,omitempty"`
	Action           *ModelAction          `json:"action,omitempty"`
	Capability       *CapabilityOutcome    `json:"capability,omitempty"`
	Feedback         string                `json:"feedback,omitempty"`
}

func (t TurnRecord) clone() TurnRecord {
	if t.PromptRef != nil {
		value := *t.PromptRef
		t.PromptRef = &value
	}
	if t.ModelResponseRef != nil {
		value := *t.ModelResponseRef
		t.ModelResponseRef = &value
	}
	if t.Action != nil {
		value := t.Action.clone()
		t.Action = &value
	}
	if t.Capability != nil {
		value := t.Capability.clone()
		t.Capability = &value
	}
	return t
}

type RunState struct {
	RunID               string                  `json:"run_id"`
	Phase               RunPhase                `json:"phase"`
	Caller              contract.ServiceAddress `json:"caller,omitempty"`
	ReplyTo             contract.ServiceAddress `json:"reply_to"`
	UserID              string                  `json:"user_id,omitempty"`
	GoalID              string                  `json:"goal_id,omitempty"`
	CorrelationID       string                  `json:"correlation_id"`
	IdentityFingerprint string                  `json:"identity_fingerprint"`
	Input               string                  `json:"input,omitempty"`
	InputArtifact       *contract.ArtifactRef   `json:"input_artifact,omitempty"`
	Capabilities        []ResolvedCapability    `json:"capabilities,omitempty"`
	Turns               []TurnRecord            `json:"turns,omitempty"`
	PendingCorrelation  string                  `json:"pending_correlation,omitempty"`
	PendingCallID       string                  `json:"pending_call_id,omitempty"`
	PendingTurn         int                     `json:"pending_turn,omitempty"`
	Output              *contract.ArtifactRef   `json:"output,omitempty"`
	ErrorCode           string                  `json:"error_code,omitempty"`
	ErrorMessage        string                  `json:"error_message,omitempty"`
	Deadline            *time.Time              `json:"deadline,omitempty"`
	StartedAt           time.Time               `json:"started_at"`
	CompletedAt         *time.Time              `json:"completed_at,omitempty"`
}

func (r RunState) clone() RunState {
	if r.InputArtifact != nil {
		value := *r.InputArtifact
		r.InputArtifact = &value
	}
	r.Capabilities = append([]ResolvedCapability(nil), r.Capabilities...)
	for index := range r.Capabilities {
		r.Capabilities[index] = r.Capabilities[index].clone()
	}
	r.Turns = append([]TurnRecord(nil), r.Turns...)
	for index := range r.Turns {
		r.Turns[index] = r.Turns[index].clone()
	}
	if r.Output != nil {
		value := *r.Output
		r.Output = &value
	}
	if r.Deadline != nil {
		value := *r.Deadline
		r.Deadline = &value
	}
	if r.CompletedAt != nil {
		value := *r.CompletedAt
		r.CompletedAt = &value
	}
	return r
}

type ExecuteResult struct {
	RunID        string                `json:"run_id"`
	Phase        RunPhase              `json:"phase"`
	Output       *contract.ArtifactRef `json:"output,omitempty"`
	ErrorCode    string                `json:"error_code,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
	Turns        int                   `json:"turns"`
}

type GetResponse struct {
	Run *RunState `json:"run,omitempty"`
}

type aggregateState struct {
	Runs map[string]RunState `json:"runs"`
}

func initialAggregateState() aggregateState { return aggregateState{Runs: make(map[string]RunState)} }

func validateInputArtifact(ref *contract.ArtifactRef) error {
	if ref == nil {
		return nil
	}
	return artifact.ValidateRef(*ref)
}
