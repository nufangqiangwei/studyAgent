package capability

import (
	"agent/serviceruntime/contract"
	"encoding/json"
	"strings"
	"time"
)

type CallPhase string

const (
	PhaseReceived         CallPhase = "received"
	PhaseWaitingApproval  CallPhase = "waiting_approval"
	PhaseAuthorized       CallPhase = "authorized"
	PhaseWaitingExecution CallPhase = "waiting_execution"
	PhaseSucceeded        CallPhase = "succeeded"
	PhaseFailed           CallPhase = "failed"
	PhaseDenied           CallPhase = "denied"
	PhaseExpired          CallPhase = "expired"
	PhaseCancelled        CallPhase = "cancelled"
)

func (p CallPhase) Valid() bool {
	switch p {
	case PhaseReceived, PhaseWaitingApproval, PhaseAuthorized, PhaseWaitingExecution,
		PhaseSucceeded, PhaseFailed, PhaseDenied, PhaseExpired, PhaseCancelled:
		return true
	default:
		return false
	}
}

func (p CallPhase) Terminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseDenied, PhaseExpired, PhaseCancelled:
		return true
	default:
		return false
	}
}

type ExecutionKind string

const (
	ExecutionEffect         ExecutionKind = "effect"
	ExecutionServiceCommand ExecutionKind = "service_command"
)

func (k ExecutionKind) Valid() bool { return k == ExecutionEffect || k == ExecutionServiceCommand }

type AuthorizationDecisionKind string

const (
	AuthorizationAllow AuthorizationDecisionKind = "allow"
	AuthorizationAsk   AuthorizationDecisionKind = "ask"
	AuthorizationDeny  AuthorizationDecisionKind = "deny"
)

func (d AuthorizationDecisionKind) Valid() bool {
	return d == AuthorizationAllow || d == AuthorizationAsk || d == AuthorizationDeny
}

type Arguments struct {
	Inline json.RawMessage       `json:"inline,omitempty"`
	Ref    *contract.ArtifactRef `json:"ref,omitempty"`
	Digest string                `json:"digest"`
}

func (a Arguments) Clone() Arguments {
	a.Inline = contract.CloneRaw(a.Inline)
	if a.Ref != nil {
		value := *a.Ref
		a.Ref = &value
	}
	return a
}

type CallState struct {
	CallID              string                  `json:"call_id"`
	InvocationMessageID string                  `json:"invocation_message_id"`
	Caller              contract.ServiceAddress `json:"caller"`
	ReplyTo             contract.ServiceAddress `json:"reply_to"`
	UserID              string                  `json:"user_id,omitempty"`
	CorrelationID       string                  `json:"correlation_id"`
	PlanRevision        contract.PlanRevision   `json:"plan_revision"`
	CapabilityRef       string                  `json:"capability_ref"`
	CapabilityVersion   string                  `json:"capability_version"`
	DescriptorRevision  string                  `json:"descriptor_revision"`
	Phase               CallPhase               `json:"phase"`
	Arguments           Arguments               `json:"arguments"`
	IdentityFingerprint string                  `json:"identity_fingerprint"`

	AuthorizationDecision AuthorizationDecisionKind `json:"authorization_decision,omitempty"`
	AuthorizationRule     string                    `json:"authorization_rule,omitempty"`
	AuthorizationReason   string                    `json:"authorization_reason,omitempty"`
	RiskSummary           string                    `json:"risk_summary,omitempty"`
	ApprovalID            string                    `json:"approval_id,omitempty"`
	ApprovalService       contract.ServiceAddress   `json:"approval_service,omitempty"`
	ApprovalDecision      string                    `json:"approval_decision,omitempty"`
	ApprovalDecidedBy     string                    `json:"approval_decided_by,omitempty"`
	ApprovalDecidedAt     *time.Time                `json:"approval_decided_at,omitempty"`

	ExecutionKind           ExecutionKind           `json:"execution_kind,omitempty"`
	ExecutionKey            string                  `json:"execution_key,omitempty"`
	ExecutionGeneration     uint64                  `json:"execution_generation,omitempty"`
	ExecutorRef             string                  `json:"executor_ref,omitempty"`
	EffectType              contract.EffectType     `json:"effect_type,omitempty"`
	ExecutionTarget         contract.ServiceAddress `json:"execution_target,omitempty"`
	ExecutionMessageType    contract.MessageType    `json:"execution_message_type,omitempty"`
	ExecutionMessageVersion int                     `json:"execution_message_version,omitempty"`
	ExecutionReplyType      contract.MessageType    `json:"execution_reply_type,omitempty"`
	ExecutionReplyVersion   int                     `json:"execution_reply_version,omitempty"`
	ExecutionCorrelationID  string                  `json:"execution_correlation_id,omitempty"`

	ResultRef       *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result          json.RawMessage       `json:"result,omitempty"`
	ErrorCode       string                `json:"error_code,omitempty"`
	ErrorMessage    string                `json:"error_message,omitempty"`
	Deadline        *time.Time            `json:"deadline,omitempty"`
	ReceivedAt      time.Time             `json:"received_at"`
	CompletedAt     *time.Time            `json:"completed_at,omitempty"`
	LateOutcomeID   string                `json:"late_outcome_id,omitempty"`
	LateOutcomeKind string                `json:"late_outcome_kind,omitempty"`
	LateResultRef   *contract.ArtifactRef `json:"late_result_ref,omitempty"`
	LateResult      json.RawMessage       `json:"late_result,omitempty"`
	LateErrorCode   string                `json:"late_error_code,omitempty"`
}

func (c CallState) Clone() CallState {
	c.Arguments = c.Arguments.Clone()
	c.Result = contract.CloneRaw(c.Result)
	if c.ResultRef != nil {
		value := *c.ResultRef
		c.ResultRef = &value
	}
	c.Deadline = cloneTime(c.Deadline)
	c.CompletedAt = cloneTime(c.CompletedAt)
	c.ApprovalDecidedAt = cloneTime(c.ApprovalDecidedAt)
	c.LateResult = contract.CloneRaw(c.LateResult)
	if c.LateResultRef != nil {
		value := *c.LateResultRef
		c.LateResultRef = &value
	}
	return c
}

type CallTombstone struct {
	CallID              string                  `json:"call_id"`
	Caller              contract.ServiceAddress `json:"caller"`
	ReplyTo             contract.ServiceAddress `json:"reply_to"`
	CapabilityRef       string                  `json:"capability_ref"`
	CapabilityVersion   string                  `json:"capability_version"`
	IdentityFingerprint string                  `json:"identity_fingerprint"`
	Phase               CallPhase               `json:"phase"`
	ResultRef           *contract.ArtifactRef   `json:"result_ref,omitempty"`
	Result              json.RawMessage         `json:"result,omitempty"`
	ErrorCode           string                  `json:"error_code,omitempty"`
	ErrorMessage        string                  `json:"error_message,omitempty"`
	CompletedAt         time.Time               `json:"completed_at"`
	ExpiresAt           time.Time               `json:"expires_at"`
}

func (t CallTombstone) Clone() CallTombstone {
	t.Result = contract.CloneRaw(t.Result)
	if t.ResultRef != nil {
		value := *t.ResultRef
		t.ResultRef = &value
	}
	return t
}

type InvokeRequest struct {
	CallID             string                `json:"call_id"`
	CapabilityRef      string                `json:"capability_ref"`
	CapabilityVersion  string                `json:"capability_version"`
	DescriptorRevision string                `json:"descriptor_revision,omitempty"`
	Arguments          json.RawMessage       `json:"arguments,omitempty"`
	ArgumentsRef       *contract.ArtifactRef `json:"arguments_ref,omitempty"`
}

type CancelRequest struct {
	CallID     string `json:"call_id"`
	ReasonCode string `json:"reason_code,omitempty"`
}

type GetRequest struct {
	CallID string `json:"call_id"`
}

type ListRequest struct{}

type PruneRequest struct {
	Before time.Time `json:"before"`
}

type Result struct {
	CallID            string                `json:"call_id"`
	CapabilityRef     string                `json:"capability_ref"`
	CapabilityVersion string                `json:"capability_version"`
	Phase             CallPhase             `json:"phase"`
	ResultRef         *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result            json.RawMessage       `json:"result,omitempty"`
	ErrorCode         string                `json:"error_code,omitempty"`
	ErrorMessage      string                `json:"error_message,omitempty"`
}

func (r Result) Clone() Result {
	r.Result = contract.CloneRaw(r.Result)
	if r.ResultRef != nil {
		value := *r.ResultRef
		r.ResultRef = &value
	}
	return r
}

type GetResponse struct {
	Call      *CallState     `json:"call,omitempty"`
	Tombstone *CallTombstone `json:"tombstone,omitempty"`
}

type ListResponse struct {
	Descriptors []CapabilityDescriptor `json:"descriptors"`
}

type ExecutionCompleted struct {
	CallID       string                `json:"call_id"`
	ExecutionKey string                `json:"execution_key"`
	ExecutorRef  string                `json:"executor_ref"`
	Generation   uint64                `json:"generation"`
	OutcomeID    string                `json:"outcome_id"`
	ResultRef    *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result       json.RawMessage       `json:"result,omitempty"`
}

type ExecutionFailed struct {
	CallID       string `json:"call_id"`
	ExecutionKey string `json:"execution_key"`
	ExecutorRef  string `json:"executor_ref"`
	Generation   uint64 `json:"generation"`
	OutcomeID    string `json:"outcome_id"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

type aggregateState struct {
	Calls      map[string]CallState     `json:"calls"`
	Tombstones map[string]CallTombstone `json:"tombstones"`
}

func clean(value string) string { return strings.TrimSpace(value) }

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
