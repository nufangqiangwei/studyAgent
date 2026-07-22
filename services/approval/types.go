package approval

import (
	"agent/serviceruntime/contract"
	"encoding/json"
	"strings"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusApproved  Status = "approved"
	StatusDenied    Status = "denied"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusApproved, StatusDenied, StatusCancelled, StatusExpired:
		return true
	default:
		return false
	}
}

func (s Status) Terminal() bool { return s != StatusPending && s.Valid() }

type Decision string

const (
	DecisionApprove Decision = "approve"
	DecisionDeny    Decision = "deny"
)

func (d Decision) Valid() bool { return d == DecisionApprove || d == DecisionDeny }

// State is the ApprovalService-owned durable business record. It contains
// only the audit facts required for this approval, never the capability's
// complete arguments or authorization rule implementation.
type State struct {
	ApprovalID        string                  `json:"approval_id"`
	RequestMessageID  string                  `json:"request_message_id"`
	Requester         contract.ServiceAddress `json:"requester"`
	NotifyTo          contract.ServiceAddress `json:"notify_to"`
	CallID            string                  `json:"call_id"`
	UserID            string                  `json:"user_id"`
	CapabilityRef     string                  `json:"capability_ref"`
	CapabilityVersion string                  `json:"capability_version"`
	Status            Status                  `json:"status"`
	RiskSummary       string                  `json:"risk_summary"`
	ArgumentsDigest   string                  `json:"arguments_digest"`
	ArgumentsRef      *contract.ArtifactRef   `json:"arguments_ref,omitempty"`
	RequestedAt       time.Time               `json:"requested_at"`
	ExpiresAt         *time.Time              `json:"expires_at,omitempty"`
	DecidedAt         *time.Time              `json:"decided_at,omitempty"`
	DecidedBy         string                  `json:"decided_by,omitempty"`
	Decision          Decision                `json:"decision,omitempty"`
	ReasonCode        string                  `json:"reason_code,omitempty"`
}

func (s State) Clone() State {
	if s.ArgumentsRef != nil {
		value := *s.ArgumentsRef
		s.ArgumentsRef = &value
	}
	s.ExpiresAt = cloneTime(s.ExpiresAt)
	s.DecidedAt = cloneTime(s.DecidedAt)
	return s
}

type Request struct {
	ApprovalID        string                `json:"approval_id"`
	CallID            string                `json:"call_id"`
	UserID            string                `json:"user_id"`
	CapabilityRef     string                `json:"capability_ref"`
	CapabilityVersion string                `json:"capability_version"`
	RiskSummary       string                `json:"risk_summary"`
	ArgumentsDigest   string                `json:"arguments_digest"`
	ArgumentsRef      *contract.ArtifactRef `json:"arguments_ref,omitempty"`
	RequestedAt       time.Time             `json:"requested_at"`
	ExpiresAt         *time.Time            `json:"expires_at,omitempty"`
}

func (r Request) clone() Request {
	if r.ArgumentsRef != nil {
		value := *r.ArgumentsRef
		r.ArgumentsRef = &value
	}
	r.ExpiresAt = cloneTime(r.ExpiresAt)
	return r
}

type ResolveRequest struct {
	ApprovalID string   `json:"approval_id"`
	CallID     string   `json:"call_id"`
	Decision   Decision `json:"decision"`
	ReasonCode string   `json:"reason_code,omitempty"`
}

type CancelRequest struct {
	ApprovalID string `json:"approval_id"`
	CallID     string `json:"call_id"`
	ReasonCode string `json:"reason_code,omitempty"`
}

type ExpireRequest struct {
	ApprovalID string `json:"approval_id"`
	CallID     string `json:"call_id"`
}

type GetRequest struct {
	ApprovalID string `json:"approval_id"`
}

type ListPendingRequest struct {
	UserID string `json:"user_id,omitempty"`
}

type Response struct {
	Approval  *State  `json:"approval,omitempty"`
	Approvals []State `json:"approvals,omitempty"`
	Accepted  bool    `json:"accepted"`
}

type Requested struct {
	ApprovalID        string                `json:"approval_id"`
	CallID            string                `json:"call_id"`
	UserID            string                `json:"user_id"`
	CapabilityRef     string                `json:"capability_ref"`
	CapabilityVersion string                `json:"capability_version"`
	RiskSummary       string                `json:"risk_summary"`
	ArgumentsDigest   string                `json:"arguments_digest"`
	ArgumentsRef      *contract.ArtifactRef `json:"arguments_ref,omitempty"`
	RequestedAt       time.Time             `json:"requested_at"`
	ExpiresAt         *time.Time            `json:"expires_at,omitempty"`
}

type Result struct {
	ApprovalID        string                  `json:"approval_id"`
	CallID            string                  `json:"call_id"`
	Requester         contract.ServiceAddress `json:"requester"`
	CapabilityRef     string                  `json:"capability_ref"`
	CapabilityVersion string                  `json:"capability_version"`
	Status            Status                  `json:"status"`
	Decision          Decision                `json:"decision,omitempty"`
	DecidedAt         time.Time               `json:"decided_at"`
	DecidedBy         string                  `json:"decided_by,omitempty"`
	ReasonCode        string                  `json:"reason_code,omitempty"`
}

type aggregateState struct {
	Approvals map[string]State `json:"approvals"`
}

type transitionEvent struct {
	ApprovalID string    `json:"approval_id"`
	Status     Status    `json:"status"`
	Decision   Decision  `json:"decision,omitempty"`
	DecidedAt  time.Time `json:"decided_at"`
	DecidedBy  string    `json:"decided_by,omitempty"`
	ReasonCode string    `json:"reason_code,omitempty"`
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneRaw(value json.RawMessage) json.RawMessage { return contract.CloneRaw(value) }

func clean(value string) string { return strings.TrimSpace(value) }
