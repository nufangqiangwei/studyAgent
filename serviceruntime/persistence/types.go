package persistence

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"context"
	"encoding/json"
	"time"
)

type JournalStore interface {
	LoadStream(ctx context.Context, streamID contract.StreamID, afterSequence uint64, limit int) ([]contract.StoredEvent, error)
	Head(ctx context.Context, streamID contract.StreamID) (uint64, error)
}

type SnapshotStore interface {
	LoadLatest(ctx context.Context, streamID contract.StreamID) (contract.Snapshot, bool, error)
}

type InboxStatus string

const (
	InboxPending    InboxStatus = "pending"
	InboxClaimed    InboxStatus = "claimed"
	InboxAcked      InboxStatus = "acked"
	InboxRetry      InboxStatus = "retry"
	InboxDeadLetter InboxStatus = "dead_letter"
)

type InboxRecord struct {
	InboxID    string                     `json:"inbox_id"`
	MailboxID  contract.MailboxID         `json:"mailbox_id"`
	InstanceID contract.ServiceInstanceID `json:"instance_id"`
	Message    contract.Message           `json:"message"`
	Status     InboxStatus                `json:"status"`

	Attempt     int        `json:"attempt"`
	AvailableAt time.Time  `json:"available_at"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseToken  string     `json:"lease_token,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`

	ReceivedAt time.Time  `json:"received_at"`
	AckedAt    *time.Time `json:"acked_at,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

func (r InboxRecord) Clone() InboxRecord {
	r.Message = r.Message.Clone()
	r.LeaseUntil = cloneTime(r.LeaseUntil)
	r.AckedAt = cloneTime(r.AckedAt)
	return r
}

type InboxClaim struct {
	Record     InboxRecord `json:"record"`
	LeaseToken string      `json:"lease_token"`
}

type InboxStore interface {
	Enqueue(ctx context.Context, target instance.DeliveryTarget, message contract.Message) (InboxRecord, bool, error)
	ClaimNext(ctx context.Context, mailboxID contract.MailboxID, ownerID string, lease time.Duration) (InboxClaim, bool, error)
	RenewClaim(ctx context.Context, claim InboxClaim, lease time.Duration) error
	ReleaseClaim(ctx context.Context, claim InboxClaim, retryAt time.Time, cause error) error
	MoveToDeadLetter(ctx context.Context, claim InboxClaim, cause error) error
	CountPending(ctx context.Context, mailboxID contract.MailboxID) (int, error)
}

type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "pending"
	OutboxClaimed   OutboxStatus = "claimed"
	OutboxDelivered OutboxStatus = "delivered"
	OutboxRetry     OutboxStatus = "retry"
	OutboxDead      OutboxStatus = "dead_letter"
)

type OutboxRecord struct {
	OutboxID   string                     `json:"outbox_id"`
	InstanceID contract.ServiceInstanceID `json:"instance_id"`
	Message    contract.Message           `json:"message"`
	Status     OutboxStatus               `json:"status"`

	Attempt     int        `json:"attempt"`
	AvailableAt time.Time  `json:"available_at"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseToken  string     `json:"lease_token,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

func (r OutboxRecord) Clone() OutboxRecord {
	r.Message = r.Message.Clone()
	r.LeaseUntil = cloneTime(r.LeaseUntil)
	r.DeliveredAt = cloneTime(r.DeliveredAt)
	return r
}

type OutboxClaim struct {
	Record     OutboxRecord `json:"record"`
	LeaseToken string       `json:"lease_token"`
}

type DeliverySummary struct {
	MessageID string `json:"message_id"`
	Delivered int    `json:"delivered"`
	Duplicate int    `json:"duplicate"`
	Failed    int    `json:"failed"`
}

type OutboxStore interface {
	ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (OutboxClaim, bool, error)
	RenewClaim(ctx context.Context, claim OutboxClaim, lease time.Duration) error
	MarkDelivered(ctx context.Context, claim OutboxClaim, result DeliverySummary) error
	MarkRetry(ctx context.Context, claim OutboxClaim, retryAt time.Time, cause error) error
	MoveToDeadLetter(ctx context.Context, claim OutboxClaim, cause error) error
	CountPending(ctx context.Context, runtimeID contract.RuntimeID) (int, error)
}

type EffectStatus string

const (
	EffectPlanned                EffectStatus = "planned"
	EffectStarted                EffectStatus = "started"
	EffectSucceeded              EffectStatus = "succeeded"
	EffectFailed                 EffectStatus = "failed"
	EffectTerminalFailed         EffectStatus = "terminal_failed"
	EffectReconciliationRequired EffectStatus = "reconciliation_required"
)

type EffectRecord struct {
	EffectID        string                     `json:"effect_id"`
	RuntimeID       contract.RuntimeID         `json:"runtime_id"`
	PlanRevision    contract.PlanRevision      `json:"plan_revision"`
	InstanceID      contract.ServiceInstanceID `json:"instance_id"`
	SourceMessageID string                     `json:"source_message_id"`
	Type            contract.EffectType        `json:"type"`
	Version         int                        `json:"version"`
	ExecutorRef     string                     `json:"executor_ref"`
	IdempotencyKey  string                     `json:"idempotency_key"`

	Status         EffectStatus      `json:"status"`
	Attempt        int               `json:"attempt"`
	AvailableAt    time.Time         `json:"available_at"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Result         json.RawMessage   `json:"result,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	ResultMetadata map[string]string `json:"result_metadata,omitempty"`
	Deadline       *time.Time        `json:"deadline,omitempty"`
	LastError      string            `json:"last_error,omitempty"`

	PlannedAt   time.Time  `json:"planned_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseToken  string     `json:"lease_token,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`
}

func (r EffectRecord) Clone() EffectRecord {
	r.Payload = contract.CloneRaw(r.Payload)
	r.Result = contract.CloneRaw(r.Result)
	r.Metadata = contract.CloneStrings(r.Metadata)
	r.ResultMetadata = contract.CloneStrings(r.ResultMetadata)
	r.Deadline = cloneTime(r.Deadline)
	r.StartedAt = cloneTime(r.StartedAt)
	r.CompletedAt = cloneTime(r.CompletedAt)
	r.LeaseUntil = cloneTime(r.LeaseUntil)
	return r
}

type EffectClaim struct {
	Record     EffectRecord `json:"record"`
	LeaseToken string       `json:"lease_token"`
}

type EffectResult struct {
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type EffectStore interface {
	ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (EffectClaim, bool, error)
	Claim(ctx context.Context, effectID string, ownerID string, lease time.Duration) (EffectClaim, error)
	RenewClaim(ctx context.Context, claim EffectClaim, lease time.Duration) error
	MarkStarted(ctx context.Context, claim EffectClaim) error
	MarkSucceeded(ctx context.Context, claim EffectClaim, result EffectResult) error
	MarkFailed(ctx context.Context, claim EffectClaim, cause error, retryAt *time.Time) error
	MarkTerminalFailed(ctx context.Context, claim EffectClaim, cause error) error
	RequireReconciliation(ctx context.Context, claim EffectClaim, cause error) error
	ListUnfinished(ctx context.Context, runtimeID contract.RuntimeID) ([]EffectRecord, error)
}

type InboxAck struct {
	InboxID    string    `json:"inbox_id"`
	MessageID  string    `json:"message_id"`
	LeaseToken string    `json:"lease_token"`
	AckedAt    time.Time `json:"acked_at"`
}

type MessageCommit struct {
	RuntimeID        contract.RuntimeID         `json:"runtime_id"`
	PlanRevision     contract.PlanRevision      `json:"plan_revision"`
	InstanceID       contract.ServiceInstanceID `json:"instance_id"`
	ActivationEpoch  uint64                     `json:"activation_epoch"`
	Ack              InboxAck                   `json:"ack"`
	StreamID         contract.StreamID          `json:"stream_id"`
	ExpectedSequence uint64                     `json:"expected_sequence"`
	Events           []contract.StoredEvent     `json:"events,omitempty"`
	Snapshot         *contract.Snapshot         `json:"snapshot,omitempty"`
	Outbox           []OutboxRecord             `json:"outbox,omitempty"`
	Effects          []EffectRecord             `json:"effects,omitempty"`
}

type CommitResult struct {
	LastSequence    uint64   `json:"last_sequence"`
	Duplicate       bool     `json:"duplicate"`
	StoredEventIDs  []string `json:"stored_event_ids,omitempty"`
	StoredOutboxIDs []string `json:"stored_outbox_ids,omitempty"`
	StoredEffectIDs []string `json:"stored_effect_ids,omitempty"`
}

type MessageCommitStore interface {
	CommitMessage(ctx context.Context, commit MessageCommit) (CommitResult, error)
}

type PlanRecord struct {
	RuntimeID    contract.RuntimeID    `json:"runtime_id"`
	PlanRevision contract.PlanRevision `json:"plan_revision"`
	PlanHash     string                `json:"plan_hash"`
	Manifest     json.RawMessage       `json:"manifest"`
	CreatedAt    time.Time             `json:"created_at"`
}

func (r PlanRecord) Clone() PlanRecord {
	r.Manifest = contract.CloneRaw(r.Manifest)
	return r
}

type PlanStore interface {
	Put(ctx context.Context, record PlanRecord) (bool, error)
	Get(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision) (PlanRecord, bool, error)
	List(ctx context.Context, runtimeID contract.RuntimeID) ([]PlanRecord, error)
}

type MessageSequenceStore interface {
	Assign(ctx context.Context, scope string, message contract.Message) (contract.Message, error)
}

type RuntimeStorage interface {
	Journal() JournalStore
	Snapshots() SnapshotStore
	Inbox() InboxStore
	Outbox() OutboxStore
	Effects() EffectStore
	Instances() instance.Store
	Leases() instance.ActivationLeaseStore
	Committer() MessageCommitStore
	Plans() PlanStore
	Sequences() MessageSequenceStore
	Close() error
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
