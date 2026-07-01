package state

import (
	"context"
	"fmt"
	"time"
)

type EffectStatus string

const (
	EffectStatusPending    EffectStatus = "pending"
	EffectStatusDispatched EffectStatus = "dispatched"
	EffectStatusCompleted  EffectStatus = "completed"
	EffectStatusFailed     EffectStatus = "failed"
)

type StoredEffect struct {
	Effect        Effect       `json:"effect"`
	Status        EffectStatus `json:"status"`
	Owner         string       `json:"owner,omitempty"`
	LeaseDeadline *time.Time   `json:"lease_deadline,omitempty"`
	ClaimCount    int          `json:"claim_count,omitempty"`
	Error         string       `json:"error,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
	DispatchedAt  *time.Time   `json:"dispatched_at,omitempty"`
	CompletedAt   *time.Time   `json:"completed_at,omitempty"`
	FailedAt      *time.Time   `json:"failed_at,omitempty"`
}

type EffectStore interface {
	Append(ctx context.Context, effect Effect) (StoredEffect, error)
	ListPending(ctx context.Context, runID string) ([]StoredEffect, error)
	Claim(ctx context.Context, runID string, owner string, leaseDuration time.Duration) (StoredEffect, bool, error)
	MarkDispatched(ctx context.Context, effectID string) error
	MarkCompleted(ctx context.Context, effectID string, owner string) error
	MarkFailed(ctx context.Context, effectID string, owner string, cause error) error
	RenewLease(ctx context.Context, effectID string, owner string, leaseDuration time.Duration) (StoredEffect, error)
}

func (e StoredEffect) Clone() StoredEffect {
	cloned := e
	cloned.Effect = e.Effect.Clone()
	cloned.LeaseDeadline = cloneTimePtr(e.LeaseDeadline)
	cloned.DispatchedAt = cloneTimePtr(e.DispatchedAt)
	cloned.CompletedAt = cloneTimePtr(e.CompletedAt)
	cloned.FailedAt = cloneTimePtr(e.FailedAt)
	return cloned
}

func normalizeStoredEffect(effect Effect, now time.Time) (StoredEffect, error) {
	if effect.ID == "" {
		return StoredEffect{}, fmt.Errorf("effect id is required")
	}
	if effect.RunID == "" {
		return StoredEffect{}, fmt.Errorf("effect run_id is required")
	}
	if effect.Type == "" {
		return StoredEffect{}, fmt.Errorf("effect type is required")
	}
	return StoredEffect{
		Effect:    effect.Clone(),
		Status:    EffectStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	cloned := *t
	return &cloned
}
