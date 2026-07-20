package fault

import (
	"errors"
	"testing"
	"time"
)

func TestExponentialRetryPolicyUsesErrorKind(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	policy := ExponentialRetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	if decision := policy.DecideRetry(RetryInput{Error: Wrap(Permanent, "handle", errors.New("invalid")), Attempt: 1, MaxAttempts: 8, Now: now}); decision.Retry {
		t.Fatal("permanent error must not retry")
	}
	decision := policy.DecideRetry(RetryInput{Error: Wrap(Conflict, "commit", errors.New("conflict")), Attempt: 1, MaxAttempts: 8, Now: now})
	if !decision.Retry || !decision.RetryAt.Equal(now) {
		t.Fatalf("conflict retry=%#v, want immediate retry", decision)
	}
}

func TestExponentialRetryPolicyAlwaysRetriesDeferredWork(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	policy := ExponentialRetryPolicy{BaseDelay: 250 * time.Millisecond}
	decision := policy.DecideRetry(RetryInput{
		Error:       Wrap(Deferred, "workflow_waiting", errors.New("waiting for reply")),
		Attempt:     100,
		MaxAttempts: 8,
		Now:         now,
	})
	if !decision.Retry || !decision.RetryAt.Equal(now.Add(250*time.Millisecond)) {
		t.Fatalf("deferred retry=%#v, want retry after base delay", decision)
	}
}
