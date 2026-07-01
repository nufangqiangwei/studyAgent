package state

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrLeaseOwnerRequired    = errors.New("lease owner is required")
	ErrLeaseDurationRequired = errors.New("lease duration must be positive")
	ErrLeaseOwnerMismatch    = errors.New("lease owner mismatch")
	ErrLeaseExpired          = errors.New("lease expired")
	ErrTaskNotClaimed        = errors.New("task is not claimed")
)

func normalizeLeaseOwner(owner string) (string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", ErrLeaseOwnerRequired
	}
	return owner, nil
}

func normalizeLease(owner string, leaseDuration time.Duration) (string, error) {
	owner, err := normalizeLeaseOwner(owner)
	if err != nil {
		return "", err
	}
	if leaseDuration <= 0 {
		return "", ErrLeaseDurationRequired
	}
	return owner, nil
}

func leaseDeadline(now time.Time, leaseDuration time.Duration) time.Time {
	return now.Add(leaseDuration)
}

func leaseActive(deadline *time.Time, now time.Time) bool {
	return deadline != nil && now.Before(*deadline)
}

func validateLeaseOwner(currentOwner string, deadline *time.Time, owner string, now time.Time) error {
	owner, err := normalizeLeaseOwner(owner)
	if err != nil {
		return err
	}
	if err := validateTaskOwner(currentOwner, owner); err != nil {
		return err
	}
	if !leaseActive(deadline, now) {
		return fmt.Errorf("%w: owner %q", ErrLeaseExpired, owner)
	}
	return nil
}

func validateTaskOwner(currentOwner string, owner string) error {
	owner, err := normalizeLeaseOwner(owner)
	if err != nil {
		return err
	}
	if currentOwner == "" {
		return ErrTaskNotClaimed
	}
	if currentOwner != owner {
		return fmt.Errorf("%w: current owner %q, requested owner %q", ErrLeaseOwnerMismatch, currentOwner, owner)
	}
	return nil
}

func currentStoreTime(now func() time.Time) time.Time {
	if now != nil {
		return now().UTC()
	}
	return time.Now().UTC()
}
