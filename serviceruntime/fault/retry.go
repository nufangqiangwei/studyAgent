package fault

import "time"

type RetryInput struct {
	Error       error
	Attempt     int
	MaxAttempts int
	Now         time.Time
}

type RetryDecision struct {
	Retry   bool
	RetryAt time.Time
}

type RetryPolicy interface {
	DecideRetry(input RetryInput) RetryDecision
}

type ExponentialRetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (p ExponentialRetryPolicy) DecideRetry(input RetryInput) RetryDecision {
	kind := KindOf(input.Error)
	if kind == Deferred {
		delay := p.BaseDelay
		if delay <= 0 {
			delay = time.Second
		}
		return RetryDecision{Retry: true, RetryAt: input.Now.Add(delay)}
	}
	if kind == Validation || kind == NotFound || kind == Permanent || kind == CorruptState ||
		kind == ReconcileNeeded || input.MaxAttempts > 0 && input.Attempt >= input.MaxAttempts {
		return RetryDecision{}
	}
	if kind == Conflict || kind == StaleActivation || kind == LeaseLost {
		return RetryDecision{Retry: true, RetryAt: input.Now}
	}
	base := p.BaseDelay
	if base <= 0 {
		base = time.Second
	}
	maximum := p.MaxDelay
	if maximum <= 0 {
		maximum = time.Minute
	}
	exponent := input.Attempt - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 20 {
		exponent = 20
	}
	delay := base * time.Duration(1<<exponent)
	if delay > maximum {
		delay = maximum
	}
	return RetryDecision{Retry: true, RetryAt: input.Now.Add(delay)}
}
