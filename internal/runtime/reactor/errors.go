package reactor

import (
	"fmt"
	"strings"
)

type ExecutionError struct {
	EffectID   string
	EffectType EffectType
	Err        error
}

type ExecutionErrors []ExecutionError

func (e ExecutionErrors) Error() string {
	parts := make([]string, 0, len(e))
	for _, failure := range e {
		if failure.Err == nil {
			continue
		}
		label := string(failure.EffectType)
		if failure.EffectID != "" {
			label = fmt.Sprintf("%s/%s", label, failure.EffectID)
		}
		parts = append(parts, fmt.Sprintf("%s: %v", label, failure.Err))
	}
	return strings.Join(parts, "; ")
}

func (e ExecutionErrors) Unwrap() error {
	if len(e) == 0 {
		return nil
	}
	return e[0].Err
}
