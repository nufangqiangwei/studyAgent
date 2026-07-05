package eventbus

import (
	"fmt"
	"strings"
)

var ErrBusClosed = fmt.Errorf("event bus is closed")

type DeliveryError struct {
	SubscriptionID string
	Err            error
}

type DeliveryErrors []DeliveryError

func (e DeliveryErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	parts := make([]string, 0, len(e))
	for _, failure := range e {
		if failure.Err == nil {
			continue
		}
		if failure.SubscriptionID == "" {
			parts = append(parts, failure.Err.Error())
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %v", failure.SubscriptionID, failure.Err))
	}
	return strings.Join(parts, "; ")
}

func (e DeliveryErrors) Unwrap() error {
	if len(e) == 0 {
		return nil
	}
	return e[0].Err
}
