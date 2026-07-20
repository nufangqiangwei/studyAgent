package lease

import (
	"context"
	"testing"
	"time"
)

func TestHeartbeatRenewsUntilStopped(t *testing.T) {
	renewed := make(chan struct{}, 1)
	heartbeat := Start(context.Background(), time.Millisecond, func(context.Context) error {
		select {
		case renewed <- struct{}{}:
		default:
		}
		return nil
	})
	select {
	case <-renewed:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not renew")
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatal(err)
	}
}
