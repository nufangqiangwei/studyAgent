package approval

import (
	"agent/serviceruntime/service"
	"encoding/json"
	"fmt"
)

func initialAggregateState() aggregateState {
	return aggregateState{Approvals: make(map[string]State)}
}

func encodeState(value aggregateState) (service.State, error) {
	if value.Approvals == nil {
		value.Approvals = make(map[string]State)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return service.State{}, fmt.Errorf("encode approval state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: data}, nil
}

func decodeState(raw service.State) (aggregateState, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return aggregateState{}, fmt.Errorf("approval state schema version %d is unsupported", raw.SchemaVersion)
	}
	var value aggregateState
	if len(raw.Data) == 0 {
		return aggregateState{}, fmt.Errorf("approval state data is empty")
	}
	if err := json.Unmarshal(raw.Data, &value); err != nil {
		return aggregateState{}, fmt.Errorf("decode approval state: %w", err)
	}
	if value.Approvals == nil {
		value.Approvals = make(map[string]State)
	}
	cloned := make(map[string]State, len(value.Approvals))
	for id, approval := range value.Approvals {
		if clean(id) == "" || approval.ApprovalID != id || !approval.Status.Valid() {
			return aggregateState{}, fmt.Errorf("approval state contains an invalid record %q", id)
		}
		cloned[id] = approval.Clone()
	}
	value.Approvals = cloned
	return value, nil
}
