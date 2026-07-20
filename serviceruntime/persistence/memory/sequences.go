package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
)

type sequenceStore struct{ owner *Store }

func (s *sequenceStore) Assign(ctx context.Context, scope string, message contract.Message) (contract.Message, error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if err := s.owner.check(ctx); err != nil {
		return contract.Message{}, err
	}
	return assignMessage(scope, message, s.owner.messageSequences, s.owner.messageHeads)
}

func assignMessage(scope string, message contract.Message, assignments map[messageSequenceKey]messageSequence, heads map[messageStreamKey]uint64) (contract.Message, error) {
	if message.StreamID == "" {
		if message.Sequence != 0 {
			return contract.Message{}, fmt.Errorf("message sequence requires a stream id")
		}
		return message.Clone(), nil
	}
	if scope == "" {
		return contract.Message{}, fmt.Errorf("message sequence scope is required")
	}
	assignmentKey := messageSequenceKey{scope: scope, message: message.ID}
	if assigned, found := assignments[assignmentKey]; found {
		if assigned.runtime != message.RuntimeID || assigned.stream != message.StreamID || message.Sequence != 0 && assigned.sequence != message.Sequence {
			return contract.Message{}, persistence.ErrDuplicateID
		}
		message.Sequence = assigned.sequence
		return message.Clone(), nil
	}
	key := messageStreamKey{scope: scope, runtime: message.RuntimeID, stream: message.StreamID}
	next := heads[key] + 1
	if message.Sequence != 0 && message.Sequence != next {
		return contract.Message{}, fmt.Errorf("message stream %q sequence %d is not next after %d", message.StreamID, message.Sequence, heads[key])
	}
	message.Sequence = next
	assignments[assignmentKey] = messageSequence{runtime: message.RuntimeID, stream: message.StreamID, sequence: next}
	heads[key] = next
	return message.Clone(), nil
}

func cloneMessageSequences(values map[messageSequenceKey]messageSequence) map[messageSequenceKey]messageSequence {
	cloned := make(map[messageSequenceKey]messageSequence, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneMessageHeads(values map[messageStreamKey]uint64) map[messageStreamKey]uint64 {
	cloned := make(map[messageStreamKey]uint64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
