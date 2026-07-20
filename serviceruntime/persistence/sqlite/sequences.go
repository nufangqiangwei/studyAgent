package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
)

type sequenceStore struct{ owner *Store }

func (s *sequenceStore) Assign(ctx context.Context, scope string, message contract.Message) (contract.Message, error) {
	if message.StreamID == "" {
		if message.Sequence != 0 {
			return contract.Message{}, fmt.Errorf("message sequence requires a stream id")
		}
		return message.Clone(), nil
	}
	tx, err := s.owner.db.BeginTx(ctx, nil)
	if err != nil {
		return contract.Message{}, err
	}
	defer rollback(tx)
	message, err = assignMessageTx(ctx, tx, scope, message)
	if err != nil {
		return contract.Message{}, err
	}
	if err := tx.Commit(); err != nil {
		return contract.Message{}, err
	}
	return message, nil
}

func assignMessageTx(ctx context.Context, tx *sql.Tx, scope string, message contract.Message) (contract.Message, error) {
	if message.StreamID == "" {
		if message.Sequence != 0 {
			return contract.Message{}, fmt.Errorf("message sequence requires a stream id")
		}
		return message.Clone(), nil
	}
	if scope == "" {
		return contract.Message{}, fmt.Errorf("message sequence scope is required")
	}
	var runtimeID contract.RuntimeID
	var streamID contract.StreamID
	var sequence uint64
	err := tx.QueryRowContext(ctx, `SELECT runtime_id, stream_id, sequence FROM message_sequences WHERE scope = ? AND message_id = ?`, scope, message.ID).
		Scan(&runtimeID, &streamID, &sequence)
	if err == nil {
		if runtimeID != message.RuntimeID || streamID != message.StreamID || message.Sequence != 0 && message.Sequence != sequence {
			return contract.Message{}, persistence.ErrDuplicateID
		}
		message.Sequence = sequence
		return message.Clone(), nil
	}
	if err != sql.ErrNoRows {
		return contract.Message{}, err
	}
	var head uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM message_sequences
		WHERE scope = ? AND runtime_id = ? AND stream_id = ?`, scope, message.RuntimeID, message.StreamID).Scan(&head); err != nil {
		return contract.Message{}, err
	}
	next := head + 1
	if message.Sequence != 0 && message.Sequence != next {
		return contract.Message{}, fmt.Errorf("message stream %q sequence %d is not next after %d", message.StreamID, message.Sequence, head)
	}
	message.Sequence = next
	if _, err := tx.ExecContext(ctx, `INSERT INTO message_sequences(scope, message_id, runtime_id, stream_id, sequence)
		VALUES (?, ?, ?, ?, ?)`, scope, message.ID, message.RuntimeID, message.StreamID, message.Sequence); err != nil {
		return contract.Message{}, err
	}
	return message.Clone(), nil
}
