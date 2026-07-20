package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
)

type planStore struct{ owner *Store }

func (s *planStore) Put(ctx context.Context, record persistence.PlanRecord) (bool, error) {
	return s.owner.putPlan(ctx, record)
}

func (s *planStore) Get(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision) (persistence.PlanRecord, bool, error) {
	return s.owner.getPlan(ctx, runtimeID, revision)
}

func (s *planStore) List(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.PlanRecord, error) {
	return s.owner.listPlans(ctx, runtimeID)
}

func (s *Store) putPlan(ctx context.Context, record persistence.PlanRecord) (bool, error) {
	existing, found, err := s.getPlan(ctx, record.RuntimeID, record.PlanRevision)
	if err != nil {
		return false, err
	}
	if found {
		if existing.PlanHash != record.PlanHash {
			return false, persistence.ErrPlanConflict
		}
		return false, nil
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = s.now()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO runtime_plans(runtime_id, plan_revision, plan_hash, manifest, created_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(runtime_id, plan_revision) DO NOTHING`, record.RuntimeID, record.PlanRevision,
		record.PlanHash, []byte(record.Manifest), timeValue(record.CreatedAt))
	if err != nil {
		return false, fmt.Errorf("store runtime plan: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed == 0 {
		existing, found, err = s.getPlan(ctx, record.RuntimeID, record.PlanRevision)
		if err != nil {
			return false, err
		}
		if !found || existing.PlanHash != record.PlanHash {
			return false, persistence.ErrPlanConflict
		}
		return false, nil
	}
	return true, nil
}

func (s *Store) getPlan(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision) (persistence.PlanRecord, bool, error) {
	var record persistence.PlanRecord
	var createdAt int64
	var manifest []byte
	err := s.db.QueryRowContext(ctx, `SELECT runtime_id, plan_revision, plan_hash, manifest, created_at
		FROM runtime_plans WHERE runtime_id = ? AND plan_revision = ?`, runtimeID, revision).
		Scan(&record.RuntimeID, &record.PlanRevision, &record.PlanHash, &manifest, &createdAt)
	if err == sql.ErrNoRows {
		return persistence.PlanRecord{}, false, nil
	}
	if err != nil {
		return persistence.PlanRecord{}, false, err
	}
	record.Manifest = contract.CloneRaw(manifest)
	record.CreatedAt = timeFromValue(createdAt)
	return record, true, nil
}

func (s *Store) listPlans(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.PlanRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT runtime_id, plan_revision, plan_hash, manifest, created_at
		FROM runtime_plans WHERE runtime_id = ? ORDER BY plan_revision`, runtimeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []persistence.PlanRecord
	for rows.Next() {
		var record persistence.PlanRecord
		var manifest []byte
		var createdAt int64
		if err := rows.Scan(&record.RuntimeID, &record.PlanRevision, &record.PlanHash, &manifest, &createdAt); err != nil {
			return nil, err
		}
		record.Manifest = contract.CloneRaw(manifest)
		record.CreatedAt = timeFromValue(createdAt)
		result = append(result, record)
	}
	return result, rows.Err()
}
