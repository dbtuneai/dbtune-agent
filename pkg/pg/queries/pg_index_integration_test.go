//go:build integration

package queries

import (
	"context"
	"encoding/json"
	"testing"
)

func pgIndexLatest(t *testing.T) pgInstance {
	t.Helper()
	if len(pgInstances) == 0 {
		t.Skip("no pg instances provisioned")
	}
	return pgInstances[len(pgInstances)-1]
}

func decodePgIndexPayload(t *testing.T, raw []byte) Payload[PgIndexRow] {
	t.Helper()
	var p Payload[PgIndexRow]
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("failed to decode pg_index payload: %v", err)
	}
	return p
}

// TestPgIndex_FirstCollectEmitsAll: a fresh collector emits every existing
// index on its first call.
func TestPgIndex_FirstCollectEmitsAll(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	r, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r == nil {
		t.Fatal("first Collect() should not return nil — fixtures populate pg_index")
	}
	p := decodePgIndexPayload(t, r.JSON)
	if len(p.Rows) == 0 {
		t.Fatal("first payload should contain rows from fixture indexes")
	}
}

// TestPgIndex_NoChangesReturnsNil: with no volatile-field changes between
// collections, the per-row dedup leaves an empty payload and the collector
// signals "nothing to send" by returning nil.
func TestPgIndex_NoChangesReturnsNil(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("second Collect() error: %v", err)
	}
	if r2 != nil {
		t.Fatalf("unchanged second Collect() should return nil, got %d bytes", len(r2.JSON))
	}
}

// TestPgIndex_OnlyChangedRowEmitted: after the first collection seeds the
// cache, a single new index produces a payload containing only that row.
func TestPgIndex_OnlyChangedRowEmitted(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("seed Collect() error: %v", err)
	}

	const idxName = "idx_pg_index_dedup_only_changed"
	if _, err := inst.admin.Exec(ctx, "CREATE INDEX "+idxName+" ON test_users(name, score)"); err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = inst.admin.Exec(context.Background(), "DROP INDEX IF EXISTS "+idxName)
	})

	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() after CREATE INDEX error: %v", err)
	}
	if r2 == nil {
		t.Fatal("Collect() after CREATE INDEX should emit the new row, got nil")
	}
	p := decodePgIndexPayload(t, r2.JSON)
	if len(p.Rows) != 1 {
		names := make([]string, 0, len(p.Rows))
		for _, row := range p.Rows {
			if row.IndexName != nil {
				names = append(names, string(*row.IndexName))
			}
		}
		t.Fatalf("expected exactly 1 changed row, got %d: %v", len(p.Rows), names)
	}
	if p.Rows[0].IndexName == nil || string(*p.Rows[0].IndexName) != idxName {
		got := "<nil>"
		if p.Rows[0].IndexName != nil {
			got = string(*p.Rows[0].IndexName)
		}
		t.Fatalf("expected only %q in payload, got %q", idxName, got)
	}
}

// TestPgIndex_DroppedIndexNotEmitted: a DROP causes the index to disappear
// from the cache without producing a row in the next payload.
func TestPgIndex_DroppedIndexNotEmitted(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	const idxName = "idx_pg_index_dedup_drop"
	if _, err := inst.admin.Exec(ctx, "CREATE INDEX "+idxName+" ON test_users(score)"); err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = inst.admin.Exec(context.Background(), "DROP INDEX IF EXISTS "+idxName)
	})

	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if _, err := inst.admin.Exec(ctx, "DROP INDEX "+idxName); err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}

	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() after DROP INDEX error: %v", err)
	}
	if r2 != nil {
		p := decodePgIndexPayload(t, r2.JSON)
		for _, row := range p.Rows {
			if row.IndexName != nil && string(*row.IndexName) == idxName {
				t.Fatalf("dropped index %q should not appear in payload", idxName)
			}
		}
		if len(p.Rows) > 0 {
			t.Logf("post-drop payload non-empty (%d rows) due to incidental stats churn", len(p.Rows))
		}
	}
}

// TestPgIndex_VolatileChangeEmitsOnlyAffectedRow drives stats churn on one
// table while leaving an adjacent table untouched, and verifies the next
// payload contains only the affected index.
func TestPgIndex_VolatileChangeEmitsOnlyAffectedRow(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()

	setup := []string{
		`CREATE TABLE test_pg_index_hot (id int)`,
		`CREATE INDEX idx_pg_index_hot_id ON test_pg_index_hot(id)`,
		`CREATE TABLE test_pg_index_cold (id int)`,
		`CREATE INDEX idx_pg_index_cold_id ON test_pg_index_cold(id)`,
		`INSERT INTO test_pg_index_hot SELECT i FROM generate_series(1, 100) AS i`,
		`INSERT INTO test_pg_index_cold SELECT i FROM generate_series(1, 100) AS i`,
		`ANALYZE test_pg_index_hot`,
		`ANALYZE test_pg_index_cold`,
	}
	for _, sql := range setup {
		if _, err := inst.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		_, _ = inst.admin.Exec(context.Background(), "DROP TABLE IF EXISTS test_pg_index_hot")
		_, _ = inst.admin.Exec(context.Background(), "DROP TABLE IF EXISTS test_pg_index_cold")
	})

	c := PgIndexCollector(inst.pool, noopPrepareCtx)
	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("seed Collect() error: %v", err)
	}

	churn := []string{
		`INSERT INTO test_pg_index_hot SELECT i FROM generate_series(1, 10000) AS i`,
		`ANALYZE test_pg_index_hot`,
	}
	for _, sql := range churn {
		if _, err := inst.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("churn %q: %v", sql, err)
		}
	}

	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("post-churn Collect() error: %v", err)
	}
	if r2 == nil {
		t.Fatal("post-churn Collect() should emit the affected row, got nil")
	}
	p := decodePgIndexPayload(t, r2.JSON)

	var foundHot, foundCold bool
	for _, row := range p.Rows {
		if row.IndexName == nil {
			continue
		}
		switch string(*row.IndexName) {
		case "idx_pg_index_hot_id":
			foundHot = true
		case "idx_pg_index_cold_id":
			foundCold = true
		}
	}
	if !foundHot {
		t.Error("hot-table index should appear in payload")
	}
	if foundCold {
		t.Error("cold-table index should NOT appear in payload (its volatile fields were unchanged)")
	}
}

// TestPgIndex_HeartbeatResendsAll: even with no catalog changes, the
// heartbeat causes a full re-emit. Uses the test-only injectable threshold
// to avoid 288 collections.
func TestPgIndex_HeartbeatResendsAll(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	const heartbeat = 2
	c := pgIndexCollectorWithHeartbeat(inst.pool, noopPrepareCtx, heartbeat)

	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil")
	}
	first := decodePgIndexPayload(t, r1.JSON)
	initialCount := len(first.Rows)
	if initialCount == 0 {
		t.Fatal("first payload should be non-empty")
	}

	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("heartbeat Collect() error: %v", err)
	}
	if r2 == nil {
		t.Fatal("heartbeat Collect() should re-emit all rows, got nil")
	}
	beat := decodePgIndexPayload(t, r2.JSON)
	if len(beat.Rows) != initialCount {
		t.Fatalf("heartbeat re-emit row count mismatch: got %d, want %d", len(beat.Rows), initialCount)
	}
}
