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

// TestPgIndex_RepeatedCollectsEmitFullSnapshot: every tick re-emits the
// full snapshot regardless of whether anything changed.
func TestPgIndex_RepeatedCollectsEmitFullSnapshot(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil")
	}
	first := decodePgIndexPayload(t, r1.JSON)

	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("second Collect() error: %v", err)
	}
	if r2 == nil {
		t.Fatal("second Collect() should re-emit full snapshot, got nil")
	}
	second := decodePgIndexPayload(t, r2.JSON)
	if len(second.Rows) != len(first.Rows) {
		t.Fatalf("repeated Collect() row count mismatch: got %d, want %d", len(second.Rows), len(first.Rows))
	}
}

// TestPgIndex_NewIndexAppearsInNextPayload: creating an index between
// collections causes the new row to appear in the next payload alongside
// the existing ones.
func TestPgIndex_NewIndexAppearsInNextPayload(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("seed Collect() error: %v", err)
	}

	const idxName = "idx_pg_index_new_appears"
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
		t.Fatal("Collect() after CREATE INDEX should emit a payload, got nil")
	}
	p := decodePgIndexPayload(t, r2.JSON)
	var found bool
	for _, row := range p.Rows {
		if row.IndexName != nil && string(*row.IndexName) == idxName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected new index %q in payload", idxName)
	}
}

// TestPgIndex_DroppedIndexNotEmitted: a DROP causes the index to disappear
// from subsequent payloads.
func TestPgIndex_DroppedIndexNotEmitted(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)

	const idxName = "idx_pg_index_drop"
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
	if r2 == nil {
		t.Fatal("Collect() after DROP INDEX should still emit other indexes, got nil")
	}
	p := decodePgIndexPayload(t, r2.JSON)
	for _, row := range p.Rows {
		if row.IndexName != nil && string(*row.IndexName) == idxName {
			t.Fatalf("dropped index %q should not appear in payload", idxName)
		}
	}
}
