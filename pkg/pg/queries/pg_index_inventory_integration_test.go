//go:build integration

package queries

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
)

func decodePgIndexInventoryPayload(t *testing.T, raw []byte) Payload[PgIndexInventoryRow] {
	t.Helper()
	var p Payload[PgIndexInventoryRow]
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("failed to decode pg_index_inventory payload: %v", err)
	}
	return p
}

// TestPgIndexInventory_HasStructuralFields verifies that each inventory row
// carries the structural fields the hot pg_index path no longer ships.
func TestPgIndexInventory_HasStructuralFields(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexInventoryCollector(inst.pool, noopPrepareCtx, inst.version)

	r, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
	if r == nil {
		t.Fatal("Collect() returned nil — fixtures should populate inventory")
	}
	p := decodePgIndexInventoryPayload(t, r.JSON)
	if len(p.Rows) == 0 {
		t.Fatal("inventory should be non-empty when fixture indexes exist")
	}
	for i, row := range p.Rows {
		if row.SchemaName == nil || row.IndexName == nil {
			t.Fatalf("row %d missing schema/index name", i)
		}
		if row.IndexDef == nil {
			t.Errorf("row %d (%q): expected indexdef to be non-nil", i, *row.IndexName)
		}
		if row.IndexRelID == nil {
			t.Errorf("row %d (%q): expected indexrelid to be non-nil", i, *row.IndexName)
		}
		if row.IndNatts == nil {
			t.Errorf("row %d (%q): expected indnatts to be non-nil", i, *row.IndexName)
		}
	}
}

// TestPgIndexInventory_ReflectsCreateAndDrop: the inventory should include
// new indexes immediately and drop them on the next collection.
func TestPgIndexInventory_ReflectsCreateAndDrop(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexInventoryCollector(inst.pool, noopPrepareCtx, inst.version)

	const idxName = "idx_pg_index_inventory_lifecycle"

	if _, err := c.Collect(ctx); err != nil {
		t.Fatalf("baseline Collect() error: %v", err)
	}

	// Create the index — inventory must include it.
	if _, err := inst.admin.Exec(ctx, "CREATE INDEX "+idxName+" ON test_users(score)"); err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = inst.admin.Exec(context.Background(), "DROP INDEX IF EXISTS "+idxName)
	})

	rCreate, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() after CREATE INDEX error: %v", err)
	}
	if rCreate == nil {
		t.Fatal("inventory should re-emit after CREATE INDEX, got nil")
	}
	pCreate := decodePgIndexInventoryPayload(t, rCreate.JSON)
	if !inventoryHas(pCreate, idxName) {
		t.Fatalf("inventory after CREATE INDEX missing %q", idxName)
	}

	// Drop the index — inventory must NOT include it on the next collection.
	if _, err := inst.admin.Exec(ctx, "DROP INDEX "+idxName); err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}

	rDrop, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() after DROP INDEX error: %v", err)
	}
	if rDrop == nil {
		t.Fatal("inventory should re-emit after DROP INDEX, got nil")
	}
	pDrop := decodePgIndexInventoryPayload(t, rDrop.JSON)
	if inventoryHas(pDrop, idxName) {
		t.Fatalf("inventory after DROP INDEX still contains %q", idxName)
	}
}

// TestPgIndexInventory_KeyUniqueness: every (schemaname, indexname) pair in
// a payload is unique.
func TestPgIndexInventory_KeyUniqueness(t *testing.T) {
	inst := pgIndexLatest(t)
	ctx := context.Background()
	c := PgIndexInventoryCollector(inst.pool, noopPrepareCtx, inst.version)

	r, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
	if r == nil {
		t.Fatal("Collect() returned nil")
	}
	p := decodePgIndexInventoryPayload(t, r.JSON)
	keys := make([]string, 0, len(p.Rows))
	for _, row := range p.Rows {
		if row.SchemaName == nil || row.IndexName == nil {
			t.Fatalf("row missing schema/index name: %#v", row)
		}
		keys = append(keys, string(*row.SchemaName)+"."+string(*row.IndexName))
	}
	sort.Strings(keys)
	for i := 1; i < len(keys); i++ {
		if keys[i] == keys[i-1] {
			t.Fatalf("duplicate key in inventory: %q", keys[i])
		}
	}
}

func inventoryHas(p Payload[PgIndexInventoryRow], name string) bool {
	for _, row := range p.Rows {
		if row.IndexName != nil && string(*row.IndexName) == name {
			return true
		}
	}
	return false
}
