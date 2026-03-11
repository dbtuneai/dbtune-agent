package pgxutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

// ═══════════════════════════════════════════════════════════════════════
// TEST INVENTORY
//
// Tests are grouped by concern. Each group is documented with why it
// matters for a read-only monitoring agent connecting to arbitrary PG
// versions via SELECT *.
//
// ── GROUP 1: Core laxness ──────────────────────────────────────────
//   Tests that the two fundamental lax behaviors work independently
//   and together.
//
//   TestScan_ExtraColumnsSkipped
//     Row has columns not in struct → silently skipped.
//     Why: PG upgrades add columns; unknown future columns must not break us.
//
//   TestScan_MissingColumnsZeroValued
//     Struct has fields not in row → zero value.
//     Why: newer struct fields must degrade gracefully on older PG.
//
//   TestScan_BothDirections
//     Extra columns AND missing fields simultaneously.
//     Why: the real-world scenario — connecting to any PG version.
//
// ── GROUP 2: Struct field control ──────────────────────────────────
//   Tests for db tags, naming, and field visibility.
//
//   TestScan_DbTagIgnore
//     `db:"-"` fields are never scanned, even when the column exists.
//     Why: explicitly excluding columns (e.g. heavy/unneeded data).
//
//   TestScan_DbTagRename
//     `db:"custom_name"` maps a Go field to a differently-named column.
//     Why: PG system views use snake_case; Go uses CamelCase.
//
//   TestScan_CaseInsensitiveMatch
//     Column "DATNAME" matches field tagged `db:"datname"`.
//     Why: PG normalizes identifiers to lowercase, but quoted identifiers
//     or custom views might not.
//
//   TestScan_UnexportedFieldsIgnored
//     Unexported struct fields are silently skipped.
//     Why: defense against accidentally matching private fields.
//
//   TestScan_NoMatchingFieldsAtAll
//     Struct and row share zero column names.
//     Why: degenerate case; must not panic, just produce zero-valued struct.
//
// ── GROUP 3: Embedded structs ──────────────────────────────────────
//   Tests for struct composition, which is how you'd organize a large
//   monitoring struct (base fields + version-specific groups).
//
//   TestScan_EmbeddedStruct
//     Single-level anonymous embed.
//
//   TestScan_DeeplyNestedEmbed
//     Multi-level nesting: fields at depth 3.
//     Why: version-grouped sub-structs (Base > PG14Fields > PG17Fields).
//
//   TestScan_EmbeddedWithIgnoredField
//     Embedded struct contains a `db:"-"` field.
//     Why: composing structs where inner fields need exclusion.
//
//   TestScan_EmbeddedWithMissingColumns
//     Embedded struct's fields have no matching columns.
//     Why: the whole point — PG14Fields embedded but running on PG12.
//
// ── GROUP 4: PG type handling ──────────────────────────────────────
//   Tests that pgx's type system handles the types we care about in
//   system views, via our nil-skip scan target mechanism.
//
//   TestScan_NullIntoPointer
//     PG NULL scans into *int64 / *string as nil.
//     Why: pg_stat_database has nullable columns (checksum_failures, stats_reset).
//
//   TestScan_NullIntoNonPointer
//     PG NULL into a non-nullable Go type → scan error.
//     Why: we must know if we've typed a field wrong.
//
//   TestScan_TypeCoercions
//     PG integer → int32, bigint → int64, double → float64, text → string.
//     Why: pg_stat_database mixes int4/int8/float8/text columns.
//
//   TestScan_BoolColumn
//     PG boolean columns scan correctly even when skipped or present.
//     Why: some system views have boolean columns.
//
// ── GROUP 5: Version transitions ───────────────────────────────────
//   Tests simulating PG upgrades, downgrades, and mixed-version fleets
//   using the same Scanner instance.
//
//   TestScan_Upgrade
//     Column set grows across versions (PG N → N+1 → N+2).
//
//   TestScan_Downgrade
//     Column set shrinks (failover to older replica).
//
//   TestScan_UpgradeDowngradeCycle
//     Full cycle: old → new → old → new.
//
//   TestScan_ColumnOrderChange
//     Same column names but in different order.
//     Why: a PG major version could theoretically reorder columns in a
//     system view. Our name-based matching must handle this.
//
// ── GROUP 6: Multi-row and empty results ───────────────────────────
//   Tests for result set shape — not just single-row synthetics.
//
//   TestScan_EmptyResultSet
//     Zero rows returned → empty slice, no error.
//     Why: a freshly-created database might have no stats.
//
//   TestScan_MultipleRows
//     Multiple rows with varying data.
//     Why: pg_stat_database returns one row per database.
//
//   TestScan_MultipleRowsWithNulls
//     Some rows have NULLs, others don't.
//     Why: stats_reset is NULL until the first reset.
//
// ── GROUP 7: Concurrency ───────────────────────────────────────────
//   Tests that the Scanner is safe for concurrent use, which matters
//   for a monitoring agent querying multiple PG instances in parallel.
//
//   TestScan_ConcurrentSameLayout
//     Multiple goroutines scanning the same column layout.
//
//   TestScan_ConcurrentDifferentLayouts
//     Multiple goroutines with different column layouts racing to
//     populate the cache.
//
// ── GROUP 8: Scanner construction ──────────────────────────────────
//
//   TestNewScanner_PanicsOnNonStruct
//     NewScanner[int]() must panic.
//
// ── GROUP 9: Integration ───────────────────────────────────────────
//
//   TestScan_PgStatDatabase
//     End-to-end with the real pg_stat_database view.
//
//   TestScan_PgStatActivity
//     Another real system view to prove generality.
//
// ═══════════════════════════════════════════════════════════════════════

// ── Shared types ────────────────────────────────────────────────────

type PgStatDatabase struct {
	// Present in all supported versions
	Datid        int64   `db:"datid"`
	Datname      string  `db:"datname"`
	Numbackends  int32   `db:"numbackends"`
	XactCommit   int64   `db:"xact_commit"`
	XactRollback int64   `db:"xact_rollback"`
	BlksRead     int64   `db:"blks_read"`
	BlksHit      int64   `db:"blks_hit"`
	TupReturned  int64   `db:"tup_returned"`
	TupFetched   int64   `db:"tup_fetched"`
	TupInserted  int64   `db:"tup_inserted"`
	TupUpdated   int64   `db:"tup_updated"`
	TupDeleted   int64   `db:"tup_deleted"`
	Conflicts    int64   `db:"conflicts"`
	TempFiles    int64   `db:"temp_files"`
	TempBytes    int64   `db:"temp_bytes"`
	Deadlocks    int64   `db:"deadlocks"`
	BlkReadTime  float64 `db:"blk_read_time"`
	BlkWriteTime float64 `db:"blk_write_time"`
	StatsReset   *string `db:"stats_reset"`

	// PG 12+
	ChecksumFailures    *int64  `db:"checksum_failures"`
	ChecksumLastFailure *string `db:"checksum_last_failure"`

	// PG 14+
	SessionTime           float64 `db:"session_time"`
	ActiveTime            float64 `db:"active_time"`
	IdleInTransactionTime float64 `db:"idle_in_transaction_time"`
	Sessions              int64   `db:"sessions"`
	SessionsAbandoned     int64   `db:"sessions_abandoned"`
	SessionsFatal         int64   `db:"sessions_fatal"`
	SessionsKilled        int64   `db:"sessions_killed"`

	// PG 17+
	ParallelWorkers  int64 `db:"parallel_workers"`
	TempBytesRead    int64 `db:"temp_bytes_read"`
	TempBytesWritten int64 `db:"temp_bytes_written"`
	TempFilesRead    int64 `db:"temp_files_read"`
	TempFilesWritten int64 `db:"temp_files_written"`
}

var pgStatScanner = NewScanner[PgStatDatabase]()

func QueryPgStatDatabase(ctx context.Context, conn *pgx.Conn) ([]PgStatDatabase, error) {
	rows, err := conn.Query(ctx, "SELECT * FROM pg_stat_database")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgStatScanner.Scan)
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 1: Core laxness
// ═════════════════════════════════════════════════════════════════════

func TestScan_ExtraColumnsSkipped(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type small struct {
		Name string `db:"name"`
		Age  int32  `db:"age"`
	}
	scanner := NewScanner[small]()

	rows, _ := conn.Query(ctx,
		`SELECT 'Alice' AS name, 30 AS age, 'ignored' AS extra_col`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Name != "Alice" || result[0].Age != 30 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestScan_MissingColumnsZeroValued(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type wide struct {
		Name    string  `db:"name"`
		Age     int32   `db:"age"`
		Missing float64 `db:"does_not_exist"`
	}
	scanner := NewScanner[wide]()

	rows, _ := conn.Query(ctx, `SELECT 'Bob' AS name, 25 AS age`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Missing != 0 {
		t.Fatalf("expected Missing=0, got %f", result[0].Missing)
	}
}

func TestScan_BothDirections(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type mixed struct {
		Name     string `db:"name"`
		NotInRow int64  `db:"not_in_row"`
	}
	scanner := NewScanner[mixed]()

	rows, _ := conn.Query(ctx, `SELECT 'Carol' AS name, 99 AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "Carol" || result[0].NotInRow != 0 {
		t.Fatalf("unexpected: %+v", result[0])
	}
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 2: Struct field control
// ═════════════════════════════════════════════════════════════════════

func TestScan_DbTagIgnore(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withIgnored struct {
		Name   string `db:"name"`
		Age    int32  `db:"age"`
		Ignore bool   `db:"-"`
	}
	scanner := NewScanner[withIgnored]()

	t.Run("column absent", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Dave' AS name, 30 AS age`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Dave" || result[0].Age != 30 {
			t.Fatalf("unexpected values: %+v", result[0])
		}
		if result[0].Ignore != false {
			t.Fatal("ignored field should be zero")
		}
	})

	t.Run("column present but ignored", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Eve' AS name, 25 AS age, true AS ignore`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Eve" || result[0].Age != 25 {
			t.Fatalf("unexpected values: %+v", result[0])
		}
		if result[0].Ignore != false {
			t.Fatal("ignored field should be zero even though column was returned")
		}
	})

	t.Run("column absent again after being present", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'Frank' AS name, 40 AS age`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Name != "Frank" || result[0].Age != 40 || result[0].Ignore != false {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})
}

func TestScan_DbTagRename(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type renamed struct {
		TransactionsCommitted  int64 `db:"xact_commit"`
		TransactionsRolledBack int64 `db:"xact_rollback"`
	}
	scanner := NewScanner[renamed]()

	rows, _ := conn.Query(ctx, `SELECT 100 AS xact_commit, 5 AS xact_rollback`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].TransactionsCommitted != 100 || result[0].TransactionsRolledBack != 5 {
		t.Fatalf("tag rename failed: %+v", result[0])
	}
}

func TestScan_CaseInsensitiveMatch(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type cased struct {
		Name string `db:"name"`
		Age  int32  `db:"age"`
	}
	scanner := NewScanner[cased]()

	// PG lowercases unquoted identifiers, but quoted ones preserve case.
	// Our scanner lowercases both sides, so this should work.
	rows, _ := conn.Query(ctx, `SELECT 'Alice' AS "Name", 30 AS "AGE"`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "Alice" || result[0].Age != 30 {
		t.Fatalf("case-insensitive match failed: %+v", result[0])
	}
}

func TestScan_UnexportedFieldsIgnored(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withUnexported struct {
		Name   string `db:"name"`
		secret string //nolint:unused // unexported — must not match column "secret"
	}
	scanner := NewScanner[withUnexported]()

	// Column "secret" exists in the row but the struct field is unexported.
	// This column should be skipped (not cause an error or silently write
	// to the unexported field).
	rows, _ := conn.Query(ctx, `SELECT 'visible' AS name, 'hidden' AS secret`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Name != "visible" {
		t.Fatalf("unexpected: %+v", result[0])
	}
	if result[0].secret != "" {
		t.Fatal("unexported field should not have been written to")
	}
}

func TestScan_NoMatchingFieldsAtAll(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type disjoint struct {
		Foo string `db:"foo"`
		Bar int32  `db:"bar"`
	}
	scanner := NewScanner[disjoint]()

	// No column matches any struct field. All columns skipped, all fields zeroed.
	rows, _ := conn.Query(ctx, `SELECT 1 AS x, 2 AS y, 3 AS z`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Foo != "" || result[0].Bar != 0 {
		t.Fatalf("all fields should be zero: %+v", result[0])
	}
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 3: Embedded structs
// ═════════════════════════════════════════════════════════════════════

func TestScan_EmbeddedStruct(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Base struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	type Extended struct {
		Base
		Extra string `db:"extra"`
	}
	scanner := NewScanner[Extended]()

	rows, _ := conn.Query(ctx, `SELECT 1 AS id, 'test' AS name, 'bonus' AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].ID != 1 || result[0].Name != "test" || result[0].Extra != "bonus" {
		t.Fatalf("unexpected: %+v", result[0])
	}
}

func TestScan_DeeplyNestedEmbed(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	// Simulates: Base fields → PG14 group → PG17 group
	type BaseFields struct {
		Datname  string `db:"datname"`
		Backends int32  `db:"numbackends"`
	}
	type PG14Fields struct {
		BaseFields
		SessionTime float64 `db:"session_time"`
	}
	type PG17Fields struct {
		PG14Fields
		ParallelWorkers int64 `db:"parallel_workers"`
	}
	scanner := NewScanner[PG17Fields]()

	t.Run("all columns present", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'mydb' AS datname, 5 AS numbackends, 100.5 AS session_time, 8 AS parallel_workers`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.Datname != "mydb" || r.Backends != 5 || r.SessionTime != 100.5 || r.ParallelWorkers != 8 {
			t.Fatalf("unexpected: %+v", r)
		}
	})

	t.Run("only base columns present", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'olddb' AS datname, 3 AS numbackends`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		r := result[0]
		if r.Datname != "olddb" || r.Backends != 3 {
			t.Fatalf("base fields wrong: %+v", r)
		}
		if r.SessionTime != 0 || r.ParallelWorkers != 0 {
			t.Fatalf("nested fields should be zero: %+v", r)
		}
	})
}

func TestScan_EmbeddedWithIgnoredField(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type Inner struct {
		Keep string `db:"keep"`
		Skip string `db:"-"`
	}
	type Outer struct {
		Inner
		Extra int32 `db:"extra"`
	}
	scanner := NewScanner[Outer]()

	rows, _ := conn.Query(ctx, `SELECT 'kept' AS keep, 'skipped' AS skip, 42 AS extra`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Keep != "kept" || result[0].Extra != 42 {
		t.Fatalf("unexpected: %+v", result[0])
	}
	if result[0].Skip != "" {
		t.Fatal("inner db:\"-\" field should be zero")
	}
}

func TestScan_EmbeddedWithMissingColumns(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	// The entire PG14 group has no matching columns — running on PG 12.
	type CoreFields struct {
		Datname string `db:"datname"`
	}
	type PG14Fields struct {
		SessionTime float64 `db:"session_time"`
		ActiveTime  float64 `db:"active_time"`
	}
	type Combined struct {
		CoreFields
		PG14Fields
	}
	scanner := NewScanner[Combined]()

	rows, _ := conn.Query(ctx, `SELECT 'pg12db' AS datname`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := result[0]
	if r.Datname != "pg12db" {
		t.Fatalf("core field wrong: %+v", r)
	}
	if r.SessionTime != 0 || r.ActiveTime != 0 {
		t.Fatalf("PG14 fields should be zero on PG12: %+v", r)
	}
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 4: PG type handling
// ═════════════════════════════════════════════════════════════════════

func TestScan_NullIntoPointer(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withPointers struct {
		Name      string  `db:"name"`
		NullableN *int64  `db:"nullable_n"`
		NullableS *string `db:"nullable_s"`
	}
	scanner := NewScanner[withPointers]()

	t.Run("null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'test' AS name, NULL::bigint AS nullable_n, NULL::text AS nullable_s`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].NullableN != nil || result[0].NullableS != nil {
			t.Fatalf("expected nils: %+v", result[0])
		}
	})

	t.Run("non-null values", func(t *testing.T) {
		rows, _ := conn.Query(ctx,
			`SELECT 'test' AS name, 42::bigint AS nullable_n, 'hello'::text AS nullable_s`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].NullableN == nil || *result[0].NullableN != 42 {
			t.Fatalf("expected 42: %+v", result[0])
		}
		if result[0].NullableS == nil || *result[0].NullableS != "hello" {
			t.Fatalf("expected 'hello': %+v", result[0])
		}
	})
}

func TestScan_NullIntoNonPointer(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	// int64 cannot represent NULL — pgx must return a scan error.
	// This is important: if someone types a field wrong, they should
	// get a clear error, not a silent zero.
	type badNullable struct {
		Val int64 `db:"val"`
	}
	scanner := NewScanner[badNullable]()

	rows, _ := conn.Query(ctx, `SELECT NULL::bigint AS val`)
	_, err := pgx.CollectRows(rows, scanner.Scan)
	if err == nil {
		t.Fatal("expected error scanning NULL into non-pointer int64")
	}
}

func TestScan_TypeCoercions(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	// Mirrors the mix of types in pg_stat_database.
	type pgTypes struct {
		SmallInt int32   `db:"i32"`
		BigInt   int64   `db:"i64"`
		Double   float64 `db:"f64"`
		Text     string  `db:"txt"`
		Flag     bool    `db:"flag"`
	}
	scanner := NewScanner[pgTypes]()

	rows, _ := conn.Query(ctx,
		`SELECT 1::int AS i32, 9999999999::bigint AS i64, 3.14::float8 AS f64, 'hello'::text AS txt, true AS flag`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := result[0]
	if r.SmallInt != 1 || r.BigInt != 9999999999 || r.Double != 3.14 || r.Text != "hello" || r.Flag != true {
		t.Fatalf("type coercion failed: %+v", r)
	}
}

func TestScan_BoolColumn(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type withBool struct {
		Name   string `db:"name"`
		Active bool   `db:"active"`
	}
	scanner := NewScanner[withBool]()

	t.Run("bool present true", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'a' AS name, true AS active`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result[0].Active {
			t.Fatal("expected true")
		}
	})

	t.Run("bool present false", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'b' AS name, false AS active`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Active {
			t.Fatal("expected false")
		}
	})

	t.Run("bool column absent", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'c' AS name`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].Active {
			t.Fatal("missing bool should default to false")
		}
	})
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 5: Version transitions
// ═════════════════════════════════════════════════════════════════════

func TestScan_Upgrade(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
		D string `db:"d"`
	}
	scanner := NewScanner[versioned]()

	// PG N: columns a, b
	rows1, _ := conn.Query(ctx, `SELECT 'v1' AS a, 1 AS b`)
	r1, err := pgx.CollectRows(rows1, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if r1[0].A != "v1" || r1[0].B != 1 || r1[0].C != 0 || r1[0].D != "" {
		t.Fatalf("phase 1 unexpected: %+v", r1[0])
	}

	// PG N+1: columns a, b, c
	rows2, _ := conn.Query(ctx, `SELECT 'v2' AS a, 2 AS b, 42 AS c`)
	r2, err := pgx.CollectRows(rows2, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if r2[0].A != "v2" || r2[0].B != 2 || r2[0].C != 42 || r2[0].D != "" {
		t.Fatalf("phase 2 unexpected: %+v", r2[0])
	}

	// PG N+2: columns a, b, c, d, plus unknown e
	rows3, _ := conn.Query(ctx, `SELECT 'v3' AS a, 3 AS b, 99 AS c, 'new' AS d, true AS e`)
	r3, err := pgx.CollectRows(rows3, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 3: %v", err)
	}
	if r3[0].A != "v3" || r3[0].B != 3 || r3[0].C != 99 || r3[0].D != "new" {
		t.Fatalf("phase 3 unexpected: %+v", r3[0])
	}
}

func TestScan_Downgrade(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
		D string `db:"d"`
	}
	scanner := NewScanner[versioned]()

	// PG N+2: all columns present
	rows1, _ := conn.Query(ctx, `SELECT 'wide' AS a, 10 AS b, 77 AS c, 'full' AS d`)
	r1, err := pgx.CollectRows(rows1, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if r1[0].A != "wide" || r1[0].B != 10 || r1[0].C != 77 || r1[0].D != "full" {
		t.Fatalf("phase 1 unexpected: %+v", r1[0])
	}

	// PG N: only columns a, b (failover)
	rows2, _ := conn.Query(ctx, `SELECT 'narrow' AS a, 5 AS b`)
	r2, err := pgx.CollectRows(rows2, scanner.Scan)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if r2[0].A != "narrow" || r2[0].B != 5 {
		t.Fatalf("phase 2 values wrong: %+v", r2[0])
	}
	if r2[0].C != 0 || r2[0].D != "" {
		t.Fatalf("phase 2: missing fields should be zero, got C=%d D=%q", r2[0].C, r2[0].D)
	}
}

func TestScan_UpgradeDowngradeCycle(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type versioned struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
	}
	scanner := NewScanner[versioned]()

	run := func(phase string, query string, wantA string, wantB int32, wantC int64) {
		t.Helper()
		rows, _ := conn.Query(ctx, query)
		results, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if len(results) != 1 {
			t.Fatalf("%s: expected 1 row, got %d", phase, len(results))
		}
		r := results[0]
		if r.A != wantA || r.B != wantB || r.C != wantC {
			t.Fatalf("%s: got A=%q B=%d C=%d, want A=%q B=%d C=%d",
				phase, r.A, r.B, r.C, wantA, wantB, wantC)
		}
	}

	run("old-1", `SELECT 'a1' AS a, 1 AS b`, "a1", 1, 0)
	run("new-1", `SELECT 'a2' AS a, 2 AS b, 20 AS c`, "a2", 2, 20)
	run("old-2", `SELECT 'a3' AS a, 3 AS b`, "a3", 3, 0)
	run("new-2", `SELECT 'a4' AS a, 4 AS b, 40 AS c`, "a4", 4, 40)
}

func TestScan_ColumnOrderChange(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type ordered struct {
		X string `db:"x"`
		Y int32  `db:"y"`
		Z int64  `db:"z"`
	}
	scanner := NewScanner[ordered]()

	t.Run("order x,y,z", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 'a' AS x, 1 AS y, 10 AS z`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "a" || result[0].Y != 1 || result[0].Z != 10 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})

	t.Run("order z,x,y", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 20 AS z, 'b' AS x, 2 AS y`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "b" || result[0].Y != 2 || result[0].Z != 20 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})

	t.Run("order y,z,x", func(t *testing.T) {
		rows, _ := conn.Query(ctx, `SELECT 3 AS y, 30 AS z, 'c' AS x`)
		result, err := pgx.CollectRows(rows, scanner.Scan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0].X != "c" || result[0].Y != 3 || result[0].Z != 30 {
			t.Fatalf("unexpected: %+v", result[0])
		}
	})
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 6: Multi-row and empty results
// ═════════════════════════════════════════════════════════════════════

func TestScan_EmptyResultSet(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		N int32 `db:"n"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx, `SELECT 1 AS n WHERE false`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %d rows", len(result))
	}
}

func TestScan_MultipleRows(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		N     int32  `db:"n"`
		Label string `db:"label"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx,
		`SELECT n, 'row_' || n::text AS label FROM generate_series(1, 5) AS n`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}
	for i, r := range result {
		expectedN := int32(i + 1)
		expectedLabel := "row_" + fmt.Sprintf("%d", expectedN)
		if r.N != expectedN || r.Label != expectedLabel {
			t.Fatalf("row %d: got N=%d Label=%q, want N=%d Label=%q",
				i, r.N, r.Label, expectedN, expectedLabel)
		}
	}
}

func TestScan_MultipleRowsWithNulls(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	type row struct {
		ID   int32   `db:"id"`
		Name *string `db:"name"`
	}
	scanner := NewScanner[row]()

	rows, _ := conn.Query(ctx, `
		SELECT 1 AS id, 'alice'::text AS name
		UNION ALL
		SELECT 2 AS id, NULL::text AS name
		UNION ALL
		SELECT 3 AS id, 'charlie'::text AS name
	`)
	result, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// Row 1: non-null
	if result[0].ID != 1 || result[0].Name == nil || *result[0].Name != "alice" {
		t.Fatalf("row 0 unexpected: %+v", result[0])
	}
	// Row 2: null
	if result[1].ID != 2 || result[1].Name != nil {
		t.Fatalf("row 1 should have nil Name: %+v", result[1])
	}
	// Row 3: non-null
	if result[2].ID != 3 || result[2].Name == nil || *result[2].Name != "charlie" {
		t.Fatalf("row 2 unexpected: %+v", result[2])
	}
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 7: Concurrency
// ═════════════════════════════════════════════════════════════════════

func TestScan_ConcurrentSameLayout(t *testing.T) {
	ctx := context.Background()

	// Verify we can connect at all before spawning goroutines.
	probe := connectForTest(t, ctx)
	probe.Close(ctx)

	type row struct {
		N int64 `db:"n"`
	}
	scanner := NewScanner[row]()

	// Pre-warm the cache so the test focuses on concurrent read path.
	warm := connectForTest(t, ctx)
	warmRows, _ := warm.Query(ctx, `SELECT 0::bigint AS n`)
	pgx.CollectRows(warmRows, scanner.Scan)
	warm.Close(ctx)

	// pgx.Conn is NOT goroutine-safe, so each goroutine gets its own connection.
	// The Scanner is what we're testing for concurrency — its sync.Map cache.
	const goroutines = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, testConnString())
			if err != nil {
				errs <- fmt.Errorf("goroutine %d connect: %w", i, err)
				return
			}
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, `SELECT $1::bigint AS n`, int64(i))
			if err != nil {
				errs <- fmt.Errorf("goroutine %d query: %w", i, err)
				return
			}
			result, err := pgx.CollectRows(rows, scanner.Scan)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d collect: %w", i, err)
				return
			}
			if len(result) != 1 || result[0].N != int64(i) {
				errs <- fmt.Errorf("goroutine %d: got %+v, want N=%d", i, result, i)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestScan_ConcurrentDifferentLayouts(t *testing.T) {
	ctx := context.Background()

	// Verify we can connect at all before spawning goroutines.
	probe := connectForTest(t, ctx)
	probe.Close(ctx)

	type row struct {
		A string `db:"a"`
		B int32  `db:"b"`
		C int64  `db:"c"`
	}
	scanner := NewScanner[row]()

	// Different goroutines hit different "PG versions" (column layouts),
	// racing to populate different cache entries.
	queries := []struct {
		sql   string
		wantA string
		wantB int32
		wantC int64
	}{
		{`SELECT 'v1' AS a, 1 AS b`, "v1", 1, 0},                          // "PG 12" layout
		{`SELECT 'v2' AS a, 2 AS b, 20 AS c`, "v2", 2, 20},                // "PG 14" layout
		{`SELECT 'v3' AS a, 3 AS b, 30 AS c, true AS extra`, "v3", 3, 30}, // "PG 17" layout
	}

	const iterations = 10
	var wg sync.WaitGroup
	errs := make(chan error, len(queries)*iterations)

	for _, q := range queries {
		for j := 0; j < iterations; j++ {
			wg.Add(1)
			go func(sql, wantA string, wantB int32, wantC int64) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, testConnString())
				if err != nil {
					errs <- fmt.Errorf("connect for %q: %w", sql, err)
					return
				}
				defer conn.Close(ctx)

				rows, err := conn.Query(ctx, sql)
				if err != nil {
					errs <- fmt.Errorf("query %q: %w", sql, err)
					return
				}
				result, err := pgx.CollectRows(rows, scanner.Scan)
				if err != nil {
					errs <- fmt.Errorf("collect %q: %w", sql, err)
					return
				}
				if len(result) != 1 {
					errs <- fmt.Errorf("%q: expected 1 row", sql)
					return
				}
				r := result[0]
				if r.A != wantA || r.B != wantB || r.C != wantC {
					errs <- fmt.Errorf("%q: got %+v, want A=%q B=%d C=%d",
						sql, r, wantA, wantB, wantC)
				}
			}(q.sql, q.wantA, q.wantB, q.wantC)
		}
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 8: Scanner construction
// ═════════════════════════════════════════════════════════════════════

func TestNewScanner_PanicsOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for non-struct type")
		}
	}()
	NewScanner[int]()
}

// ═════════════════════════════════════════════════════════════════════
// GROUP 9: Integration — real system views
// ═════════════════════════════════════════════════════════════════════

func TestScan_PgStatDatabase(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	results, err := QueryPgStatDatabase(ctx, conn)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one database")
	}
	found := false
	for _, r := range results {
		if r.Datname != "" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected at least one row with a non-empty datname")
	}
}

func TestScan_PgStatActivity(t *testing.T) {
	ctx := context.Background()
	conn := connectForTest(t, ctx)
	defer conn.Close(ctx)

	// pg_stat_activity is another system view that varies across PG versions.
	// We define a subset struct and verify SELECT * works with our scanner.
	type PgStatActivity struct {
		Datname     *string `db:"datname"`
		Pid         int32   `db:"pid"`
		State       *string `db:"state"`
		BackendType *string `db:"backend_type"` // PG 10+
		QueryID     *int64  `db:"query_id"`     // PG 14+
	}
	scanner := NewScanner[PgStatActivity]()

	rows, err := conn.Query(ctx, "SELECT * FROM pg_stat_activity LIMIT 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	results, err := pgx.CollectRows(rows, scanner.Scan)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	// We should see at least our own connection.
	if len(results) == 0 {
		t.Fatal("expected at least one row from pg_stat_activity")
	}
	foundSelf := false
	for _, r := range results {
		if r.Pid > 0 {
			foundSelf = true
			break
		}
	}
	if !foundSelf {
		t.Fatal("expected at least one row with a valid pid")
	}
}

// ═════════════════════════════════════════════════════════════════════
// Test helpers
// ═════════════════════════════════════════════════════════════════════

// testConnString returns the PG connection string for tests.
// Override with PGXUTIL_TEST_CONN_STRING env var if needed.
func testConnString() string {
	if s := os.Getenv("PGXUTIL_TEST_CONN_STRING"); s != "" {
		return s
	}
	return "postgres://localhost:5432/postgres"
}

func connectForTest(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, testConnString())
	if err != nil {
		t.Skipf("skipping: cannot connect to test database: %v", err)
	}
	return conn
}
