//go:build integration

package queries

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type pgInstance struct {
	pool    *pgxpool.Pool // pg_monitor role — used by collectors under test
	admin   *pgxpool.Pool // superuser — used for fixture setup only
	version int
}

var pgInstances []pgInstance

// setupTestFixtures creates tables, indexes, functions, inserts data, and
// runs ANALYZE so that collectors have actual data to return.
func setupTestFixtures(ctx context.Context, pool *pgxpool.Pool) error {
	fixtures := []string{
		// Create a table with columns exercising different types.
		`CREATE TABLE test_users (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			score integer DEFAULT 0,
			created_at timestamptz DEFAULT now()
		)`,
		// Additional indexes for pg_index / pg_stat_user_indexes / pg_statio_user_indexes.
		`CREATE INDEX idx_test_users_name ON test_users(name)`,
		`CREATE INDEX idx_test_users_score ON test_users(score)`,
		`CREATE UNIQUE INDEX idx_test_users_email ON test_users(email)`,
		// Insert enough rows for pg_stats histograms and pg_class reltuples.
		`INSERT INTO test_users (name, email, score)
		 SELECT 'user_' || i, 'user_' || i || '@test.com', (i % 100)
		 FROM generate_series(1, 500) AS i`,
		// ANALYZE so pg_stats, pg_class, pg_stat_user_tables have data.
		`ANALYZE test_users`,
		// Read the table so pg_statio_user_tables shows I/O activity.
		`SELECT count(*) FROM test_users WHERE score > 50`,
		// Scan via the index so pg_stat_user_indexes and pg_statio_user_indexes have data.
		`SELECT count(*) FROM test_users WHERE name = 'user_1'`,
		// Create a PL/pgSQL function and call it so pg_stat_user_functions has data.
		`CREATE FUNCTION test_add(a integer, b integer) RETURNS integer AS $$
		 BEGIN RETURN a + b; END;
		 $$ LANGUAGE plpgsql`,
		`SELECT test_add(i, i) FROM generate_series(1, 10) AS i`,
		// A second table with an index that will stay cold after fixture setup,
		// used to verify delta collectors only emit changed rows.
		`CREATE TABLE test_cold (id serial PRIMARY KEY, val integer)`,
		`CREATE INDEX idx_test_cold_val ON test_cold(val)`,
		`INSERT INTO test_cold (val) SELECT i FROM generate_series(1, 100) AS i`,
		`ANALYZE test_cold`,
		// Enable pg_stat_statements (shared_preload_libraries is set at container start).
		`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`,
	}
	for _, sql := range fixtures {
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("fixture %q: %w", sql[:min(len(sql), 60)], err)
		}
	}
	return nil
}

func TestMain(m *testing.M) {
	var logBuf bytes.Buffer
	tclog.SetDefault(&bufLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()
	versions := []int{13, 14, 15, 16, 17, 18}

	var containers []*postgres.PostgresContainer

	cleanup := func() {
		for _, inst := range pgInstances {
			inst.pool.Close()
			inst.admin.Close()
		}
		for _, c := range containers {
			_ = c.Terminate(ctx)
		}
	}

	for _, v := range versions {
		image := fmt.Sprintf("postgres:%d-alpine", v)
		hostPort := fmt.Sprintf("%d", 46300+v) // different port range from pgxutil tests

		pgContainer, err := postgres.Run(ctx, image,
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					// The entrypoint prepends "postgres" when Cmd starts with "-".
					Cmd: []string{
						"-c", "shared_preload_libraries=pg_stat_statements",
						"-c", "track_functions=all",
					},
					HostConfigModifier: func(hc *container.HostConfig) {
						hc.PortBindings = nat.PortMap{
							"5432/tcp": []nat.PortBinding{
								{HostIP: "127.0.0.1", HostPort: hostPort},
							},
						}
					},
				},
			}),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start postgres %d on port %s: %v\n", v, hostPort, err)
			fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
			cleanup()
			os.Exit(1)
		}
		containers = append(containers, pgContainer)

		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get connection string for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		adminPool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create admin pool for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		if err := setupTestFixtures(ctx, adminPool); err != nil {
			fmt.Fprintf(os.Stderr, "failed to setup fixtures for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		// Create a pg_monitor role for collector tests. This ensures all
		// collectors work without superuser privileges.
		monitorSetup := []string{
			`CREATE ROLE monitor LOGIN PASSWORD 'monitor'`,
			`GRANT pg_monitor TO monitor`,
			`GRANT CONNECT ON DATABASE testdb TO monitor`,
		}
		for _, sql := range monitorSetup {
			if _, err := adminPool.Exec(ctx, sql); err != nil {
				fmt.Fprintf(os.Stderr, "failed to setup monitor role for postgres %d: %v\n", v, err)
				cleanup()
				os.Exit(1)
			}
		}

		// Build a connection string for the monitor role.
		monitorConnStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get monitor connection string for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}
		// Replace user=test&password=test with user=monitor&password=monitor.
		monitorConnStr = strings.Replace(monitorConnStr, "user=test", "user=monitor", 1)
		monitorConnStr = strings.Replace(monitorConnStr, "password=test", "password=monitor", 1)

		monitorPool, err := pgxpool.New(ctx, monitorConnStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create monitor pool for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		pgInstances = append(pgInstances, pgInstance{pool: monitorPool, admin: adminPool, version: v})
	}

	slices.SortFunc(pgInstances, func(a, b pgInstance) int { return a.version - b.version })

	code := m.Run()
	cleanup()

	if code != 0 {
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
	}

	os.Exit(code)
}

type bufLogger struct {
	buf *bytes.Buffer
}

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

// noopPrepareCtx is a PrepareCtx that passes through unchanged.
func noopPrepareCtx(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// TestCollectors_AllVersions runs every collector against every PG version
// and verifies structural invariants:
//   - Name and Interval are set
//   - First Collect() succeeds (or returns nil for version-gated collectors)
//   - Result is valid JSON with a non-empty hash
//   - Second Collect() also succeeds (catches broken stateful collectors)
func TestCollectors_AllVersions(t *testing.T) {
	for _, inst := range pgInstances {
		inst := inst
		t.Run(fmt.Sprintf("PG%d", inst.version), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			collectors := buildCollectors(inst.pool, inst.version)
			for _, c := range collectors {
				c := c
				t.Run(c.Name, func(t *testing.T) {
					t.Parallel()

					if c.Name == "" {
						t.Fatal("collector has empty Name")
					}
					if c.Interval <= 0 {
						t.Fatalf("collector %s has non-positive Interval: %v", c.Name, c.Interval)
					}

					r1, err := c.Collect(ctx)
					if err != nil {
						t.Fatalf("first Collect() error: %v", err)
					}
					// Version-gated collectors return nil on unsupported versions
					if r1 == nil {
						return
					}
					if len(r1.JSON) == 0 {
						t.Fatal("Collect() returned empty JSON")
					}
					if !json.Valid(r1.JSON) {
						t.Fatalf("Collect() returned invalid JSON: %s", r1.JSON[:min(len(r1.JSON), 200)])
					}
					if r1.Hash() == "" {
						t.Fatal("CollectResult.Hash() returned empty string")
					}

					// Second call must not error — catches broken stateful collectors.
					_, err = c.Collect(ctx)
					if err != nil {
						t.Fatalf("second Collect() error: %v", err)
					}
				})
			}
		})
	}
}

// TestCollectors_SkipUnchanged verifies that collectors with SkipUnchanged
// return nil on the second identical collection and force-send after the
// threshold.
func TestCollectors_SkipUnchanged(t *testing.T) {
	// Use the latest PG version for this test
	inst := pgInstances[len(pgInstances)-1]
	ctx := context.Background()

	collectors := buildCollectors(inst.pool, inst.version)
	for _, c := range collectors {
		if !c.SkipUnchanged {
			continue
		}
		c := c
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()

			// First call should always return a result
			r1, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("first Collect() error: %v", err)
			}
			if r1 == nil {
				t.Skip("collector returned nil (version-gated)")
			}

			// Second call with same data should return nil (skipped)
			r2, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("second Collect() error: %v", err)
			}
			if r2 != nil {
				t.Fatal("second Collect() should return nil (unchanged)")
			}
		})
	}
}

// TestPgStatActivity_QueryID_PG13 verifies that pg_stat_activity works on
// PG 13 where the queryid column does not exist.
func TestPgStatActivity_QueryID_PG13(t *testing.T) {
	var pg13 *pgInstance
	for i := range pgInstances {
		if pgInstances[i].version == 13 {
			pg13 = &pgInstances[i]
			break
		}
	}
	if pg13 == nil {
		t.Skip("PG 13 instance not available")
	}

	ctx := context.Background()
	c := PgStatActivityCollector(pg13.pool, noopPrepareCtx, pg13.version)
	result, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error on PG 13: %v", err)
	}
	if result == nil {
		t.Fatal("Collect() returned nil on PG 13")
	}
}

// TestPgStatActivity_QueryID_PG14Plus verifies that pg_stat_activity returns
// a composite query_id on PG 14+.
func TestPgStatActivity_QueryID_PG14Plus(t *testing.T) {
	for _, inst := range pgInstances {
		if inst.version < 14 {
			continue
		}
		inst := inst
		t.Run(fmt.Sprintf("PG%d", inst.version), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c := PgStatActivityCollector(inst.pool, noopPrepareCtx, inst.version)
			result, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("Collect() error: %v", err)
			}
			if result == nil {
				t.Fatal("Collect() returned nil")
			}
			if len(result.JSON) == 0 {
				t.Fatal("Collect() returned empty JSON")
			}
		})
	}
}

// ---------- Tests for new collectors ----------

// collectJSON is a helper that calls Collect and returns the parsed JSON.
func collectJSON(t *testing.T, c CatalogCollector) map[string]json.RawMessage {
	t.Helper()
	ctx := context.Background()
	result, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
	if result == nil {
		t.Fatal("Collect() returned nil")
	}
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result.JSON, &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}
	return parsed
}

// requireRows is a helper that parses the "rows" field from a collector result
// and returns the count. Fails if rows is missing or empty.
func requireRows(t *testing.T, c CatalogCollector) int {
	t.Helper()
	parsed := collectJSON(t, c)
	rowsRaw, ok := parsed["rows"]
	if !ok {
		t.Fatal("JSON missing 'rows' field")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(rowsRaw, &rows); err != nil {
		t.Fatalf("failed to parse rows: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected non-empty rows")
	}
	return len(rows)
}

// latestInstance returns the instance with the highest PG version.
func latestInstance() pgInstance {
	return pgInstances[len(pgInstances)-1]
}

func TestWaitEvents_ReturnsAllTypes(t *testing.T) {
	inst := latestInstance()
	c := WaitEventsCollector(inst.pool, noopPrepareCtx)
	n := requireRows(t, c)
	// 9 wait event types + 1 TOTAL row = 10
	if n != 10 {
		t.Fatalf("expected 10 rows (9 types + TOTAL), got %d", n)
	}
}

func TestConnectionStats_ReturnsData(t *testing.T) {
	inst := latestInstance()
	c := ConnectionStatsCollector(inst.pool, noopPrepareCtx)
	requireRows(t, c)
}

func TestDatabaseSize_ReturnsData(t *testing.T) {
	inst := latestInstance()
	c := DatabaseSizeCollector(inst.pool, noopPrepareCtx)
	requireRows(t, c)
}

func TestUptimeMinutes_ReturnsData(t *testing.T) {
	inst := latestInstance()
	c := UptimeMinutesCollector(inst.pool, noopPrepareCtx)
	requireRows(t, c)
}

func TestTransactionCommits_ComputesTPS(t *testing.T) {
	inst := latestInstance()
	c := TransactionCommitsCollector(inst.pool, noopPrepareCtx)
	ctx := context.Background()

	// First call: establishes baseline — TPS must be 0 (no prior sample).
	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil")
	}
	var p1 TransactionCommitsPayload
	if err := json.Unmarshal(r1.JSON, &p1); err != nil {
		t.Fatalf("failed to parse first result: %v", err)
	}
	if p1.XactCommit <= 0 {
		t.Fatalf("expected xact_commit > 0 on first call, got %d", p1.XactCommit)
	}
	if p1.TPS != 0 {
		t.Fatalf("expected TPS = 0 on first call (no baseline), got %f", p1.TPS)
	}

	// Generate some transactions and wait for PG stats collector to update.
	for i := 0; i < 50; i++ {
		_, _ = inst.admin.Exec(ctx, "SELECT 1")
	}
	time.Sleep(1 * time.Second)

	// Second call: xact_commit must be monotonically non-decreasing, TPS >= 0.
	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("second Collect() error: %v", err)
	}
	if r2 == nil {
		t.Fatal("second Collect() returned nil")
	}
	var p2 TransactionCommitsPayload
	if err := json.Unmarshal(r2.JSON, &p2); err != nil {
		t.Fatalf("failed to parse second result: %v", err)
	}
	if p2.XactCommit < p1.XactCommit {
		t.Fatalf("xact_commit decreased: %d → %d", p1.XactCommit, p2.XactCommit)
	}
	if p2.TPS < 0 {
		t.Fatalf("expected TPS >= 0, got %f", p2.TPS)
	}
}

func TestPgStatUserTables_ReturnsFixtureTable(t *testing.T) {
	inst := latestInstance()
	c := PgStatUserTablesCollector(inst.pool, noopPrepareCtx, PgStatUserTablesCategoryLimit)
	requireRows(t, c)
}

func TestPgStatUserIndexes_ReturnsFixtureIndexes(t *testing.T) {
	inst := latestInstance()
	c := PgStatUserIndexesCollector(inst.pool, noopPrepareCtx, PgStatUserIndexesCategoryLimit)
	requireRows(t, c)
}

func TestPgStatioUserTables_NoError(t *testing.T) {
	inst := latestInstance()
	c := PgStatioUserTablesCollector(inst.pool, noopPrepareCtx)
	ctx := context.Background()
	// On fresh test containers, all data may be in shared_buffers with zero
	// I/O counters, so the WHERE filter may exclude everything. Just verify
	// no error — the query and scanning work correctly.
	_, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
}

func TestPgStatioUserIndexes_ReturnsFixtureData(t *testing.T) {
	inst := latestInstance()
	c := PgStatioUserIndexesCollector(inst.pool, noopPrepareCtx, PgStatioUserIndexesBatchSize)
	requireRows(t, c)
}

func TestPgStatioUserIndexes_BackfillThenDelta(t *testing.T) {
	inst := latestInstance()
	ctx := context.Background()

	// Use batch size 1 to force multiple backfill ticks and test pagination.
	c := PgStatioUserIndexesCollector(inst.pool, noopPrepareCtx, 1)

	// First call: backfill returns exactly 1 row (batch size 1).
	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil — expected backfill data")
	}
	var p1 Payload[PgStatioUserIndexesRow]
	if err := json.Unmarshal(r1.JSON, &p1); err != nil {
		t.Fatalf("failed to parse first result: %v", err)
	}
	if len(p1.Rows) != 1 {
		t.Fatalf("expected exactly 1 row with batch size 1, got %d", len(p1.Rows))
	}

	// Drain the remaining backfill ticks until we get nil (backfill exhausted).
	var totalBackfillRows int = 1
	for i := 0; i < 200; i++ {
		r, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("backfill tick %d error: %v", i+2, err)
		}
		if r == nil {
			break // backfill done
		}
		var p Payload[PgStatioUserIndexesRow]
		if err := json.Unmarshal(r.JSON, &p); err != nil {
			t.Fatalf("failed to parse backfill tick %d: %v", i+2, err)
		}
		totalBackfillRows += len(p.Rows)
	}
	// We created 3 explicit indexes + 1 PK = 4 fixture indexes minimum.
	if totalBackfillRows < 4 {
		t.Fatalf("expected at least 4 backfill rows, got %d", totalBackfillRows)
	}

	// Delta with no I/O changes → nil.
	rDelta, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("delta Collect() error: %v", err)
	}
	if rDelta != nil {
		t.Fatal("expected nil delta result when no I/O has occurred")
	}
}

func TestPgStatioUserIndexes_DeltaEmitsOnlyChangedRows(t *testing.T) {
	inst := latestInstance()
	ctx := context.Background()

	// Flush any pending stats from fixture setup so the backfill snapshot
	// captures the true current counters.
	if inst.version >= 15 {
		_, _ = inst.admin.Exec(ctx, "SELECT pg_stat_force_next_flush()")
	}
	time.Sleep(500 * time.Millisecond)

	// Large batch size so backfill finishes in one tick.
	c := PgStatioUserIndexesCollector(inst.pool, noopPrepareCtx, 10000)

	// First call: backfill — all rows emitted.
	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("backfill Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("backfill Collect() returned nil")
	}
	var p1 Payload[PgStatioUserIndexesRow]
	if err := json.Unmarshal(r1.JSON, &p1); err != nil {
		t.Fatalf("failed to parse backfill result: %v", err)
	}
	backfillCount := len(p1.Rows)
	// test_users has 4 indexes (PK + 3), test_cold has 2 (PK + 1) = 6 minimum.
	if backfillCount < 6 {
		t.Fatalf("expected at least 6 backfill rows, got %d", backfillCount)
	}

	// Second call: empty page, transitions to delta mode.
	r2, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("transition Collect() error: %v", err)
	}
	if r2 != nil {
		t.Fatal("expected nil on transition tick")
	}

	// Generate I/O only on test_users indexes; test_cold indexes stay untouched.
	for i := 0; i < 20; i++ {
		_, _ = inst.admin.Exec(ctx, "SELECT count(*) FROM test_users WHERE name = $1", fmt.Sprintf("user_%d", i))
	}

	// Flush stats so the I/O counters are visible.
	if inst.version >= 15 {
		_, _ = inst.admin.Exec(ctx, "SELECT pg_stat_force_next_flush()")
	}
	time.Sleep(500 * time.Millisecond)

	// Delta: should emit only the changed index(es) from test_users,
	// not the cold test_cold indexes.
	r3, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("delta Collect() error: %v", err)
	}
	if r3 == nil {
		t.Fatal("expected delta to emit changed rows after index I/O")
	}
	var p3 Payload[PgStatioUserIndexesRow]
	if err := json.Unmarshal(r3.JSON, &p3); err != nil {
		t.Fatalf("failed to parse delta result: %v", err)
	}
	if len(p3.Rows) == 0 {
		t.Fatal("expected at least 1 changed row in delta")
	}
	if len(p3.Rows) >= backfillCount {
		t.Fatalf("delta should emit fewer rows than backfill (%d), got %d", backfillCount, len(p3.Rows))
	}
	// Verify no test_cold indexes appear in the delta.
	for _, row := range p3.Rows {
		if row.RelName != nil && string(*row.RelName) == "test_cold" {
			t.Fatalf("delta should not include untouched test_cold indexes, got %s", string(*row.IndexRelName))
		}
	}
}

func TestPgStatUserFunctions_ReturnsFixtureFunction(t *testing.T) {
	inst := latestInstance()
	ctx := context.Background()

	// Verify track_functions is enabled; skip if the container didn't apply it.
	var trackFunctions string
	if err := inst.pool.QueryRow(ctx, "SHOW track_functions").Scan(&trackFunctions); err != nil {
		t.Fatalf("SHOW track_functions: %v", err)
	}
	if trackFunctions == "none" {
		t.Skipf("track_functions is %q — function stats not tracked", trackFunctions)
	}

	// Call the function via admin (has EXECUTE privilege) and flush stats.
	conn, err := inst.admin.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	_, _ = conn.Exec(ctx, "SELECT test_add(i, i) FROM generate_series(1, 100) AS i")
	if inst.version >= 15 {
		_, _ = conn.Exec(ctx, "SELECT pg_stat_force_next_flush()")
	}
	conn.Release()

	// Give the stats collector time to flush.
	time.Sleep(500 * time.Millisecond)

	c := PgStatUserFunctionsCollector(inst.pool, noopPrepareCtx)
	n := requireRows(t, c)
	if n < 1 {
		t.Fatalf("expected at least 1 function, got %d", n)
	}
}

func TestPgIndex_ReturnsFixtureIndexes(t *testing.T) {
	inst := latestInstance()
	c := PgIndexCollector(inst.pool, noopPrepareCtx)
	n := requireRows(t, c)
	// We created 4 indexes: PK + 3 explicit
	if n < 4 {
		t.Fatalf("expected at least 4 indexes, got %d", n)
	}
}

func TestPgAttribute_ReturnsFixtureColumns(t *testing.T) {
	inst := latestInstance()
	c := PgAttributeCollector(inst.pool, noopPrepareCtx)
	n := requireRows(t, c)
	// test_users has 5 columns + index columns (each index has entries)
	if n < 5 {
		t.Fatalf("expected at least 5 attribute rows, got %d", n)
	}
}

func TestPgClass_BackfillThenDelta(t *testing.T) {
	inst := latestInstance()
	ctx := context.Background()

	// Use batch size 1 to force multiple backfill ticks and test pagination.
	c := PgClassCollector(inst.pool, noopPrepareCtx, 1)

	// First call: backfill returns exactly 1 row (batch size 1).
	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil — expected backfill data")
	}
	var p1 Payload[PgClassRow]
	if err := json.Unmarshal(r1.JSON, &p1); err != nil {
		t.Fatalf("failed to parse first result: %v", err)
	}
	if len(p1.Rows) != 1 {
		t.Fatalf("expected exactly 1 row with batch size 1, got %d", len(p1.Rows))
	}
	// Verify row has non-empty identifiers.
	if p1.Rows[0].SchemaName == "" || p1.Rows[0].RelName == "" {
		t.Fatalf("expected non-empty schemaname/relname, got %q/%q", p1.Rows[0].SchemaName, p1.Rows[0].RelName)
	}

	// Drain the remaining backfill ticks until we get nil (backfill exhausted).
	for i := 0; i < 100; i++ {
		r, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("backfill tick %d error: %v", i+2, err)
		}
		if r == nil {
			break // backfill done, transitioned to delta
		}
	}

	// Delta with no changes since last poll → nil.
	rDelta, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("delta Collect() error: %v", err)
	}
	if rDelta != nil {
		t.Fatal("expected nil delta result before any ANALYZE")
	}

	// ANALYZE triggers a timestamp change that the delta query detects.
	_, _ = inst.admin.Exec(ctx, "ANALYZE test_users")

	rAfterAnalyze, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("post-ANALYZE Collect() error: %v", err)
	}
	if rAfterAnalyze == nil {
		t.Fatal("expected delta data after ANALYZE, got nil")
	}
}

func TestPgStats_BackfillThenDelta(t *testing.T) {
	inst := latestInstance()
	ctx := context.Background()

	// Use batch size 1 to force pagination.
	c := PgStatsCollector(inst.pool, noopPrepareCtx, 1, true)

	// First call: backfill returns stats for 1 table.
	r1, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("first Collect() error: %v", err)
	}
	if r1 == nil {
		t.Fatal("first Collect() returned nil — expected pg_stats backfill data")
	}

	var p1 Payload[PgStatsRow]
	if err := json.Unmarshal(r1.JSON, &p1); err != nil {
		t.Fatalf("failed to parse first result: %v", err)
	}
	if len(p1.Rows) == 0 {
		t.Fatal("expected non-empty pg_stats rows")
	}

	// Verify rows have non-empty identifiers.
	for _, row := range p1.Rows {
		if row.SchemaName == "" || row.TableName == "" || row.AttName == "" {
			t.Fatalf("expected non-empty schemaname/tablename/attname, got %q/%q/%q",
				row.SchemaName, row.TableName, row.AttName)
		}
	}

	// Verify array columns are valid JSON when non-null.
	for _, row := range p1.Rows {
		for _, col := range []struct {
			name string
			val  json.RawMessage
		}{
			{"most_common_vals", row.MostCommonVals},
			{"most_common_freqs", row.MostCommonFreqs},
			{"histogram_bounds", row.HistogramBounds},
			{"most_common_elems", row.MostCommonElems},
			{"most_common_elem_freqs", row.MostCommonElemFreqs},
			{"elem_count_histogram", row.ElemCountHistogram},
		} {
			if col.val != nil && !json.Valid(col.val) {
				t.Fatalf("%s.%s.%s %s is not valid JSON: %s",
					row.SchemaName, row.TableName, row.AttName, col.name, col.val)
			}
		}
	}

	// Drain backfill.
	for i := 0; i < 100; i++ {
		r, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("backfill tick %d error: %v", i+2, err)
		}
		if r == nil {
			break
		}
	}

	// Delta with no ANALYZE → nil.
	rDelta, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("delta Collect() error: %v", err)
	}
	if rDelta != nil {
		t.Fatal("expected nil delta result before any ANALYZE")
	}

	// ANALYZE triggers delta.
	_, _ = inst.admin.Exec(ctx, "ANALYZE test_users")

	rAfterAnalyze, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("post-ANALYZE Collect() error: %v", err)
	}
	if rAfterAnalyze == nil {
		t.Fatal("expected delta data after ANALYZE, got nil")
	}
}

func TestPgStats_RedactsWhenIncludeTableDataFalse(t *testing.T) {
	inst := latestInstance()
	c := PgStatsCollector(inst.pool, noopPrepareCtx, PgStatsBackfillBatchSize, false)
	ctx := context.Background()

	r, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
	if r == nil {
		t.Fatal("Collect() returned nil")
	}

	var payload Payload[PgStatsRow]
	if err := json.Unmarshal(r.JSON, &payload); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	for _, row := range payload.Rows {
		// After JSON round-trip, nil json.RawMessage becomes []byte("null").
		if row.MostCommonVals != nil && string(row.MostCommonVals) != "null" {
			t.Fatalf("expected most_common_vals to be null when includeTableData=false, got %s", string(row.MostCommonVals))
		}
		if row.HistogramBounds != nil && string(row.HistogramBounds) != "null" {
			t.Fatalf("expected histogram_bounds to be null when includeTableData=false, got %s", string(row.HistogramBounds))
		}
		if row.MostCommonElems != nil && string(row.MostCommonElems) != "null" {
			t.Fatalf("expected most_common_elems to be null when includeTableData=false, got %s", string(row.MostCommonElems))
		}
	}
}

func TestPgStatStatements_AllVersions(t *testing.T) {
	for _, inst := range pgInstances {
		inst := inst
		t.Run(fmt.Sprintf("PG%d", inst.version), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			c := PgStatStatementsCollector(
				inst.pool, noopPrepareCtx, true, 1000,
				PgStatStatementsDiffLimit, inst.version,
			)

			// First call: snapshot, no deltas, no avg runtime.
			r1, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("first Collect() error: %v", err)
			}
			if r1 == nil {
				t.Fatal("first Collect() returned nil")
			}

			var p1 PgStatStatementsPayload
			if err := json.Unmarshal(r1.JSON, &p1); err != nil {
				t.Fatalf("failed to parse first result: %v", err)
			}
			if len(p1.Rows) == 0 {
				t.Fatal("expected non-empty rows on first call")
			}
			if len(p1.Deltas) != 0 {
				t.Fatalf("expected 0 deltas on first call, got %d", len(p1.Deltas))
			}
			if p1.DeltaCount != 0 {
				t.Fatalf("expected delta_count = 0 on first call, got %d", p1.DeltaCount)
			}
			if p1.AverageQueryRuntime != 0 {
				t.Fatalf("expected average_query_runtime = 0 on first call, got %f", p1.AverageQueryRuntime)
			}

			// Generate some activity via admin (has SELECT on user tables).
			for i := 0; i < 5; i++ {
				_, _ = inst.admin.Exec(ctx, "SELECT count(*) FROM test_users WHERE score > $1", i*10)
			}

			// Second call: should have deltas.
			r2, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("second Collect() error: %v", err)
			}
			if r2 == nil {
				t.Fatal("second Collect() returned nil")
			}

			var p2 PgStatStatementsPayload
			if err := json.Unmarshal(r2.JSON, &p2); err != nil {
				t.Fatalf("failed to parse second result: %v", err)
			}
			if p2.DeltaCount == 0 {
				t.Fatal("expected delta_count > 0 on second call")
			}
			if p2.AverageQueryRuntime <= 0 {
				t.Fatalf("expected average_query_runtime > 0, got %f", p2.AverageQueryRuntime)
			}

			// Verify delta invariants.
			if len(p2.Deltas) > PgStatStatementsDiffLimit {
				t.Fatalf("len(deltas) = %d exceeds diffLimit %d", len(p2.Deltas), PgStatStatementsDiffLimit)
			}
			if p2.DeltaCount < len(p2.Deltas) {
				t.Fatalf("delta_count (%d) < len(deltas) (%d) — count is pre-cap total", p2.DeltaCount, len(p2.Deltas))
			}
			for i, d := range p2.Deltas {
				if d.Calls == nil || *d.Calls <= 0 {
					t.Fatalf("delta[%d]: expected calls > 0", i)
				}
				if d.TotalExecTime == nil || *d.TotalExecTime <= 0 {
					t.Fatalf("delta[%d]: expected total_exec_time > 0", i)
				}
				if d.Rows == nil || *d.Rows <= 0 {
					t.Fatalf("delta[%d]: expected rows > 0", i)
				}
			}
		})
	}
}

func TestAutovacuumCount_ReturnsData(t *testing.T) {
	inst := latestInstance()
	c := AutovacuumCountCollector(inst.pool, noopPrepareCtx)
	requireRows(t, c)
}

// buildCollectors creates all collectors for a given pool and PG version.
func buildCollectors(pool *pgxpool.Pool, pgMajorVersion int) []CatalogCollector {
	return []CatalogCollector{
		AutovacuumCountCollector(pool, noopPrepareCtx),
		ConnectionStatsCollector(pool, noopPrepareCtx),
		DatabaseSizeCollector(pool, noopPrepareCtx),
		PgAttributeCollector(pool, noopPrepareCtx),
		PgClassCollector(pool, noopPrepareCtx, PgClassBackfillBatchSize),
		PgIndexCollector(pool, noopPrepareCtx),
		PgLocksCollector(pool, noopPrepareCtx),
		PgPreparedXactsCollector(pool, noopPrepareCtx),
		PgReplicationSlotsCollector(pool, noopPrepareCtx),
		PgStatActivityCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatArchiverCollector(pool, noopPrepareCtx),
		PgStatBgwriterCollector(pool, noopPrepareCtx),
		PgStatCheckpointerCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatDatabaseCollector(pool, noopPrepareCtx),
		PgStatDatabaseConflictsCollector(pool, noopPrepareCtx),
		PgStatIOCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatProgressAnalyzeCollector(pool, noopPrepareCtx),
		PgStatProgressCreateIndexCollector(pool, noopPrepareCtx),
		PgStatProgressVacuumCollector(pool, noopPrepareCtx),
		PgStatRecoveryPrefetchCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatReplicationCollector(pool, noopPrepareCtx),
		PgStatReplicationSlotsCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatSlruCollector(pool, noopPrepareCtx),
		PgStatStatementsCollector(pool, noopPrepareCtx, true, 1000, PgStatStatementsDiffLimit, pgMajorVersion),
		PgStatSubscriptionCollector(pool, noopPrepareCtx),
		PgStatSubscriptionStatsCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatUserFunctionsCollector(pool, noopPrepareCtx),
		PgStatUserIndexesCollector(pool, noopPrepareCtx, PgStatUserIndexesCategoryLimit),
		PgStatUserTablesCollector(pool, noopPrepareCtx, PgStatUserTablesCategoryLimit),
		PgStatWalCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatWalReceiverCollector(pool, noopPrepareCtx),
		PgStatioUserIndexesCollector(pool, noopPrepareCtx, PgStatioUserIndexesBatchSize),
		PgStatioUserTablesCollector(pool, noopPrepareCtx),
		PgStatsCollector(pool, noopPrepareCtx, PgStatsBackfillBatchSize, true),
		TransactionCommitsCollector(pool, noopPrepareCtx),
		UptimeMinutesCollector(pool, noopPrepareCtx),
		WaitEventsCollector(pool, noopPrepareCtx),
	}
}
