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
	pool      *pgxpool.Pool               // pg_monitor role — used by collectors under test
	admin     *pgxpool.Pool               // superuser — used for fixture setup only
	container *postgres.PostgresContainer // container — for exec'ing pg_dump etc.
	version   int
}

var pgInstances []pgInstance

// setupTestFixtures creates tables, indexes, functions, inserts data, and
// runs ANALYZE so that collectors have actual data to return.
func setupTestFixtures(ctx context.Context, pool *pgxpool.Pool, pgMajorVersion int) error {
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
		// Partial index for pg_index is_partial / indpred_sql testing.
		`CREATE INDEX idx_test_users_partial ON test_users(score) WHERE score > 50`,
		// Expression index for pg_index indexprs_sql testing.
		`CREATE INDEX idx_test_users_expr ON test_users ((lower(name)))`,
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
		// Materialized view for pg_class relkind='m' testing. ANALYZE so it has
		// pg_stats entries — otherwise it appears in pg_stat_user_tables without
		// any pg_stats rows, which breaks pagination tests that assume every
		// batch yields data.
		`CREATE MATERIALIZED VIEW mv_test_users AS SELECT id, name, score FROM test_users`,
		`ANALYZE mv_test_users`,
		// Index with custom fillfactor for pg_class reloptions testing.
		`CREATE INDEX idx_test_users_score_ff70 ON test_users(score) WITH (fillfactor = 70)`,
		// Enable pg_stat_statements (shared_preload_libraries is set at container start).
		`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`,
	}
	if pgMajorVersion >= 15 {
		// NULLS NOT DISTINCT unique index for pg_index.indnullsnotdistinct testing.
		fixtures = append(fixtures,
			`CREATE UNIQUE INDEX idx_test_users_email_nnd ON test_users(email) NULLS NOT DISTINCT`,
		)
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

	containers := make([]*postgres.PostgresContainer, 0, len(versions))

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

		if err := setupTestFixtures(ctx, adminPool, v); err != nil {
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

		pgInstances = append(pgInstances, pgInstance{pool: monitorPool, admin: adminPool, container: pgContainer, version: v})
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
					requireCollectedAt(t, r1.JSON)

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

// skipUnchangedCollectorNames lists catalog collectors that dedup identical
// payloads between collections. Kept in the test rather than on the struct
// because it's only needed here.
var skipUnchangedCollectorNames = map[string]bool{
	PgAttributeName:               true,
	PgConstraintName:              true,
	PgIndexName:                   true,
	PgTypeName:                    true,
	PgPreparedXactsName:           true,
	PgReplicationSlotsName:        true,
	PgStatArchiverName:            true,
	PgStatDatabaseConflictsName:   true,
	PgStatProgressAnalyzeName:     true,
	PgStatProgressCreateIndexName: true,
	PgStatProgressVacuumName:      true,
	PgStatSubscriptionStatsName:   true,
}

// TestCollectors_SkipUnchanged verifies that collectors with dedup enabled
// return nil on the second identical collection and force-send after the
// threshold.
func TestCollectors_SkipUnchanged(t *testing.T) {
	// Use the latest PG version for this test
	inst := pgInstances[len(pgInstances)-1]
	ctx := context.Background()

	collectors := buildCollectors(inst.pool, inst.version)
	for _, c := range collectors {
		if !skipUnchangedCollectorNames[c.Name] {
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

// requireCollectedAt verifies that the JSON payload contains a non-zero
// "collected_at" timestamp.
func requireCollectedAt(t *testing.T, data []byte) {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("requireCollectedAt: failed to parse JSON: %v", err)
	}
	raw, ok := top["collected_at"]
	if !ok {
		t.Fatal("JSON missing 'collected_at' field")
	}
	var ts time.Time
	if err := json.Unmarshal(raw, &ts); err != nil {
		t.Fatalf("failed to parse collected_at as time: %v", err)
	}
	if ts.IsZero() {
		t.Fatal("collected_at is zero")
	}
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

// forEachPG runs fn as a subtest against every PG version in pgInstances.
// Version subtests run in parallel, one per container.
func forEachPG(t *testing.T, fn func(t *testing.T, inst pgInstance)) {
	t.Helper()
	for _, inst := range pgInstances {
		inst := inst
		t.Run(fmt.Sprintf("PG%d", inst.version), func(t *testing.T) {
			t.Parallel()
			fn(t, inst)
		})
	}
}

// flushStats makes recent ANALYZE/I/O activity visible in pg_stat_* views.
// PG 15+ has shared-memory stats, so pg_stat_force_next_flush() is synchronous.
// PG 13/14 still run the async stats collector: backends send messages at
// commit, the collector writes them on a timer, and reader backends cache
// snapshots per-transaction. Rather than guessing at a fixed sleep, we poll
// until at least one idx counter is non-zero (fixture activity has landed),
// then wait a short settling period.
func flushStats(ctx context.Context, inst pgInstance) {
	if inst.version >= 15 {
		_, _ = inst.admin.Exec(ctx, "SELECT pg_stat_force_next_flush()")
		return
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var total int64
		if err := inst.admin.QueryRow(ctx,
			`SELECT COALESCE(SUM(idx_blks_read + idx_blks_hit), 0) FROM pg_statio_user_indexes`,
		).Scan(&total); err == nil && total > 0 {
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}

func TestWaitEvents_ReturnsAllTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := WaitEventsCollector(inst.pool, noopPrepareCtx)
		n := requireRows(t, c)
		// 9 wait event types + 1 TOTAL row = 10
		if n != 10 {
			t.Fatalf("expected 10 rows (9 types + TOTAL), got %d", n)
		}
	})
}

func TestConnectionStats_ReturnsData(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := ConnectionStatsCollector(inst.pool, noopPrepareCtx)
		requireRows(t, c)
	})
}

func TestDatabaseSize_ReturnsData(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := DatabaseSizeCollector(inst.pool, noopPrepareCtx)
		requireRows(t, c)
	})
}

func TestUptimeMinutes_ReturnsData(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := UptimeMinutesCollector(inst.pool, noopPrepareCtx)
		requireRows(t, c)
	})
}

func TestTransactionCommits_ComputesTPS(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
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
		var p1 Payload[TransactionCommitsRow]
		if err := json.Unmarshal(r1.JSON, &p1); err != nil {
			t.Fatalf("failed to parse first result: %v", err)
		}
		if p1.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at on first call")
		}
		if len(p1.Rows) != 1 {
			t.Fatalf("expected 1 row on first call, got %d", len(p1.Rows))
		}
		if p1.Rows[0].XactCommit <= 0 {
			t.Fatalf("expected xact_commit > 0 on first call, got %d", p1.Rows[0].XactCommit)
		}
		if p1.Rows[0].TPS != 0 {
			t.Fatalf("expected TPS = 0 on first call (no baseline), got %f", p1.Rows[0].TPS)
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
		var p2 Payload[TransactionCommitsRow]
		if err := json.Unmarshal(r2.JSON, &p2); err != nil {
			t.Fatalf("failed to parse second result: %v", err)
		}
		if p2.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at on second call")
		}
		if len(p2.Rows) != 1 {
			t.Fatalf("expected 1 row on second call, got %d", len(p2.Rows))
		}
		if p2.Rows[0].XactCommit < p1.Rows[0].XactCommit {
			t.Fatalf("xact_commit decreased: %d → %d", p1.Rows[0].XactCommit, p2.Rows[0].XactCommit)
		}
		if p2.Rows[0].TPS < 0 {
			t.Fatalf("expected TPS >= 0, got %f", p2.Rows[0].TPS)
		}
	})
}

func TestPgStatUserTables_ReturnsFixtureTable(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgStatUserTablesCollector(inst.pool, noopPrepareCtx, PgStatUserTablesConfig{CategoryLimit: PgStatUserTablesCategoryLimit})
		requireRows(t, c)
	})
}

func TestPgStatUserIndexes_ReturnsFixtureIndexes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgStatUserIndexesCollector(inst.pool, noopPrepareCtx, PgStatUserIndexesConfig{CategoryLimit: PgStatUserIndexesCategoryLimit})
		requireRows(t, c)
	})
}

func TestPgStatioUserTables_NoError(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgStatioUserTablesCollector(inst.pool, noopPrepareCtx)
		ctx := context.Background()
		// On fresh test containers, all data may be in shared_buffers with zero
		// I/O counters, so the WHERE filter may exclude everything. Just verify
		// no error — the query and scanning work correctly.
		_, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
	})
}

func TestPgStatioUserIndexes_ReturnsFixtureData(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgStatioUserIndexesCollector(inst.pool, noopPrepareCtx, PgStatioUserIndexesConfig{BackfillBatchSize: PgStatioUserIndexesBatchSize})
		requireRows(t, c)
	})
}

func TestPgStatioUserIndexes_BackfillThenDelta(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		// Use batch size 1 to force multiple backfill ticks and test pagination.
		c := PgStatioUserIndexesCollector(inst.pool, noopPrepareCtx, PgStatioUserIndexesConfig{BackfillBatchSize: 1})

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
		if p1.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at on first backfill result")
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
	})
}

func TestPgStatUserFunctions_ReturnsFixtureFunction(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
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
	})
}

func TestPgIndex_ReturnsFixtureIndexes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgIndexCollector(inst.pool, noopPrepareCtx, inst.version)
		n := requireRows(t, c)
		// We created 6 indexes: PK + 3 explicit + 1 partial + 1 expression
		if n < 6 {
			t.Fatalf("expected at least 6 indexes, got %d", n)
		}
	})
}

func TestPgIndex_NewFieldsPopulated(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()
		c := PgIndexCollector(inst.pool, noopPrepareCtx, inst.version)

		result, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if result == nil {
			t.Fatal("Collect() returned nil")
		}

		var p Payload[PgIndexRow]
		if err := json.Unmarshal(result.JSON, &p); err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}

		// Build a map of index name -> row for easy lookup.
		byName := make(map[string]*PgIndexRow, len(p.Rows))
		for i := range p.Rows {
			if p.Rows[i].IndexName != nil {
				byName[string(*p.Rows[i].IndexName)] = &p.Rows[i]
			}
		}

		// All indexes should have indkey, indoption, indclass, indcollation arrays
		// whose lengths match the number of index columns (indnatts).
		for name, row := range byName {
			if row.IndNatts == nil {
				t.Errorf("index %q: indnatts is nil", name)
				continue
			}
			natts := int(int64(*row.IndNatts))
			if len(row.IndKey) != natts {
				t.Errorf("index %q: expected len(indkey)=%d, got %d", name, natts, len(row.IndKey))
			}
			if len(row.IndOption) != natts {
				t.Errorf("index %q: expected len(indoption)=%d, got %d", name, natts, len(row.IndOption))
			}
			if len(row.IndClass) != natts {
				t.Errorf("index %q: expected len(indclass)=%d, got %d", name, natts, len(row.IndClass))
			}
			if len(row.IndCollation) != natts {
				t.Errorf("index %q: expected len(indcollation)=%d, got %d", name, natts, len(row.IndCollation))
			}
		}

		// --- Partial index: CREATE INDEX ... ON test_users(score) WHERE score > 50 ---
		partial, ok := byName["idx_test_users_partial"]
		if !ok {
			t.Fatal("could not find idx_test_users_partial in results")
		}
		if partial.IsPartial == nil || !bool(*partial.IsPartial) {
			t.Error("expected is_partial=true for partial index")
		}
		if partial.IndPredSQL == nil {
			t.Fatal("expected indpred_sql to be non-nil for partial index")
		}
		if !strings.Contains(string(*partial.IndPredSQL), "score") || !strings.Contains(string(*partial.IndPredSQL), "50") {
			t.Errorf("expected indpred_sql to reference 'score' and '50', got %q", string(*partial.IndPredSQL))
		}
		// Partial index on a single column.
		if partial.IndNKeyAtts == nil || int64(*partial.IndNKeyAtts) != 1 {
			t.Errorf("expected indnkeyatts=1 for partial index, got %v", partial.IndNKeyAtts)
		}

		// --- Regular index: CREATE INDEX ... ON test_users(name) ---
		regular, ok := byName["idx_test_users_name"]
		if !ok {
			t.Fatal("could not find idx_test_users_name in results")
		}
		if regular.IsPartial == nil || bool(*regular.IsPartial) {
			t.Error("expected is_partial=false for regular index")
		}
		if regular.IndPredSQL != nil {
			t.Errorf("expected indpred_sql=nil for regular index, got %q", string(*regular.IndPredSQL))
		}
		if regular.IndExprsSQL != nil {
			t.Errorf("expected indexprs_sql=nil for regular index, got %q", string(*regular.IndExprsSQL))
		}
		// indclass entries should be non-zero (valid opclass OIDs).
		for i, oc := range regular.IndClass {
			if uint32(oc) == 0 {
				t.Errorf("index idx_test_users_name: indclass[%d] is 0, expected valid opclass OID", i)
			}
		}
		// indkey on a regular column-reference index should have a non-zero entry
		// pointing to the underlying table column (0 is reserved for expressions).
		if len(regular.IndKey) != 1 || int64(regular.IndKey[0]) == 0 {
			t.Errorf("expected indkey=[<nonzero>] for regular index, got %v", regular.IndKey)
		}

		// --- Expression index: CREATE INDEX ... ON test_users ((lower(name))) ---
		expr, ok := byName["idx_test_users_expr"]
		if !ok {
			t.Fatal("could not find idx_test_users_expr in results")
		}
		if expr.IndExprsSQL == nil {
			t.Fatal("expected indexprs_sql to be non-nil for expression index")
		}
		if !strings.Contains(string(*expr.IndExprsSQL), "lower") {
			t.Errorf("expected indexprs_sql to contain 'lower', got %q", string(*expr.IndExprsSQL))
		}
		// Expression index is not partial.
		if expr.IsPartial == nil || bool(*expr.IsPartial) {
			t.Error("expected is_partial=false for expression index")
		}
		if expr.IndPredSQL != nil {
			t.Errorf("expected indpred_sql=nil for expression index, got %q", string(*expr.IndPredSQL))
		}
		// indkey entry of 0 signals that the column is an expression (resolved via indexprs).
		if len(expr.IndKey) != 1 || int64(expr.IndKey[0]) != 0 {
			t.Errorf("expected indkey=[0] for expression index, got %v", expr.IndKey)
		}

		// --- Unique index: CREATE UNIQUE INDEX ... ON test_users(email) ---
		unique, ok := byName["idx_test_users_email"]
		if !ok {
			t.Fatal("could not find idx_test_users_email in results")
		}
		if unique.IndIsUnique == nil || !bool(*unique.IndIsUnique) {
			t.Error("expected indisunique=true for unique index")
		}
		if unique.IsPartial == nil || bool(*unique.IsPartial) {
			t.Error("expected is_partial=false for unique index")
		}
		// indnullsnotdistinct defaults to false on PG15+ (NULLs are distinct by default).
		if inst.version >= 15 {
			if unique.IndNullsNotDistinct == nil {
				t.Error("expected indnullsnotdistinct to be non-nil on PG15+")
			} else if bool(*unique.IndNullsNotDistinct) {
				t.Error("expected indnullsnotdistinct=false for default unique index")
			}
		} else if unique.IndNullsNotDistinct != nil {
			t.Errorf("expected indnullsnotdistinct=nil on PG<15, got %v", *unique.IndNullsNotDistinct)
		}

		// --- NULLS NOT DISTINCT unique index (PG15+ only) ---
		if inst.version >= 15 {
			nnd, ok := byName["idx_test_users_email_nnd"]
			if !ok {
				t.Fatal("could not find idx_test_users_email_nnd in results")
			}
			if nnd.IndNullsNotDistinct == nil || !bool(*nnd.IndNullsNotDistinct) {
				t.Errorf("expected indnullsnotdistinct=true for NULLS NOT DISTINCT index, got %v", nnd.IndNullsNotDistinct)
			}
		}
	})
}

func TestPgAttribute_ReturnsFixtureColumns(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgAttributeCollector(inst.pool, noopPrepareCtx)
		n := requireRows(t, c)
		// test_users has 5 columns + index columns (each index has entries)
		if n < 5 {
			t.Fatalf("expected at least 5 attribute rows, got %d", n)
		}
	})
}

func TestPgAttribute_NewFieldsPopulated(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgAttributeCollector(inst.pool, noopPrepareCtx)
		ctx := context.Background()

		result, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if result == nil {
			t.Fatal("Collect() returned nil")
		}

		var p Payload[PgAttributeRow]
		if err := json.Unmarshal(result.JSON, &p); err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}
		if len(p.Rows) == 0 {
			t.Fatal("expected non-empty rows")
		}

		// Build a lookup by column name. pg_attribute rows span multiple tables,
		// so we may see duplicates; just grab the first match per name.
		byName := make(map[string]*PgAttributeRow, len(p.Rows))
		for i := range p.Rows {
			if p.Rows[i].AttName != nil {
				name := string(*p.Rows[i].AttName)
				if _, exists := byName[name]; !exists {
					byName[name] = &p.Rows[i]
				}
			}
		}

		// --- "name" column: text NOT NULL ---
		nameCol, ok := byName["name"]
		if !ok {
			t.Fatal("could not find 'name' column in pg_attribute results")
		}
		// text uses extended storage ("x").
		if nameCol.AttStorage == nil {
			t.Fatal("expected attstorage to be non-nil for 'name' column")
		}
		if string(*nameCol.AttStorage) != "x" {
			t.Errorf("expected attstorage='x' (extended) for text column, got %q", string(*nameCol.AttStorage))
		}
		// NOT NULL constraint.
		if nameCol.AttNotNull == nil || !bool(*nameCol.AttNotNull) {
			t.Error("expected attnotnull=true for NOT NULL 'name' column")
		}
		// text alignment is "i" (int).
		if nameCol.AttAlign == nil {
			t.Fatal("expected attalign to be non-nil")
		}
		if string(*nameCol.AttAlign) != "i" {
			t.Errorf("expected attalign='i' for text column, got %q", string(*nameCol.AttAlign))
		}
		// Default stats target: PG 17+ reports NULL; older versions report -1.
		if inst.version >= 17 {
			if nameCol.AttStatTarget != nil {
				t.Errorf("expected attstattarget=nil (default) on PG %d, got %d", inst.version, int64(*nameCol.AttStatTarget))
			}
		} else {
			if nameCol.AttStatTarget == nil {
				t.Fatal("expected attstattarget to be non-nil")
			}
			if int64(*nameCol.AttStatTarget) != -1 {
				t.Errorf("expected attstattarget=-1 (system default), got %d", int64(*nameCol.AttStatTarget))
			}
		}
		// Plain text has atttypmod=-1 (no length modifier).
		if nameCol.AttTypMod == nil {
			t.Fatal("expected atttypmod to be non-nil")
		}
		if int64(*nameCol.AttTypMod) != -1 {
			t.Errorf("expected atttypmod=-1 for plain text, got %d", int64(*nameCol.AttTypMod))
		}

		// --- "score" column: integer DEFAULT 0 (nullable) ---
		scoreCol, ok := byName["score"]
		if !ok {
			t.Fatal("could not find 'score' column in pg_attribute results")
		}
		// integer uses plain storage ("p").
		if scoreCol.AttStorage == nil {
			t.Fatal("expected attstorage to be non-nil for 'score' column")
		}
		if string(*scoreCol.AttStorage) != "p" {
			t.Errorf("expected attstorage='p' (plain) for integer column, got %q", string(*scoreCol.AttStorage))
		}
		// "score" is nullable (no NOT NULL).
		if scoreCol.AttNotNull == nil || bool(*scoreCol.AttNotNull) {
			t.Error("expected attnotnull=false for nullable 'score' column")
		}
		// integer alignment is "i" (int).
		if scoreCol.AttAlign == nil {
			t.Fatal("expected attalign to be non-nil for 'score' column")
		}
		if string(*scoreCol.AttAlign) != "i" {
			t.Errorf("expected attalign='i' for integer column, got %q", string(*scoreCol.AttAlign))
		}

		// --- "id" column: serial PRIMARY KEY (NOT NULL, integer) ---
		idCol, ok := byName["id"]
		if !ok {
			t.Fatal("could not find 'id' column in pg_attribute results")
		}
		if idCol.AttNotNull == nil || !bool(*idCol.AttNotNull) {
			t.Error("expected attnotnull=true for PRIMARY KEY 'id' column")
		}
	})
}

func TestPgClass_BackfillThenDelta(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		// Use batch size 1 to force multiple backfill ticks and test pagination.
		c := PgClassCollector(inst.pool, noopPrepareCtx, PgClassConfig{BackfillBatchSize: 1})

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
		if p1.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at on first backfill result")
		}
		// Verify row has non-empty identifiers.
		if p1.Rows[0].SchemaName == "" || p1.Rows[0].Oid == 0 || p1.Rows[0].RelName == "" {
			t.Fatalf("expected non-empty schemaname/oid/relname, got %q/%q/%q", p1.Rows[0].SchemaName, p1.Rows[0].Oid, p1.Rows[0].RelName)
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
		flushStats(ctx, inst)

		rAfterAnalyze, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("post-ANALYZE Collect() error: %v", err)
		}
		if rAfterAnalyze == nil {
			t.Fatal("expected delta data after ANALYZE, got nil")
		}
	})
}

func TestPgClass_FullRescanPicksUpAllRelkinds(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		c := PgClassCollector(inst.pool, noopPrepareCtx, PgClassConfig{BackfillBatchSize: 1000})

		// Drain the backfill.
		for i := 0; i < 100; i++ {
			r, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("backfill tick %d error: %v", i+1, err)
			}
			if r == nil {
				break
			}
		}

		// Run delta ticks until we hit the full rescan interval.
		// pgClassFullRescanInterval is 30, so tick 30 triggers the rescan.
		var rescanResult *CollectResult
		for i := 1; i <= pgClassFullRescanInterval; i++ {
			r, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("delta tick %d error: %v", i, err)
			}
			if i == pgClassFullRescanInterval {
				rescanResult = r
			}
		}
		if rescanResult == nil {
			t.Fatal("expected non-nil result on full rescan tick")
		}

		var p Payload[PgClassRow]
		if err := json.Unmarshal(rescanResult.JSON, &p); err != nil {
			t.Fatalf("failed to parse full rescan result: %v", err)
		}

		// Full rescan should include all relkinds (r, i, m).
		relkinds := make(map[string]bool)
		for _, row := range p.Rows {
			relkinds[string(row.RelKind)] = true
		}
		if !relkinds["r"] {
			t.Error("full rescan missing relkind='r' (tables)")
		}
		if !relkinds["i"] {
			t.Error("full rescan missing relkind='i' (indexes)")
		}
		if !relkinds["m"] {
			t.Error("full rescan missing relkind='m' (materialized views)")
		}
	})
}

func TestPgClass_NewFieldsPopulated(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		// Use a large batch size to get all rows in one backfill tick.
		c := PgClassCollector(inst.pool, noopPrepareCtx, PgClassConfig{BackfillBatchSize: 1000})
		result, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if result == nil {
			t.Fatal("Collect() returned nil")
		}

		var p Payload[PgClassRow]
		if err := json.Unmarshal(result.JSON, &p); err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}

		// Build a map of relname -> row for easy lookup.
		byName := make(map[string]*PgClassRow, len(p.Rows))
		for i := range p.Rows {
			byName[string(p.Rows[i].RelName)] = &p.Rows[i]
		}

		// Collect observed relkinds to verify broadened filter.
		relkinds := make(map[string]bool)
		for _, row := range p.Rows {
			relkinds[string(row.RelKind)] = true
		}

		// We should see tables ('r'), indexes ('i'), and materialized views ('m').
		if !relkinds["r"] {
			t.Error("expected relkind='r' (table) in results")
		}
		if !relkinds["i"] {
			t.Error("expected relkind='i' (index) in results")
		}
		if !relkinds["m"] {
			t.Error("expected relkind='m' (materialized view) in results")
		}

		// --- test_users table: relkind='r', persistent, has toast (text columns) ---
		table, ok := byName["test_users"]
		if !ok {
			t.Fatal("could not find 'test_users' in pg_class results")
		}
		if string(table.RelKind) != "r" {
			t.Errorf("expected relkind='r' for test_users, got %q", string(table.RelKind))
		}
		if string(table.RelPersistence) != "p" {
			t.Errorf("expected relpersistence='p' (permanent) for test_users, got %q", string(table.RelPersistence))
		}
		if bool(table.RelIsPartition) {
			t.Error("expected relispartition=false for test_users")
		}
		if bool(table.RelHasSubClass) {
			t.Error("expected relhassubclass=false for test_users (not inherited)")
		}
		// Regular tables use the 'heap' access method by default.
		if table.AccessMethod == nil {
			t.Fatal("expected access_method to be non-nil for table")
		}
		if string(*table.AccessMethod) != "heap" {
			t.Errorf("expected access_method='heap' for test_users, got %q", string(*table.AccessMethod))
		}
		// test_users has text columns, so it should have a TOAST table.
		if uint32(table.RelToastRelID) == 0 {
			t.Error("expected reltoastrelid != 0 for table with text columns")
		}
		// 0 = default tablespace.
		if uint32(table.RelTablespace) != 0 {
			t.Errorf("expected reltablespace=0 (default), got %d", uint32(table.RelTablespace))
		}
		// We inserted 500 rows and ran ANALYZE; reltuples should reflect this.
		if float64(table.RelTuples) < 400 {
			t.Errorf("expected reltuples >= 400 after inserting 500 rows, got %.0f", float64(table.RelTuples))
		}
		// After ANALYZE, some pages should be all-visible.
		if int64(table.RelAllVisible) < 0 {
			t.Errorf("expected relallvisible >= 0, got %d", int64(table.RelAllVisible))
		}
		// Regular table with no reloptions should have nil/empty reloptions.
		if len(table.RelOptions) != 0 {
			t.Errorf("expected empty reloptions for table with defaults, got %v", table.RelOptions)
		}
		// XID ages: we inserted data and ran ANALYZE, so relfrozenxid_age should
		// be positive (transactions have occurred since the freeze point).
		if int64(table.RelFrozenXIDAge) <= 0 {
			t.Errorf("expected relfrozenxid_age > 0 for table with data, got %d", int64(table.RelFrozenXIDAge))
		}
		// relminmxid_age should be non-negative (0 if no multixact activity).
		if int64(table.RelMinMXIDAge) < 0 {
			t.Errorf("expected relminmxid_age >= 0, got %d", int64(table.RelMinMXIDAge))
		}

		// --- idx_test_users_name: relkind='i', btree access method ---
		idx, ok := byName["idx_test_users_name"]
		if !ok {
			t.Fatal("could not find 'idx_test_users_name' in pg_class results")
		}
		if string(idx.RelKind) != "i" {
			t.Errorf("expected relkind='i' for index, got %q", string(idx.RelKind))
		}
		if idx.AccessMethod == nil {
			t.Fatal("expected access_method to be non-nil for index")
		}
		if string(*idx.AccessMethod) != "btree" {
			t.Errorf("expected access_method='btree', got %q", string(*idx.AccessMethod))
		}
		// Indexes don't have TOAST tables.
		if uint32(idx.RelToastRelID) != 0 {
			t.Errorf("expected reltoastrelid=0 for index, got %d", uint32(idx.RelToastRelID))
		}

		// --- idx_test_users_score_ff70: index with fillfactor=70 in reloptions ---
		idxFF, ok := byName["idx_test_users_score_ff70"]
		if !ok {
			t.Fatal("could not find 'idx_test_users_score_ff70' in pg_class results")
		}
		if len(idxFF.RelOptions) == 0 {
			t.Fatal("expected non-empty reloptions for index with fillfactor=70")
		}
		foundFF := false
		for _, opt := range idxFF.RelOptions {
			if string(opt) == "fillfactor=70" {
				foundFF = true
				break
			}
		}
		if !foundFF {
			t.Errorf("expected reloptions to contain 'fillfactor=70', got %v", idxFF.RelOptions)
		}
		if idxFF.AccessMethod == nil || string(*idxFF.AccessMethod) != "btree" {
			t.Errorf("expected access_method='btree' for idx_test_users_score_ff70")
		}

		// --- mv_test_users: relkind='m', materialized view ---
		mv, ok := byName["mv_test_users"]
		if !ok {
			t.Fatal("could not find 'mv_test_users' in pg_class results")
		}
		if string(mv.RelKind) != "m" {
			t.Errorf("expected relkind='m' for materialized view, got %q", string(mv.RelKind))
		}
		if string(mv.RelPersistence) != "p" {
			t.Errorf("expected relpersistence='p' for materialized view, got %q", string(mv.RelPersistence))
		}
		// Materialized views have heap access method.
		if mv.AccessMethod == nil {
			t.Fatal("expected access_method to be non-nil for materialized view")
		}
		if string(*mv.AccessMethod) != "heap" {
			t.Errorf("expected access_method='heap' for materialized view, got %q", string(*mv.AccessMethod))
		}
	})
}

func TestPgConstraint_ReturnsFixtureConstraints(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()
		c := PgConstraintCollector(inst.pool, noopPrepareCtx)

		result, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if result == nil {
			t.Fatal("Collect() returned nil")
		}

		var p Payload[PgConstraintRow]
		if err := json.Unmarshal(result.JSON, &p); err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}
		if len(p.Rows) == 0 {
			t.Fatal("expected non-empty constraint rows")
		}

		// Build a map of conname -> row.
		byName := make(map[string]*PgConstraintRow, len(p.Rows))
		for i := range p.Rows {
			if p.Rows[i].ConName != nil {
				byName[string(*p.Rows[i].ConName)] = &p.Rows[i]
			}
		}

		// test_users has a PRIMARY KEY on 'id' → contype='p'.
		var pk *PgConstraintRow
		for i := range p.Rows {
			row := &p.Rows[i]
			if row.ConType != nil && string(*row.ConType) == "p" && row.ConRelID != nil {
				pk = row
				break
			}
		}
		if pk == nil {
			t.Fatal("could not find a primary key constraint (contype='p')")
		}
		if pk.ConName == nil || string(*pk.ConName) == "" {
			t.Error("expected non-empty conname for primary key")
		}
		// PK has a single key column.
		if len(pk.ConKey) != 1 {
			t.Errorf("expected conkey length 1 for single-column PK, got %d", len(pk.ConKey))
		}
		// PK should reference a backing index.
		if pk.ConIndID == nil || uint32(*pk.ConIndID) == 0 {
			t.Error("expected non-zero conindid for primary key (backing index)")
		}
		// PK is not deferrable.
		if pk.ConDeferrable == nil || bool(*pk.ConDeferrable) {
			t.Error("expected condeferrable=false for primary key")
		}
		// PK is validated.
		if pk.ConValidated == nil || !bool(*pk.ConValidated) {
			t.Error("expected convalidated=true for primary key")
		}
		// PK is not a foreign key, so confrelid should be 0 and confkey empty.
		if pk.ConfRelID != nil && uint32(*pk.ConfRelID) != 0 {
			t.Errorf("expected confrelid=0 for PK, got %d", uint32(*pk.ConfRelID))
		}
		if len(pk.ConfKey) != 0 {
			t.Errorf("expected empty confkey for PK, got %v", pk.ConfKey)
		}

		// Verify we see all expected constraint types in the results.
		conTypes := make(map[string]bool)
		for _, row := range p.Rows {
			if row.ConType != nil {
				conTypes[string(*row.ConType)] = true
			}
		}
		// At minimum, we should see primary key constraints from test_users and test_cold.
		if !conTypes["p"] {
			t.Error("expected at least one primary key constraint (contype='p')")
		}
	})
}

func TestPgType_ReturnsBuiltinAndUserTypes(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()
		c := PgTypeCollector(inst.pool, noopPrepareCtx)

		result, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if result == nil {
			t.Fatal("Collect() returned nil")
		}

		var p Payload[PgTypeRow]
		if err := json.Unmarshal(result.JSON, &p); err != nil {
			t.Fatalf("failed to parse result: %v", err)
		}
		if len(p.Rows) == 0 {
			t.Fatal("expected non-empty type rows")
		}

		// Build a map of typname -> row.
		byName := make(map[string]*PgTypeRow, len(p.Rows))
		for i := range p.Rows {
			if p.Rows[i].TypName != nil {
				byName[string(*p.Rows[i].TypName)] = &p.Rows[i]
			}
		}

		// --- Built-in type: "int4" (integer) ---
		int4, ok := byName["int4"]
		if !ok {
			t.Fatal("could not find built-in type 'int4' in results")
		}
		// int4 is a base type.
		if int4.TypType == nil || string(*int4.TypType) != "b" {
			t.Errorf("expected typtype='b' (base) for int4, got %v", int4.TypType)
		}
		// int4 is 4 bytes, pass-by-value.
		if int4.TypLen == nil || int64(*int4.TypLen) != 4 {
			t.Errorf("expected typlen=4 for int4, got %v", int4.TypLen)
		}
		if int4.TypByVal == nil || !bool(*int4.TypByVal) {
			t.Error("expected typbyval=true for int4")
		}
		// int4 uses plain storage.
		if int4.TypStorage == nil || string(*int4.TypStorage) != "p" {
			t.Errorf("expected typstorage='p' (plain) for int4, got %v", int4.TypStorage)
		}
		// int4 alignment is "i" (int).
		if int4.TypAlign == nil || string(*int4.TypAlign) != "i" {
			t.Errorf("expected typalign='i' for int4, got %v", int4.TypAlign)
		}
		// int4 is in the numeric category.
		if int4.TypCategory == nil || string(*int4.TypCategory) != "N" {
			t.Errorf("expected typcategory='N' (numeric) for int4, got %v", int4.TypCategory)
		}

		// --- Built-in type: "text" ---
		text, ok := byName["text"]
		if !ok {
			t.Fatal("could not find built-in type 'text' in results")
		}
		// text is variable-length (typlen=-1), pass-by-reference.
		if text.TypLen == nil || int64(*text.TypLen) != -1 {
			t.Errorf("expected typlen=-1 for text, got %v", text.TypLen)
		}
		if text.TypByVal == nil || bool(*text.TypByVal) {
			t.Error("expected typbyval=false for text")
		}
		// text uses extended storage.
		if text.TypStorage == nil || string(*text.TypStorage) != "x" {
			t.Errorf("expected typstorage='x' (extended) for text, got %v", text.TypStorage)
		}
		// text is in the string category.
		if text.TypCategory == nil || string(*text.TypCategory) != "S" {
			t.Errorf("expected typcategory='S' (string) for text, got %v", text.TypCategory)
		}

		// --- Built-in type: "bool" ---
		boolType, ok := byName["bool"]
		if !ok {
			t.Fatal("could not find built-in type 'bool' in results")
		}
		// bool is 1 byte, pass-by-value.
		if boolType.TypLen == nil || int64(*boolType.TypLen) != 1 {
			t.Errorf("expected typlen=1 for bool, got %v", boolType.TypLen)
		}
		if boolType.TypByVal == nil || !bool(*boolType.TypByVal) {
			t.Error("expected typbyval=true for bool")
		}
	})
}

func TestPgStats_BackfillThenDelta(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()

		// Use batch size 1 to force pagination.
		c := PgStatsCollector(inst.pool, noopPrepareCtx, PgStatsConfig{BackfillBatchSize: 1, IncludeTableData: true})

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
		if p1.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at on first backfill result")
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
		flushStats(ctx, inst)

		rAfterAnalyze, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("post-ANALYZE Collect() error: %v", err)
		}
		if rAfterAnalyze == nil {
			t.Fatal("expected delta data after ANALYZE, got nil")
		}
	})
}

func TestPgStats_RedactsWhenIncludeTableDataFalse(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := PgStatsCollector(inst.pool, noopPrepareCtx, PgStatsConfig{BackfillBatchSize: PgStatsBackfillBatchSize})
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

		if payload.CollectedAt.IsZero() {
			t.Fatal("expected non-zero collected_at")
		}
		// After JSON round-trip, nil json.RawMessage becomes []byte("null").
		const jsonNull = "null"
		for _, row := range payload.Rows {
			if row.MostCommonVals != nil && string(row.MostCommonVals) != jsonNull {
				t.Fatalf("expected most_common_vals to be null when includeTableData=false, got %s", string(row.MostCommonVals))
			}
			if row.HistogramBounds != nil && string(row.HistogramBounds) != jsonNull {
				t.Fatalf("expected histogram_bounds to be null when includeTableData=false, got %s", string(row.HistogramBounds))
			}
			if row.MostCommonElems != nil && string(row.MostCommonElems) != jsonNull {
				t.Fatalf("expected most_common_elems to be null when includeTableData=false, got %s", string(row.MostCommonElems))
			}
		}
	})
}

func TestPgStatStatements_AllVersions(t *testing.T) {
	for _, inst := range pgInstances {
		inst := inst
		t.Run(fmt.Sprintf("PG%d", inst.version), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			c := PgStatStatementsCollector(
				inst.pool, noopPrepareCtx, PgStatStatementsConfig{
					DiffLimit:          PgStatStatementsDiffLimit,
					IncludeQueries:     true,
					MaxQueryTextLength: 1000,
				},
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
			if p1.CollectedAt.IsZero() {
				t.Fatal("expected non-zero collected_at on first call")
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
			if p2.CollectedAt.IsZero() {
				t.Fatal("expected non-zero collected_at on second call")
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
			}
		})
	}
}

// TestPgStatStatements_AdaptsToOldExtensionOnNewServer is the integration-level
// regression test for the bug where the collector built the query from the
// PostgreSQL server major version instead of the pg_stat_statements extension
// version. On a server upgraded to PG 17 with the extension still pinned at
// 1.10 (the realistic Amazon RDS state until ALTER EXTENSION ... UPDATE is
// run), the old logic generated SELECT shared_blk_read_time which does not
// exist in the 1.10 view (SQLSTATE 42703). This test reproduces that exact
// "new server / old extension" combination by explicitly creating the
// extension at version 1.10 in a fresh database, then exercises the
// version-change rebuild path by ALTERing the extension up to 1.11.
//
// The reproduction depends on the PG container shipping the
// pg_stat_statements--*--*.sql upgrade scripts down to 1.4 (it does, see
// contrib/pg_stat_statements in the postgres source tree). PG 15 is the
// earliest version that supports installing at exactly 1.10; PG 17 is the
// earliest version that supports updating to 1.11.
func TestPgStatStatements_AdaptsToOldExtensionOnNewServer(t *testing.T) {
	for _, inst := range pgInstances {
		// PG 15 is the earliest version where 1.10 is reachable as the
		// install target. On PG 13/14, the contrib package still tops out at
		// 1.9 / 1.8 respectively, so installing at 1.10 isn't possible.
		if inst.version < 15 {
			continue
		}
		inst := inst
		t.Run(fmt.Sprintf("PG%d_install_ext_1_10", inst.version), func(t *testing.T) {
			// No t.Parallel() — we create and drop a fresh database, but
			// keeping things sequential makes failure modes easier to read.
			ctx := context.Background()

			// Create an isolated database so we don't disturb shared
			// extension state in testdb (which other parallel subtests rely
			// on).
			dbName := fmt.Sprintf("pgss_oldext_pg%d", inst.version)
			if _, err := inst.admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
				t.Fatalf("CREATE DATABASE %s: %v", dbName, err)
			}
			t.Cleanup(func() {
				// Force-disconnect any leftover sessions so DROP succeeds.
				_, _ = inst.admin.Exec(ctx,
					"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
					dbName)
				_, _ = inst.admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
			})

			// Build a pool to the fresh database (admin user, since CREATE
			// EXTENSION needs superuser).
			baseConn, err := inst.container.ConnectionString(ctx, "sslmode=disable")
			if err != nil {
				t.Fatalf("ConnectionString: %v", err)
			}
			cfg, err := pgxpool.ParseConfig(baseConn)
			if err != nil {
				t.Fatalf("pgxpool.ParseConfig: %v", err)
			}
			cfg.ConnConfig.Database = dbName
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatalf("pgxpool.NewWithConfig: %v", err)
			}
			defer pool.Close()

			// Install pg_stat_statements at exactly 1.10 — the extension
			// version that lacks shared_blk_read_time.
			if _, err := pool.Exec(ctx, "CREATE EXTENSION pg_stat_statements VERSION '1.10'"); err != nil {
				t.Fatalf("CREATE EXTENSION pg_stat_statements VERSION '1.10': %v", err)
			}

			// Sanity: confirm the installed version is what we asked for.
			var installed string
			if err := pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname='pg_stat_statements'").Scan(&installed); err != nil {
				t.Fatalf("SELECT extversion: %v", err)
			}
			if installed != "1.10" {
				t.Fatalf("expected installed extversion 1.10, got %q", installed)
			}

			// Generate some pg_stat_statements activity.
			for i := 0; i < 5; i++ {
				_, _ = pool.Exec(ctx, "SELECT count(*) FROM pg_class WHERE oid > $1", i*100)
			}

			c := PgStatStatementsCollector(pool, noopPrepareCtx, PgStatStatementsConfig{
				DiffLimit:          PgStatStatementsDiffLimit,
				IncludeQueries:     true,
				MaxQueryTextLength: 1000,
			})

			// REGRESSION: under the old (server-version-based) gate, this
			// Collect() would fail on PG >= 17 with
			//   ERROR: column "shared_blk_read_time" does not exist
			// because the server reports >=17 but the 1.10 view only has
			// blk_read_time. The new (extension-version-based) gate must
			// succeed.
			r1, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("PG%d + ext 1.10: Collect failed (regression): %v", inst.version, err)
			}
			var p1 PgStatStatementsPayload
			if err := json.Unmarshal(r1.JSON, &p1); err != nil {
				t.Fatalf("unmarshal first payload: %v", err)
			}
			if len(p1.Rows) == 0 {
				t.Fatalf("PG%d + ext 1.10: expected non-empty rows", inst.version)
			}

			// Exercise the version-change rebuild path by upgrading the
			// extension to 1.11. This is only meaningful (and only
			// reachable) on PG 17+, since 1.11 was added with PG 17.
			if inst.version >= 17 {
				if _, err := pool.Exec(ctx, "ALTER EXTENSION pg_stat_statements UPDATE TO '1.11'"); err != nil {
					t.Fatalf("ALTER EXTENSION ... UPDATE TO '1.11': %v", err)
				}

				// Generate additional activity so the collector has fresh
				// rows to report after the rebuild.
				for i := 0; i < 5; i++ {
					_, _ = pool.Exec(ctx, "SELECT count(*) FROM pg_attribute WHERE attnum > $1", i)
				}

				r2, err := c.Collect(ctx)
				if err != nil {
					t.Fatalf("PG%d + ext 1.11 (after UPDATE): Collect failed: %v", inst.version, err)
				}
				var p2 PgStatStatementsPayload
				if err := json.Unmarshal(r2.JSON, &p2); err != nil {
					t.Fatalf("unmarshal second payload: %v", err)
				}
				if len(p2.Rows) == 0 {
					t.Fatalf("PG%d + ext 1.11: expected non-empty rows after extension UPDATE", inst.version)
				}
			}
		})
	}
}

func TestAutovacuumCount_ReturnsData(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		c := AutovacuumCountCollector(inst.pool, noopPrepareCtx)
		requireRows(t, c)
	})
}

// buildCollectors creates all collectors for a given pool and PG version.
func buildCollectors(pool *pgxpool.Pool, pgMajorVersion int) []CatalogCollector {
	return []CatalogCollector{
		AutovacuumCountCollector(pool, noopPrepareCtx),
		ConnectionStatsCollector(pool, noopPrepareCtx),
		DatabaseSizeCollector(pool, noopPrepareCtx),
		PgAttributeCollector(pool, noopPrepareCtx),
		PgClassCollector(pool, noopPrepareCtx, PgClassConfig{BackfillBatchSize: PgClassBackfillBatchSize}),
		PgConstraintCollector(pool, noopPrepareCtx),
		PgTypeCollector(pool, noopPrepareCtx),
		PgDatabaseCollector(pool, noopPrepareCtx),
		PgIndexCollector(pool, noopPrepareCtx, pgMajorVersion),
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
		PgStatStatementsCollector(pool, noopPrepareCtx, PgStatStatementsConfig{
			DiffLimit:          PgStatStatementsDiffLimit,
			IncludeQueries:     true,
			MaxQueryTextLength: 1000,
		}),
		PgStatSubscriptionCollector(pool, noopPrepareCtx),
		PgStatSubscriptionStatsCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatUserFunctionsCollector(pool, noopPrepareCtx),
		PgStatUserIndexesCollector(pool, noopPrepareCtx, PgStatUserIndexesConfig{CategoryLimit: PgStatUserIndexesCategoryLimit}),
		PgStatUserTablesCollector(pool, noopPrepareCtx, PgStatUserTablesConfig{CategoryLimit: PgStatUserTablesCategoryLimit}),
		PgStatWalCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatWalReceiverCollector(pool, noopPrepareCtx),
		PgStatioUserIndexesCollector(pool, noopPrepareCtx, PgStatioUserIndexesConfig{BackfillBatchSize: PgStatioUserIndexesBatchSize}),
		PgStatioUserTablesCollector(pool, noopPrepareCtx),
		PgStatsCollector(pool, noopPrepareCtx, PgStatsConfig{
			BackfillBatchSize: PgStatsBackfillBatchSize,
			IncludeTableData:  true,
		}),
		TransactionCommitsCollector(pool, noopPrepareCtx),
		UptimeMinutesCollector(pool, noopPrepareCtx),
		WaitEventsCollector(pool, noopPrepareCtx),
	}
}
