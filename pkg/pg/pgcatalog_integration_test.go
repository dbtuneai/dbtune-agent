//go:build integration

package pg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type pgInstance struct {
	pool    *pgxpool.Pool
	version int
}

var pgInstances []pgInstance

// bufLogger is a testcontainers Logger that writes to a buffer.
type bufLogger struct {
	buf *bytes.Buffer
}

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

func TestMain(m *testing.M) {
	// Capture all library log output; only print on failure.
	var logBuf bytes.Buffer
	tclog.SetDefault(&bufLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()
	versions := []int{13, 14, 15, 16, 17, 18}

	seedPath, err := filepath.Abs("testdata/seed.sql")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve seed.sql path: %v\n", err)
		os.Exit(1)
	}

	var containers []*postgres.PostgresContainer

	cleanup := func() {
		for _, c := range containers {
			_ = c.Terminate(ctx)
		}
	}

	for _, v := range versions {
		image := fmt.Sprintf("postgres:%d-alpine", v)
		hostPort := fmt.Sprintf("%d", 45400+v)

		pgContainer, err := postgres.Run(ctx, image,
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			postgres.WithInitScripts(seedPath),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Cmd: []string{"-c", "shared_preload_libraries=pg_stat_statements"},
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

		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create pool for postgres %d: %v\n", v, err)
			cleanup()
			os.Exit(1)
		}

		pgInstances = append(pgInstances, pgInstance{pool: pool, version: v})
	}

	code := m.Run()

	// Cleanup
	for _, inst := range pgInstances {
		inst.pool.Close()
	}
	cleanup()

	// Print captured logs only if tests failed
	if code != 0 {
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
	}

	os.Exit(code)
}

// runSimpleCollectorTest runs a collector against all PG versions and asserts no error.
// If expectNonEmpty is true, also asserts that at least one row was returned.
func runSimpleCollectorTest[T any](
	t *testing.T,
	name string,
	collect func(*pgxpool.Pool, context.Context) ([]T, error),
	expectNonEmpty bool,
) {
	t.Helper()
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("%s/pg%d", name, pg.version), func(t *testing.T) {
			rows, err := collect(pg.pool, context.Background())
			require.NoError(t, err)
			if expectNonEmpty {
				assert.NotEmpty(t, rows, "expected rows from %s on PG %d", name, pg.version)
			}
			for i, row := range rows {
				_, err := json.Marshal(row)
				require.NoError(t, err, "json.Marshal failed for %s row %d on PG %d", name, i, pg.version)
			}
		})
	}
}

// runVersionGatedTest runs a version-gated collector against all PG versions.
// Asserts nil result below minVersion and non-nil at/above.
func runVersionGatedTest[T any](
	t *testing.T,
	name string,
	minVersion int,
	collect func(*pgxpool.Pool, context.Context, int) ([]T, error),
) {
	t.Helper()
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("%s/pg%d", name, pg.version), func(t *testing.T) {
			rows, err := collect(pg.pool, context.Background(), pg.version)
			require.NoError(t, err)
			if pg.version >= minVersion {
				assert.NotNil(t, rows, "expected data on PG %d (>= %d)", pg.version, minVersion)
			} else {
				assert.Nil(t, rows, "expected nil on PG %d (< %d)", pg.version, minVersion)
			}
			for i, row := range rows {
				_, err := json.Marshal(row)
				require.NoError(t, err, "json.Marshal failed for %s row %d on PG %d", name, i, pg.version)
			}
		})
	}
}

func TestSimpleCollectors(t *testing.T) {
	runSimpleCollectorTest(t, "pg_stats", func(pool *pgxpool.Pool, ctx context.Context) ([]queries.PgStatsRow, error) {
		return queries.CollectPgStats(pool, ctx, queries.PgStatsBackfillQuery(queries.PgStatsBackfillBatchSize, 0), true)
	}, true)
	runSimpleCollectorTest(t, "pg_stat_user_tables", func(pool *pgxpool.Pool, ctx context.Context) ([]queries.PgStatUserTableRow, error) {
		return queries.CollectPgStatUserTables(pool, ctx, queries.BuildPgStatUserTablesQuery(queries.PgStatUserTablesCategoryLimit))
	}, true)
	runSimpleCollectorTest(t, "pg_class", func(pool *pgxpool.Pool, ctx context.Context) ([]queries.PgClassRow, error) {
		return queries.CollectPgClass(pool, ctx, queries.PgClassBackfillQuery(queries.PgClassBackfillBatchSize, 0))
	}, true)
	runSimpleCollectorTest(t, "pg_stat_activity", queries.CollectPgStatActivity, true)
	runSimpleCollectorTest(t, "pg_stat_database", queries.CollectPgStatDatabase, true)
	runSimpleCollectorTest(t, "pg_stat_database_conflicts", queries.CollectPgStatDatabaseConflicts, true)
	runSimpleCollectorTest(t, "pg_stat_archiver", queries.CollectPgStatArchiver, true)
	runSimpleCollectorTest(t, "pg_stat_bgwriter", queries.CollectPgStatBgwriter, true)
	runSimpleCollectorTest(t, "pg_stat_replication", queries.CollectPgStatReplication, false)
	runSimpleCollectorTest(t, "pg_stat_slru", queries.CollectPgStatSlru, true)
	runSimpleCollectorTest(t, "pg_stat_user_indexes", func(pool *pgxpool.Pool, ctx context.Context) ([]queries.PgStatUserIndexesRow, error) {
		return queries.CollectPgStatUserIndexes(pool, ctx, queries.BuildPgStatUserIndexesQuery(queries.PgStatUserIndexesCategoryLimit))
	}, true)
	runSimpleCollectorTest(t, "pg_statio_user_tables", queries.CollectPgStatioUserTables, false)
	runSimpleCollectorTest(t, "pg_statio_user_indexes", func(pool *pgxpool.Pool, ctx context.Context) ([]queries.PgStatioUserIndexesRow, error) {
		return queries.CollectPgStatioUserIndexes(pool, ctx, queries.BuildPgStatioUserIndexesQuery(queries.PgStatioUserIndexesCategoryLimit))
	}, false)
	runSimpleCollectorTest(t, "pg_stat_user_functions", queries.CollectPgStatUserFunctions, false)
	runSimpleCollectorTest(t, "pg_locks", queries.CollectPgLocks, false)
	runSimpleCollectorTest(t, "pg_stat_progress_vacuum", queries.CollectPgStatProgressVacuum, false)
	runSimpleCollectorTest(t, "pg_stat_progress_analyze", queries.CollectPgStatProgressAnalyze, false)
	runSimpleCollectorTest(t, "pg_stat_progress_create_index", queries.CollectPgStatProgressCreateIndex, false)
	runSimpleCollectorTest(t, "pg_prepared_xacts", queries.CollectPgPreparedXacts, false)
	runSimpleCollectorTest(t, "pg_replication_slots", queries.CollectPgReplicationSlots, false)
	runSimpleCollectorTest(t, "pg_index", queries.CollectPgIndex, true)
	runSimpleCollectorTest(t, "pg_stat_wal_receiver", queries.CollectPgStatWalReceiver, false)
	runSimpleCollectorTest(t, "pg_stat_subscription", queries.CollectPgStatSubscription, false)
}

func TestVersionGatedCollectors(t *testing.T) {
	runVersionGatedTest(t, "pg_stat_checkpointer", 17, queries.CollectPgStatCheckpointer)
	runVersionGatedTest(t, "pg_stat_wal", 14, queries.CollectPgStatWal)
	runVersionGatedTest(t, "pg_stat_io", 16, queries.CollectPgStatIO)
	runVersionGatedTest(t, "pg_stat_replication_slots", 14, queries.CollectPgStatReplicationSlots)
	runVersionGatedTest(t, "pg_stat_recovery_prefetch", 15, queries.CollectPgStatRecoveryPrefetch)
	runVersionGatedTest(t, "pg_stat_subscription_stats", 15, queries.CollectPgStatSubscriptionStats)
}

func TestPgStatStatementsCollector(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ctx := context.Background()
			prepareCtx := func(ctx context.Context) (context.Context, error) { return ctx, nil }

			// Test with query text included
			collector := queries.PgStatStatementsCollector(pg.pool, prepareCtx, true, 1000, queries.PgStatStatementsDiffLimit, pg.version)
			assert.Equal(t, "pg_stat_statements", collector.Name)

			// First tick: should return rows but no deltas
			result1, err := collector.Collect(ctx)
			require.NoError(t, err)
			payload1, ok := result1.(*queries.PgStatStatementsPayload)
			require.True(t, ok, "expected *PgStatStatementsPayload")
			assert.NotEmpty(t, payload1.Rows, "first tick should have rows on PG %d", pg.version)
			assert.Empty(t, payload1.Deltas, "first tick should have no deltas")
			assert.Zero(t, payload1.DeltaCount)

			// Verify rows are JSON-marshalable and have required fields
			for i, row := range payload1.Rows {
				assert.NotNil(t, row.UserID, "userid should not be nil, row %d", i)
				assert.NotNil(t, row.DbID, "dbid should not be nil, row %d", i)
				assert.NotNil(t, row.QueryID, "queryid should not be nil, row %d", i)
				assert.NotNil(t, row.Calls, "calls should not be nil, row %d", i)
				_, err := json.Marshal(row)
				require.NoError(t, err, "json.Marshal failed for row %d on PG %d", i, pg.version)
			}

			// Generate some query activity between snapshots
			for i := 0; i < 5; i++ {
				_, _ = pg.pool.Exec(ctx, fmt.Sprintf("SELECT count(*) FROM orders WHERE customer_id = %d", i+1))
			}

			// Second tick: should have both rows and deltas
			result2, err := collector.Collect(ctx)
			require.NoError(t, err)
			payload2, ok := result2.(*queries.PgStatStatementsPayload)
			require.True(t, ok)
			assert.NotEmpty(t, payload2.Rows, "second tick should have rows on PG %d", pg.version)
			// Deltas should exist since we ran queries between ticks
			assert.NotEmpty(t, payload2.Deltas, "second tick should have deltas on PG %d", pg.version)
			assert.Greater(t, payload2.DeltaCount, 0)
			assert.Greater(t, payload2.AverageQueryRuntime, 0.0)

			// Verify delta rows have positive diff fields
			for _, delta := range payload2.Deltas {
				assert.NotNil(t, delta.Calls)
				assert.NotNil(t, delta.TotalExecTime)
				assert.NotNil(t, delta.Rows)
				assert.Greater(t, int64(*delta.Calls), int64(0), "delta calls should be positive")
				assert.Greater(t, float64(*delta.TotalExecTime), 0.0, "delta exec time should be positive")
				assert.Greater(t, int64(*delta.Rows), int64(0), "delta rows should be positive")
			}

			// Full payload should marshal cleanly
			_, err = json.Marshal(payload2)
			require.NoError(t, err)
		})
	}
}

func TestPgStatStatementsCollectorNoQueryText(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ctx := context.Background()
			prepareCtx := func(ctx context.Context) (context.Context, error) { return ctx, nil }

			collector := queries.PgStatStatementsCollector(pg.pool, prepareCtx, false, 1000, queries.PgStatStatementsDiffLimit, pg.version)
			result, err := collector.Collect(ctx)
			require.NoError(t, err)
			payload, ok := result.(*queries.PgStatStatementsPayload)
			require.True(t, ok)
			assert.NotEmpty(t, payload.Rows)

			// When includeQueries=false, query and query_len should be nil
			for i, row := range payload.Rows {
				assert.Nil(t, row.Query, "query should be nil when includeQueries=false, row %d", i)
				assert.Nil(t, row.QueryLen, "query_len should be nil when includeQueries=false, row %d", i)
			}
		})
	}
}

func TestPgStatStatementsQueryTruncation(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ctx := context.Background()
			prepareCtx := func(ctx context.Context) (context.Context, error) { return ctx, nil }

			// Use a very short max length to test truncation
			collector := queries.PgStatStatementsCollector(pg.pool, prepareCtx, true, 10, queries.PgStatStatementsDiffLimit, pg.version)
			result, err := collector.Collect(ctx)
			require.NoError(t, err)
			payload, ok := result.(*queries.PgStatStatementsPayload)
			require.True(t, ok)
			assert.NotEmpty(t, payload.Rows)

			for _, row := range payload.Rows {
				if row.Query != nil {
					assert.LessOrEqual(t, len(string(*row.Query)), 10,
						"query text should be truncated to max length")
				}
			}
		})
	}
}

func TestPgStatStatementsVersionSpecificFields(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ctx := context.Background()
			prepareCtx := func(ctx context.Context) (context.Context, error) { return ctx, nil }

			collector := queries.PgStatStatementsCollector(pg.pool, prepareCtx, true, 1000, queries.PgStatStatementsDiffLimit, pg.version)
			result, err := collector.Collect(ctx)
			require.NoError(t, err)
			payload, ok := result.(*queries.PgStatStatementsPayload)
			require.True(t, ok)
			require.NotEmpty(t, payload.Rows)

			row := payload.Rows[0]

			// All versions should have shared_blk_read_time (aliased from blk_read_time on PG13-16)
			assert.NotNil(t, row.SharedBlkReadTime, "shared_blk_read_time should be present on PG %d", pg.version)
			assert.NotNil(t, row.SharedBlkWriteTime, "shared_blk_write_time should be present on PG %d", pg.version)

			// PG14+ should have toplevel
			if pg.version >= 14 {
				assert.NotNil(t, row.TopLevel, "toplevel should be present on PG %d", pg.version)
			}

			// PG15+ should have temp_blk_read_time and JIT fields
			if pg.version >= 15 {
				assert.NotNil(t, row.TempBlkReadTime, "temp_blk_read_time should be present on PG %d", pg.version)
				assert.NotNil(t, row.TempBlkWriteTime, "temp_blk_write_time should be present on PG %d", pg.version)
			}

			// PG17+ should have local_blk_read_time/local_blk_write_time
			if pg.version >= 17 {
				assert.NotNil(t, row.LocalBlkReadTime, "local_blk_read_time should be present on PG %d", pg.version)
				assert.NotNil(t, row.LocalBlkWriteTime, "local_blk_write_time should be present on PG %d", pg.version)
			} else {
				assert.Nil(t, row.LocalBlkReadTime, "local_blk_read_time should be nil on PG %d", pg.version)
				assert.Nil(t, row.LocalBlkWriteTime, "local_blk_write_time should be nil on PG %d", pg.version)
			}
		})
	}
}

func TestCollectDDL(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ddl, err := queries.CollectDDL(pg.pool, context.Background())
			require.NoError(t, err)
			assert.Contains(t, ddl, "CREATE TABLE orders")
			assert.Contains(t, ddl, "CREATE TABLE customers")
			assert.Contains(t, ddl, "idx_orders_customer_id")
			assert.Contains(t, ddl, "idx_orders_created_at")
			assert.Contains(t, ddl, "idx_customers_name")
			assert.Contains(t, ddl, "PRIMARY KEY")
			assert.Contains(t, ddl, "UNIQUE")

			// Hash should be a 64-char hex SHA-256
			hash := queries.HashDDL(ddl)
			assert.Len(t, hash, 64)
		})
	}
}

// TestCollectDDL_WithoutPgCatalogOnSearchPath verifies that the DDL query
// works even when pg_catalog is not on the search_path, which happens in
// some managed PG environments. This caught a real bug where unqualified
// pg_format_type() failed to resolve.
func TestCollectDDL_WithoutPgCatalogOnSearchPath(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ctx := context.Background()

			// Pin a single connection so SET applies to our query.
			conn, err := pg.pool.Acquire(ctx)
			require.NoError(t, err)
			defer conn.Release()

			// Remove pg_catalog from search_path for this session
			_, err = conn.Exec(ctx, "SET search_path = 'public'")
			require.NoError(t, err)

			var ddl string
			err = conn.QueryRow(ctx, queries.CollectDDLQuery).Scan(&ddl)
			require.NoError(t, err)
			assert.Contains(t, ddl, "CREATE TABLE orders")
		})
	}
}
