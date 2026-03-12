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

	"github.com/dbtuneai/agent/pkg/pg/catalog"
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
	runSimpleCollectorTest(t, "pg_stats", func(pool *pgxpool.Pool, ctx context.Context) ([]catalog.PgStatsRow, error) {
		return catalog.CollectPgStats(pool, ctx, catalog.PgStatsBackfillQuery(catalog.PgStatsBackfillBatchSize, 0))
	}, true)
	runSimpleCollectorTest(t, "pg_stat_user_tables", catalog.CollectPgStatUserTables, true)
	runSimpleCollectorTest(t, "pg_class", func(pool *pgxpool.Pool, ctx context.Context) ([]catalog.PgClassRow, error) {
		return catalog.CollectPgClass(pool, ctx, catalog.PgClassBackfillQuery(catalog.PgClassBackfillBatchSize, 0))
	}, true)
	runSimpleCollectorTest(t, "pg_stat_activity", catalog.CollectPgStatActivity, true)
	runSimpleCollectorTest(t, "pg_stat_database", catalog.CollectPgStatDatabase, true)
	runSimpleCollectorTest(t, "pg_stat_database_conflicts", catalog.CollectPgStatDatabaseConflicts, true)
	runSimpleCollectorTest(t, "pg_stat_archiver", catalog.CollectPgStatArchiver, true)
	runSimpleCollectorTest(t, "pg_stat_bgwriter", catalog.CollectPgStatBgwriter, true)
	runSimpleCollectorTest(t, "pg_stat_replication", catalog.CollectPgStatReplication, false)
	runSimpleCollectorTest(t, "pg_stat_slru", catalog.CollectPgStatSlru, true)
	runSimpleCollectorTest(t, "pg_stat_user_indexes", catalog.CollectPgStatUserIndexes, true)
	runSimpleCollectorTest(t, "pg_statio_user_tables", catalog.CollectPgStatioUserTables, false)
	runSimpleCollectorTest(t, "pg_statio_user_indexes", catalog.CollectPgStatioUserIndexes, false)
	runSimpleCollectorTest(t, "pg_stat_user_functions", catalog.CollectPgStatUserFunctions, false)
	runSimpleCollectorTest(t, "pg_locks", catalog.CollectPgLocks, false)
	runSimpleCollectorTest(t, "pg_stat_progress_vacuum", catalog.CollectPgStatProgressVacuum, false)
	runSimpleCollectorTest(t, "pg_stat_progress_analyze", catalog.CollectPgStatProgressAnalyze, false)
	runSimpleCollectorTest(t, "pg_stat_progress_create_index", catalog.CollectPgStatProgressCreateIndex, false)
	runSimpleCollectorTest(t, "pg_prepared_xacts", catalog.CollectPgPreparedXacts, false)
	runSimpleCollectorTest(t, "pg_replication_slots", catalog.CollectPgReplicationSlots, false)
	runSimpleCollectorTest(t, "pg_index", catalog.CollectPgIndex, true)
	runSimpleCollectorTest(t, "pg_stat_wal_receiver", catalog.CollectPgStatWalReceiver, false)
	runSimpleCollectorTest(t, "pg_stat_subscription", catalog.CollectPgStatSubscription, false)
}

func TestVersionGatedCollectors(t *testing.T) {
	runVersionGatedTest(t, "pg_stat_checkpointer", 17, catalog.CollectPgStatCheckpointer)
	runVersionGatedTest(t, "pg_stat_wal", 14, catalog.CollectPgStatWal)
	runVersionGatedTest(t, "pg_stat_io", 16, catalog.CollectPgStatIO)
	runVersionGatedTest(t, "pg_stat_replication_slots", 14, catalog.CollectPgStatReplicationSlots)
	runVersionGatedTest(t, "pg_stat_recovery_prefetch", 15, catalog.CollectPgStatRecoveryPrefetch)
	runVersionGatedTest(t, "pg_stat_subscription_stats", 15, catalog.CollectPgStatSubscriptionStats)
}

func TestCollectDDL(t *testing.T) {
	for _, pg := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", pg.version), func(t *testing.T) {
			ddl, err := CollectDDL(pg.pool, context.Background())
			require.NoError(t, err)
			assert.Contains(t, ddl, "CREATE TABLE orders")
			assert.Contains(t, ddl, "CREATE TABLE customers")
			assert.Contains(t, ddl, "idx_orders_customer_id")
			assert.Contains(t, ddl, "idx_orders_created_at")
			assert.Contains(t, ddl, "idx_customers_name")
			assert.Contains(t, ddl, "PRIMARY KEY")
			assert.Contains(t, ddl, "UNIQUE")

			// Hash should be a 64-char hex SHA-256
			hash := HashDDL(ddl)
			assert.Len(t, hash, 64)
		})
	}
}
