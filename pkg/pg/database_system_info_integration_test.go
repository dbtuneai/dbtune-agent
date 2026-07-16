//go:build integration

package pg

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Integration tests for the system-info database queries added for DBT-2064:
// CurrentDatabase, UserDatabaseCount and the DatabaseSystemInfo helper that
// wraps them into metrics.FlatValues.

const dbSysInfoIntegrationPort = "45410" // distinct from other integration test ports

var dbSysInfoConnStr string

type dbSysInfoLogger struct{ buf *bytes.Buffer }

func (l *dbSysInfoLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

func TestMain(m *testing.M) {
	var logBuf bytes.Buffer
	tclog.SetDefault(&dbSysInfoLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.PortBindings = nat.PortMap{
						"5432/tcp": []nat.PortBinding{
							{HostIP: "127.0.0.1", HostPort: dbSysInfoIntegrationPort},
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
		fmt.Fprintf(os.Stderr, "failed to start postgres: %v\n", err)
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
		os.Exit(1)
	}
	dbSysInfoConnStr, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		_ = ctr.Terminate(ctx)
		os.Exit(1)
	}

	code := m.Run()
	_ = ctr.Terminate(ctx)
	if code != 0 {
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
	}
	os.Exit(code)
}

// poolTo opens a pool connected to dbName on the test container. The caller is
// responsible for closing it (important for tests that later DROP the database).
func poolTo(t *testing.T, dbName string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbSysInfoConnStr)
	require.NoError(t, err)
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	return pool
}

// adminExec runs a statement (e.g. CREATE/DROP DATABASE) against the default
// testdb using the superuser role.
func adminExec(t *testing.T, sql string) {
	t.Helper()
	pool := poolTo(t, "testdb")
	defer pool.Close()
	_, err := pool.Exec(context.Background(), sql)
	require.NoError(t, err)
}

func TestCurrentDatabase(t *testing.T) {
	t.Run("returns the connected database name", func(t *testing.T) {
		pool := poolTo(t, "testdb")
		defer pool.Close()

		got, err := CurrentDatabase(pool)
		require.NoError(t, err)
		require.Equal(t, "testdb", got)
	})

	// Edge case: a database whose name needs quoting (uppercase, space, dash,
	// digits). current_database() must return the exact stored name verbatim,
	// with no case-folding or escaping artifacts.
	t.Run("returns quoted/special-character names verbatim", func(t *testing.T) {
		const weird = `Mixed Case-DB_123`
		adminExec(t, fmt.Sprintf(`CREATE DATABASE "%s"`, weird))
		defer adminExec(t, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, weird))

		pool := poolTo(t, weird)
		got, err := CurrentDatabase(pool)
		pool.Close() // close before the deferred DROP so it isn't blocked
		require.NoError(t, err)
		require.Equal(t, weird, got)
	})

	// Edge case: the function must reflect whichever database the pool is bound
	// to, not a hard-coded or cached value.
	t.Run("tracks the pool's database, not a constant", func(t *testing.T) {
		adminExec(t, `CREATE DATABASE other_db`)
		defer adminExec(t, `DROP DATABASE IF EXISTS other_db WITH (FORCE)`)

		poolA := poolTo(t, "testdb")
		defer poolA.Close()
		poolB := poolTo(t, "other_db")

		gotA, err := CurrentDatabase(poolA)
		require.NoError(t, err)
		gotB, err := CurrentDatabase(poolB)
		poolB.Close()
		require.NoError(t, err)

		require.Equal(t, "testdb", gotA)
		require.Equal(t, "other_db", gotB)
	})
}

func TestUserDatabaseCount(t *testing.T) {
	pool := poolTo(t, "testdb")
	defer pool.Close()
	ctx := context.Background()

	baseline, err := UserDatabaseCount(pool)
	require.NoError(t, err)
	// A fresh cluster always has at least postgres + testdb connectable.
	require.GreaterOrEqual(t, baseline, 2)

	// Invariant: the built-in templates (template0, template1) exist but must
	// never be counted, so the raw pg_database row count exceeds our count.
	t.Run("excludes built-in template databases", func(t *testing.T) {
		var rawTotal int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_database`).Scan(&rawTotal))
		require.Greater(t, rawTotal, baseline, "template0/template1 should be excluded from the user count")
	})

	// Edge case: a normal user database increments the count by exactly one.
	t.Run("counts a new user database", func(t *testing.T) {
		adminExec(t, `CREATE DATABASE counted_db`)
		defer adminExec(t, `DROP DATABASE IF EXISTS counted_db WITH (FORCE)`)

		got, err := UserDatabaseCount(pool)
		require.NoError(t, err)
		require.Equal(t, baseline+1, got)
	})

	// Edge case: an explicit template database (datistemplate = true) must be
	// excluded even though it is otherwise a normal database.
	t.Run("excludes a user-created template database", func(t *testing.T) {
		adminExec(t, `CREATE DATABASE tmpl_db IS_TEMPLATE true`)
		// A template database cannot be dropped directly; clear the flag first.
		defer func() {
			adminExec(t, `ALTER DATABASE tmpl_db IS_TEMPLATE false`)
			adminExec(t, `DROP DATABASE IF EXISTS tmpl_db WITH (FORCE)`)
		}()

		got, err := UserDatabaseCount(pool)
		require.NoError(t, err)
		require.Equal(t, baseline, got, "template database must not be counted")
	})

	// Edge case: a database that disallows connections (datallowconn = false)
	// must be excluded.
	t.Run("excludes a database with connections disallowed", func(t *testing.T) {
		adminExec(t, `CREATE DATABASE noconn_db ALLOW_CONNECTIONS false`)
		defer adminExec(t, `DROP DATABASE IF EXISTS noconn_db WITH (FORCE)`)

		got, err := UserDatabaseCount(pool)
		require.NoError(t, err)
		require.Equal(t, baseline, got, "no-connection database must not be counted")
	})

	// Edge case: dropping a counted database returns the count to baseline.
	t.Run("decrements when a database is dropped", func(t *testing.T) {
		adminExec(t, `CREATE DATABASE transient_db`)
		afterCreate, err := UserDatabaseCount(pool)
		require.NoError(t, err)
		require.Equal(t, baseline+1, afterCreate)

		adminExec(t, `DROP DATABASE transient_db WITH (FORCE)`)
		afterDrop, err := UserDatabaseCount(pool)
		require.NoError(t, err)
		require.Equal(t, baseline, afterDrop)
	})
}

func TestDatabaseSystemInfo(t *testing.T) {
	pool := poolTo(t, "testdb")
	defer pool.Close()

	fvs, err := DatabaseSystemInfo(pool)
	require.NoError(t, err)
	require.Len(t, fvs, 2)

	byKey := make(map[string]metrics.FlatValue, len(fvs))
	for _, fv := range fvs {
		byKey[fv.Key] = fv
	}

	wantDB, err := CurrentDatabase(pool)
	require.NoError(t, err)
	wantCount, err := UserDatabaseCount(pool)
	require.NoError(t, err)

	t.Run("current database flat value", func(t *testing.T) {
		fv, ok := byKey[metrics.PGCurrentDatabase.Key]
		require.True(t, ok, "pg_current_database missing")
		require.Equal(t, metrics.String, fv.Type)
		require.Equal(t, wantDB, fv.Value)
	})

	t.Run("database count flat value", func(t *testing.T) {
		fv, ok := byKey[metrics.PGDatabaseCount.Key]
		require.True(t, ok, "pg_database_count missing")
		require.Equal(t, metrics.Int, fv.Type)
		require.Equal(t, wantCount, fv.Value)
	})
}
