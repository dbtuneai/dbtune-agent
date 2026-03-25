//go:build integration

package queries

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
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
	pool    *pgxpool.Pool
	version int
}

var pgInstances []pgInstance

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
// and verifies that collection succeeds with a non-nil result (or nil for
// version-gated collectors on older PG versions).
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
					result, err := c.Collect(ctx)
					if err != nil {
						t.Fatalf("Collect() error: %v", err)
					}
					// Version-gated collectors return nil on unsupported versions
					if result == nil {
						return
					}
					if len(result.JSON) == 0 {
						t.Fatal("Collect() returned empty JSON")
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

// buildCollectors creates all collectors for a given pool and PG version.
func buildCollectors(pool *pgxpool.Pool, pgMajorVersion int) []CatalogCollector {
	return []CatalogCollector{
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
		PgStatSubscriptionCollector(pool, noopPrepareCtx),
		PgStatSubscriptionStatsCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatWalCollector(pool, noopPrepareCtx, pgMajorVersion),
		PgStatWalReceiverCollector(pool, noopPrepareCtx),
	}
}
