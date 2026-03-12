//go:build integration

package pg

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// nextPort allocates unique fixed host ports for health gate test containers,
// so the port survives stop/start cycles (random ports get reassigned).
var nextPort atomic.Int32

func init() {
	nextPort.Store(45500)
}

// startHealthGateContainer spins up a dedicated Postgres container on a fixed
// host port for health gate testing. A fixed port is required because the pool's
// connection string must remain valid across container stop/start cycles.
// The caller is responsible for termination via testcontainers.CleanupContainer.
func startHealthGateContainer(t *testing.T, ctx context.Context) (*postgres.PostgresContainer, *pgxpool.Pool) {
	t.Helper()

	hostPort := fmt.Sprintf("%d", nextPort.Add(1))

	pgContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("healthgate_test"),
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
	require.NoError(t, err, "failed to start postgres container")

	connStr := fmt.Sprintf("postgres://test:test@127.0.0.1:%s/healthgate_test?sslmode=disable", hostPort)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err, "failed to create pool")

	return pgContainer, pool
}

func integrationLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
}

// requireGateOpen polls hg.Check() until it returns nil or the timeout expires.
func requireGateOpen(t *testing.T, hg *HealthGate, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hg.Check() == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, hg.Check(), msg)
}

// ---------------------------------------------------------------------------
// Happy path: gate stays open with healthy database
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_HealthyDB_GateStaysOpen(t *testing.T) {
	// Uses the shared pgInstances from TestMain (pgcatalog_integration_test.go).
	require.NotEmpty(t, pgInstances, "no pgInstances available from TestMain")

	inst := pgInstances[0]
	hg := NewHealthGate(inst.pool, integrationLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg.Start(ctx)
	defer hg.Stop()

	// Gate should be open.
	assert.NoError(t, hg.Check(), "gate should be open with healthy DB")

	// Ping should succeed through the pool.
	assert.NoError(t, inst.pool.Ping(ctx), "pool ping should succeed")

	// Reporting a real query error should NOT trip the gate.
	hg.ReportError(&pgconn.PgError{Code: "42P01", Message: "relation does not exist"})
	assert.NoError(t, hg.Check(), "gate should remain open after query error")
}

// ---------------------------------------------------------------------------
// Gate trips when database stops, recovers when it restarts
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_StopAndRestart(t *testing.T) {
	ctx := context.Background()
	pgContainer, pool := startHealthGateContainer(t, ctx)
	testcontainers.CleanupContainer(t, pgContainer)

	// Verify pool works.
	require.NoError(t, pool.Ping(ctx), "pool should be healthy initially")

	hg := NewHealthGate(pool, integrationLogger())
	gateCtx, gateCancel := context.WithCancel(ctx)
	defer gateCancel()
	hg.Start(gateCtx)
	defer hg.Stop()

	// --- Stop the container ---
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout), "failed to stop container")

	// Force a real connection error by pinging the stopped DB.
	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr, "ping should fail after container stop")

	// Report the real error to the gate.
	hg.ReportError(pingErr)

	// Gate should now be closed.
	err := hg.Check()
	assert.ErrorIs(t, err, ErrDatabaseDown, "gate should be closed after reporting connection error")
	assert.True(t, hg.pinging.Load(), "ping goroutine should be running")

	// --- Restart the container ---
	require.NoError(t, pgContainer.Start(ctx), "failed to restart container")

	// Wait for the gate to reopen. The ping goroutine uses exponential backoff
	// (1s → 2s → 4s → … → 30s max), so after the container is back we may need
	// to wait up to ~maxBackoff before the next ping succeeds.
	requireGateOpen(t, hg, 60*time.Second, "gate should have reopened after container restart")

	// Verify the pool actually works again.
	assert.NoError(t, pool.Ping(ctx), "pool ping should succeed after recovery")
}

// ---------------------------------------------------------------------------
// Gate trips with unreachable host (no container, immediate failure)
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_UnreachableHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pool pointing at a port where nothing is listening.
	pool, err := pgxpool.New(ctx, "postgres://test:test@127.0.0.1:1/nonexistent?sslmode=disable&connect_timeout=1")
	require.NoError(t, err, "pgxpool.New should not fail (lazy connect)")

	hg := NewHealthGate(pool, integrationLogger())
	hg.Start(ctx)
	defer hg.Stop()

	// Gate starts open.
	assert.NoError(t, hg.Check())

	// Try to ping — will fail immediately with connection refused.
	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr)
	assert.True(t, IsConnectionError(pingErr),
		"ping error to unreachable host should be classified as connection error, got: %v", pingErr)

	// Report it.
	hg.ReportError(pingErr)
	assert.ErrorIs(t, hg.Check(), ErrDatabaseDown, "gate should be closed")
	assert.True(t, hg.pinging.Load(), "ping goroutine should be running")
}

// ---------------------------------------------------------------------------
// Multiple collectors all see ErrDatabaseDown when gate is tripped
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_MultipleCollectorsBlocked(t *testing.T) {
	ctx := context.Background()
	pgContainer, pool := startHealthGateContainer(t, ctx)
	testcontainers.CleanupContainer(t, pgContainer)

	require.NoError(t, pool.Ping(ctx), "pool should be healthy")

	hg := NewHealthGate(pool, integrationLogger())
	gateCtx, gateCancel := context.WithCancel(ctx)
	defer gateCancel()
	hg.Start(gateCtx)
	defer hg.Stop()

	// Stop the container and trip the gate.
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr)
	hg.ReportError(pingErr)

	// Simulate 30 collectors calling Check concurrently — all should get ErrDatabaseDown.
	const numCollectors = 30
	errs := make(chan error, numCollectors)
	for i := 0; i < numCollectors; i++ {
		go func() {
			errs <- hg.Check()
		}()
	}

	for i := 0; i < numCollectors; i++ {
		err := <-errs
		assert.ErrorIs(t, err, ErrDatabaseDown,
			"collector %d should see ErrDatabaseDown", i)
	}
}

// ---------------------------------------------------------------------------
// CatalogGetter.prepareCtx integration with real pool
// ---------------------------------------------------------------------------

func TestIntegration_CatalogGetter_PrepareCtx_GatedByHealthGate(t *testing.T) {
	require.NotEmpty(t, pgInstances, "no pgInstances available from TestMain")

	inst := pgInstances[0]
	hg := NewHealthGate(inst.pool, integrationLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg.Start(ctx)
	defer hg.Stop()

	var prepareCalled bool
	g := &CatalogGetter{
		PGPool:         inst.pool,
		PGMajorVersion: inst.version,
		HealthGate:     hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			prepareCalled = true
			return ctx, nil
		},
	}

	// Gate open — prepareCtx should call PrepareContext.
	_, err := g.prepareCtx(ctx)
	require.NoError(t, err)
	assert.True(t, prepareCalled, "PrepareContext should be called when gate is open")

	// Manually close the gate.
	hg.down.Store(true)
	prepareCalled = false

	_, err = g.prepareCtx(ctx)
	assert.ErrorIs(t, err, ErrDatabaseDown)
	assert.False(t, prepareCalled, "PrepareContext should NOT be called when gate is closed")
}

// ---------------------------------------------------------------------------
// Real connection error classification from a stopped container
// ---------------------------------------------------------------------------

func TestIntegration_IsConnectionError_RealErrors(t *testing.T) {
	ctx := context.Background()
	pgContainer, pool := startHealthGateContainer(t, ctx)
	testcontainers.CleanupContainer(t, pgContainer)

	// Stop the container.
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	// The real error from pinging a stopped container should be classified
	// as a connection error.
	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr)
	assert.True(t, IsConnectionError(pingErr),
		"real ping error after container stop should be a connection error, got: %T: %v", pingErr, pingErr)

	// Now also try a query — same classification expected.
	var dummy int
	queryErr := pool.QueryRow(ctx, "SELECT 1").Scan(&dummy)
	require.Error(t, queryErr)

	// Context errors from pool exhaustion are not connection errors, but
	// actual TCP failures should be.
	if !errors.Is(queryErr, context.DeadlineExceeded) && !errors.Is(queryErr, context.Canceled) {
		assert.True(t, IsConnectionError(queryErr),
			"real query error after container stop should be a connection error, got: %T: %v", queryErr, queryErr)
	}
}

// ---------------------------------------------------------------------------
// Gate does NOT trip from a real query error on a healthy DB
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_QueryErrorDoesNotTrip(t *testing.T) {
	require.NotEmpty(t, pgInstances, "no pgInstances available from TestMain")

	inst := pgInstances[0]
	hg := NewHealthGate(inst.pool, integrationLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg.Start(ctx)
	defer hg.Stop()

	// Execute a query that will produce a real PgError (relation does not exist).
	var dummy int
	queryErr := inst.pool.QueryRow(ctx, "SELECT 1 FROM nonexistent_table_xyz_123").Scan(&dummy)
	require.Error(t, queryErr)

	// Verify the error is NOT classified as a connection error.
	assert.False(t, IsConnectionError(queryErr),
		"query error on healthy DB should not be a connection error, got: %T: %v", queryErr, queryErr)

	// Report it — gate should stay open.
	hg.ReportError(queryErr)
	assert.NoError(t, hg.Check(), "gate should remain open after query error")
	assert.False(t, hg.pinging.Load(), "no ping goroutine should have started")
}

// ---------------------------------------------------------------------------
// Gate recovery: stop → gate trips → restart → gate reopens → query works
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_FullRecoveryCycle(t *testing.T) {
	ctx := context.Background()
	pgContainer, pool := startHealthGateContainer(t, ctx)
	testcontainers.CleanupContainer(t, pgContainer)

	hg := NewHealthGate(pool, integrationLogger())
	gateCtx, gateCancel := context.WithCancel(ctx)
	defer gateCancel()
	hg.Start(gateCtx)
	defer hg.Stop()

	// 1. Healthy — query works, gate open.
	var result int
	require.NoError(t, pool.QueryRow(ctx, "SELECT 42").Scan(&result))
	assert.Equal(t, 42, result)
	assert.NoError(t, hg.Check())

	// 2. Stop DB → trip gate.
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr)
	hg.ReportError(pingErr)
	assert.ErrorIs(t, hg.Check(), ErrDatabaseDown)

	// 3. Restart DB → gate recovers automatically via ping goroutine.
	require.NoError(t, pgContainer.Start(ctx))
	requireGateOpen(t, hg, 60*time.Second, "gate should have reopened after restart")

	// 4. Query works again.
	require.NoError(t, pool.QueryRow(ctx, "SELECT 42").Scan(&result))
	assert.Equal(t, 42, result)

	// 5. Report a query error — gate should NOT re-trip.
	hg.ReportError(&pgconn.PgError{Code: "42601", Message: "syntax error"})
	assert.NoError(t, hg.Check(), "gate should stay open after query error")

	// 6. Stop again → trip again (proves re-tripping works after full recovery).
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	pingErr = pool.Ping(ctx)
	require.Error(t, pingErr)
	hg.ReportError(pingErr)
	assert.ErrorIs(t, hg.Check(), ErrDatabaseDown, "gate should re-trip on second outage")

	// 7. Final restart → gate recovers again.
	require.NoError(t, pgContainer.Start(ctx))
	requireGateOpen(t, hg, 60*time.Second, "gate should recover a second time")

	// Pool works.
	assert.NoError(t, pool.Ping(ctx), "pool should be healthy after second recovery")
}

// ---------------------------------------------------------------------------
// Verify gate context cancellation stops ping goroutine even when DB is down
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_ContextCancelStopsPinging(t *testing.T) {
	ctx := context.Background()
	pgContainer, pool := startHealthGateContainer(t, ctx)
	testcontainers.CleanupContainer(t, pgContainer)

	hg := NewHealthGate(pool, integrationLogger())
	gateCtx, gateCancel := context.WithCancel(ctx)
	hg.Start(gateCtx)

	// Stop DB and trip gate.
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	pingErr := pool.Ping(ctx)
	require.Error(t, pingErr)
	hg.ReportError(pingErr)
	assert.True(t, hg.pinging.Load(), "should be pinging")

	// Cancel the gate context — goroutine should exit.
	gateCancel()

	deadline := time.Now().Add(5 * time.Second)
	for hg.pinging.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.False(t, hg.pinging.Load(), "ping goroutine should have stopped after context cancel")

	// Gate stays down (nobody recovered it).
	assert.ErrorIs(t, hg.Check(), ErrDatabaseDown)
}

// ---------------------------------------------------------------------------
// Verify ReportError with real connection errors from multiple pg versions
// ---------------------------------------------------------------------------

func TestIntegration_HealthGate_RealConnectionErrorsAcrossVersions(t *testing.T) {
	// Use the shared pgInstances to verify IsConnectionError works with
	// real errors from each PG version.
	require.NotEmpty(t, pgInstances, "no pgInstances available from TestMain")

	for _, inst := range pgInstances {
		t.Run(fmt.Sprintf("pg%d", inst.version), func(t *testing.T) {
			ctx := context.Background()

			// Valid query error — should NOT be a connection error.
			var dummy int
			queryErr := inst.pool.QueryRow(ctx, "SELECT 1 FROM this_table_does_not_exist_abc").Scan(&dummy)
			require.Error(t, queryErr)
			assert.False(t, IsConnectionError(queryErr),
				"PG%d: query error should not be classified as connection error: %T: %v",
				inst.version, queryErr, queryErr)
		})
	}
}
