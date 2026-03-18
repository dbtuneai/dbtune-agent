//go:build integration

package healthgatetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const integrationPort = "45399"

var (
	pgContainer testcontainers.Container
	connStr     string
)

func TestMain(m *testing.M) {
	var logBuf bytes.Buffer
	tclog.SetDefault(&bufLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.PortBindings = nat.PortMap{
						"5432/tcp": []nat.PortBinding{
							{HostIP: "127.0.0.1", HostPort: integrationPort},
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
	pgContainer = ctr

	connStr, err = ctr.ConnectionString(ctx, "sslmode=disable")
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

type bufLogger struct {
	buf *bytes.Buffer
}

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

// pgQueryCollector returns a MetricCollector that runs a real query against the pool.
func pgQueryCollector(key string, pool *pgxpool.Pool) agent.MetricCollector {
	return agent.MetricCollector{
		Key: key,
		Collector: func(ctx context.Context, state *agent.MetricsState) error {
			var result int
			err := pool.QueryRow(ctx, "SELECT 1").Scan(&result)
			if err != nil {
				return fmt.Errorf("%s: query failed: %w", key, err)
			}
			state.AddMetric(metrics.FlatValue{Key: key, Value: result, Type: "int"})
			return nil
		},
	}
}

// newTestAgent creates a CommonAgent with the given pool and collectors wired to the HealthGate.
func newTestAgent(pool *pgxpool.Pool, hg *agent.HealthGate, collectors []agent.MetricCollector) *agent.CommonAgent {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	a := &agent.CommonAgent{
		AgentID:   "integration-test",
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test",
		MetricsState: agent.MetricsState{
			Collectors: collectors,
			Mutex:      &sync.Mutex{},
			Metrics:    []metrics.FlatValue{},
		},
		CollectionTimeout: 10 * time.Second,
		IndividualTimeout: 5 * time.Second,
		HealthGate:        hg,
	}
	a.WithLogger(logger)
	return a
}

func TestIntegration_HealthGate_CollectorsSucceedWhenDBUp(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	defer pool.Close()

	logger := logrus.New()
	hg := agent.NewHealthGate(pool, pg.IsConnectionError, logger)
	defer hg.Stop()

	a := newTestAgent(pool, hg, []agent.MetricCollector{
		pgQueryCollector("c1", pool),
		pgQueryCollector("c2", pool),
		pgQueryCollector("c3", pool),
	})

	result, err := a.GetMetrics()
	assert.NoError(t, err)
	assert.Len(t, result, 3, "all 3 collectors should produce metrics")
}

func TestIntegration_HealthGate_BlocksCollectorsWhenDBDown(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	defer pool.Close()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	hg := agent.NewHealthGate(pool, pg.IsConnectionError, logger)
	defer hg.Stop()

	const numCollectors = 10
	var collectorCalls int
	var mu sync.Mutex

	// Build collectors that both track call count and produce a metric.
	makeCollector := func(key string) agent.MetricCollector {
		return agent.MetricCollector{
			Key: key,
			Collector: func(ctx context.Context, state *agent.MetricsState) error {
				mu.Lock()
				collectorCalls++
				mu.Unlock()
				var result int
				err := pool.QueryRow(ctx, "SELECT 1").Scan(&result)
				if err != nil {
					return fmt.Errorf("%s: %w", key, err)
				}
				state.AddMetric(metrics.FlatValue{Key: key, Value: result, Type: "int"})
				return nil
			},
		}
	}

	collectors := make([]agent.MetricCollector, numCollectors)
	for i := range numCollectors {
		collectors[i] = makeCollector(fmt.Sprintf("collector_%d", i))
	}

	a := newTestAgent(pool, hg, collectors)

	// Verify the pool is working before starting.
	require.NoError(t, pool.Ping(ctx), "pool should be able to ping before phase 1")

	// --- Phase 1: DB is up, collectors work ---
	result, err := a.GetMetrics()
	require.NoError(t, err)
	require.Len(t, result, numCollectors, "phase 1: all collectors should produce metrics")

	// --- Phase 2: Stop the database ---
	stopTimeout := 5 * time.Second
	err = pgContainer.Stop(ctx, &stopTimeout)
	require.NoError(t, err)

	// The pool's existing connections are now broken. Run GetMetrics so
	// collectors hit real connection errors and trip the gate.
	mu.Lock()
	collectorCalls = 0
	mu.Unlock()

	result, err = a.GetMetrics()
	// Some collectors fail with real pg errors. The gate should trip.
	t.Logf("phase 2: got %d metrics, err=%v", len(result), err)

	// If the gate was already tripped at the Check() at the top of
	// GetMetrics, we get ErrDatabaseDown immediately. Otherwise,
	// collectors ran and at least one failed, tripping the gate.
	if err == nil {
		assert.True(t, errors.Is(hg.Check(), agent.ErrDatabaseDown),
			"gate should be tripped after collectors hit connection errors")
	}

	// --- Phase 3: Gate is now down — call GetMetrics again ---
	// Key assertion: single ErrDatabaseDown error, zero collectors executed.
	mu.Lock()
	collectorCalls = 0
	mu.Unlock()

	result, err = a.GetMetrics()
	assert.ErrorIs(t, err, agent.ErrDatabaseDown, "should return exactly one ErrDatabaseDown")
	assert.Nil(t, result, "no metrics when gate is down")

	mu.Lock()
	calls := collectorCalls
	mu.Unlock()
	assert.Equal(t, 0, calls, "zero collectors should have executed when gate is down")

	// --- Phase 4: Restart the database, verify recovery ---
	err = pgContainer.Start(ctx)
	require.NoError(t, err)

	// Wait for the health gate's ping goroutine to detect recovery.
	deadline := time.Now().Add(60 * time.Second)
	for hg.Check() != nil {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for health gate recovery after DB restart")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The pool also needs to establish fresh connections after the restart.
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pool to reconnect after DB restart")
		}
		if pool.Ping(ctx) == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Collectors should work again.
	result, err = a.GetMetrics()
	assert.NoError(t, err)
	assert.Len(t, result, numCollectors, "all collectors should produce metrics after recovery")
}
