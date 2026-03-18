//go:build integration

package agent_test

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

type bufLogger struct{ buf *bytes.Buffer }

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

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

	a := &agent.CommonAgent{
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

	// --- Phase 1: DB is up, collectors work ---
	require.NoError(t, pool.Ping(ctx))
	result, err := a.GetMetrics()
	require.NoError(t, err)
	require.Len(t, result, numCollectors)

	// --- Phase 2: Stop the database ---
	stopTimeout := 5 * time.Second
	require.NoError(t, pgContainer.Stop(ctx, &stopTimeout))

	// Collectors hit real connection errors and trip the gate.
	mu.Lock()
	collectorCalls = 0
	mu.Unlock()

	result, err = a.GetMetrics()
	t.Logf("phase 2: got %d metrics, err=%v", len(result), err)

	if err == nil {
		assert.True(t, errors.Is(hg.Check(), agent.ErrDatabaseDown),
			"gate should be tripped after collectors hit connection errors")
	}

	// --- Phase 3: Gate is now down — single error, zero collectors ---
	mu.Lock()
	collectorCalls = 0
	mu.Unlock()

	result, err = a.GetMetrics()
	assert.ErrorIs(t, err, agent.ErrDatabaseDown)
	assert.Nil(t, result)

	mu.Lock()
	calls := collectorCalls
	mu.Unlock()
	assert.Equal(t, 0, calls, "zero collectors should have executed")

	// --- Phase 4: Restart, verify recovery ---
	require.NoError(t, pgContainer.Start(ctx))

	deadline := time.Now().Add(60 * time.Second)
	for hg.Check() != nil {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for health gate recovery")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for pool.Ping(ctx) != nil {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pool to reconnect")
		}
		time.Sleep(200 * time.Millisecond)
	}

	result, err = a.GetMetrics()
	assert.NoError(t, err)
	assert.Len(t, result, numCollectors)
}
