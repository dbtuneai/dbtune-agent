package agent

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/backend"
	"github.com/dbtuneai/agent/pkg/backend/backendtest"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCollector creates a collector that simulates different behaviors
func mockCollector(delay time.Duration, shouldError bool, values []metrics.FlatValue) metrics.Collector {
	return metrics.Collector{
		Key: "test_collector",
		Collector: func(ctx context.Context, state *metrics.State) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				if shouldError {
					return errors.New("collector error")
				}
				for _, metric := range values {
					state.AddMetric(metric)
				}
				return nil
			}
		},
	}
}

func TestGetMetrics(t *testing.T) {
	t.Run("happy path - all collectors succeed", func(t *testing.T) {
		agent := &CommonAgent{
			Client: clientWithLogger(logrus.New()),
			MetricsState: metrics.State{
				Collectors: []metrics.Collector{
					mockCollector(100*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric1",
						Value: 1,
						Type:  "int",
					}}),
					mockCollector(200*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics(context.Background())
		assert.NoError(t, err)
		assert.Len(t, flat_metrics, 2)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("partial failure - one collector errors", func(t *testing.T) {
		agent := &CommonAgent{
			Client: clientWithLogger(logrus.New()),
			MetricsState: metrics.State{
				Collectors: []metrics.Collector{
					mockCollector(100*time.Millisecond, true, nil), // This one will error
					mockCollector(200*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics(context.Background())
		assert.Error(t, err) // GetMetrics now returns collector errors
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("context timeout - slow collectors are cancelled", func(t *testing.T) {
		agent := &CommonAgent{
			Client: clientWithLogger(logrus.New()),
			MetricsState: metrics.State{
				Collectors: []metrics.Collector{
					mockCollector(100*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric1",
						Value: 1,
						Type:  "int",
					}}),
					mockCollector(600*time.Millisecond, false, []metrics.FlatValue{{ // This one will timeout as it exceeds IndividualTimeout
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics(context.Background())
		assert.Error(t, err) // timed-out collector produces an error
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
	})

	// A panicking collector used to take the whole process down, since
	// collectors run in their own goroutine and nothing recovered.
	t.Run("panicking collector does not kill the process", func(t *testing.T) {
		transport := backendtest.CreateSuccessfulTripWithRoutes([]backendtest.Route{
			{"/api/v1/agent/log-entries", http.MethodPost},
		})
		agent := CreateCommonAgentForTests(transport)
		agent.MetricsState = metrics.State{
			Collectors: []metrics.Collector{
				{
					Key: "panicking_collector",
					Collector: func(_ context.Context, _ *metrics.State) error {
						var nilMap map[string]int
						nilMap["boom"] = 1 // assignment to entry in nil map
						return nil
					},
				},
				mockCollector(10*time.Millisecond, false, []metrics.FlatValue{{
					Key:   "metric2",
					Value: 2,
					Type:  "int",
				}}),
			},
			Mutex: &sync.Mutex{},
		}
		agent.CollectionTimeout = 1 * time.Second
		agent.IndividualTimeout = 500 * time.Millisecond

		flat_metrics, err := agent.GetMetrics(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic in panicking_collector (consecutive panics: 1)")
		assert.Contains(t, err.Error(), "nil map")
		assert.Len(t, flat_metrics, 1, "healthy collectors must still report")
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})

		// The panic reaches the platform under its own type, like every other
		// guarded collector, rather than as a plain collector error.
		transport.ActionWasCalled(t, "/api/v1/agent/log-entries", http.MethodPost)
	})

	// A collector that panics once panics on every tick, so its reports back
	// off instead of firing every 5 seconds forever.
	t.Run("a collector stuck panicking is throttled", func(t *testing.T) {
		transport := backendtest.CreateSuccessfulTripWithRoutes([]backendtest.Route{
			{"/api/v1/agent/log-entries", http.MethodPost},
		})
		agent := CreateCommonAgentForTests(transport)
		agent.MetricsState = metrics.State{
			Collectors: []metrics.Collector{
				{
					Key: "panicking_collector",
					Collector: func(_ context.Context, _ *metrics.State) error {
						panic("boom")
					},
				},
			},
			Mutex: &sync.Mutex{},
		}
		agent.CollectionTimeout = 1 * time.Second
		agent.IndividualTimeout = 500 * time.Millisecond

		var errored int
		for range 10 {
			if _, err := agent.GetMetrics(context.Background()); err != nil {
				errored++
			}
		}

		assert.Equal(t, 2, errored, "the 1st and 10th panic surface, the rest are suppressed")
	})
}

// Creates a CommonAgent for testing purposes
// The fact that this exists probably implies that our CreateCommonAgent
// struct doesn't have a good enough way to create it with either:
//   - defaults that work
//   - without relying on the config existing
func CreateCommonAgentForTests(rt http.RoundTripper) CommonAgent {
	// add in our middleware that itercepts the http request
	// to the underlying http client
	underlyingHTTPClient := http.Client{}
	underlyingHTTPClient.Transport = rt

	httpClient := retryablehttp.NewClient()
	httpClient.HTTPClient = &underlyingHTTPClient

	ca := CommonAgent{
		Client: backend.Client{
			AgentID:   "test-agent",
			APIClient: httpClient,
			ServerURLs: backend.ServerURLs{
				ServerUrl: "http://localhost:8080",
				ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
				DbID:      "550e8400-e29b-41d4-a716-446655440000",
			},
			StartTime: time.Now().UTC().Format(time.RFC3339),
			Version:   "test-version",
		},
	}
	ca.WithLogger(logrus.New())
	return ca
}

// clientWithLogger returns a backend client that only has a logger, for tests
// that never reach the backend.
func clientWithLogger(logger *logrus.Logger) backend.Client {
	var c backend.Client
	c.WithLogger(logger)
	return c
}
