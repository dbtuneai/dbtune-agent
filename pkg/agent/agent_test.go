package agent

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/dbtune/dbtunetest"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockCollector creates a collector that simulates different behaviors
func mockCollector(delay time.Duration, shouldError bool, metrics []metrics.FlatValue) MetricCollector {
	return MetricCollector{
		Key: "test_collector",
		Collector: func(ctx context.Context, state *MetricsState) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				if shouldError {
					return errors.New("collector error")
				}
				for _, metric := range metrics {
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
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
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

		flat_metrics, err := agent.GetMetrics()
		assert.NoError(t, err)
		assert.Len(t, flat_metrics, 2)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("partial failure - one collector errors", func(t *testing.T) {
		agent := &CommonAgent{
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
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

		flat_metrics, err := agent.GetMetrics()
		assert.Error(t, err) // GetMetrics now returns collector errors
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("context timeout - slow collectors are cancelled", func(t *testing.T) {
		agent := &CommonAgent{
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
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

		flat_metrics, err := agent.GetMetrics()
		assert.Error(t, err) // timed-out collector produces an error
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
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

	return CommonAgent{
		AgentID:   "test-agent",
		APIClient: httpClient,
		logger:    logrus.New(),
		ServerURLs: dbtune.ServerURLs{
			ServerUrl: "http://localhost:8080",
			ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			DbID:      "550e8400-e29b-41d4-a716-446655440000",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test-version",
	}
}

func TestCommonAgent_SendHeartbeat_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendHeartbeat()
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/heartbeat", http.MethodPost)
}

func TestCommonAgent_SendActiveConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	config := ConfigArraySchema{}

	err := agent.SendActiveConfig(config)
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodPost)
}

func TestCommonAgent_SendError_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendError(ErrorPayload{
		ErrorMessage: "something went wrong",
		ErrorType:    "test_error",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/log-entries", http.MethodPost)
}

func TestCommonAgent_SendGuardrailSignal_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendGuardrailSignal(guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/guardrails", http.MethodPost)
}

func TestCommonAgent_SendMetrics_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendMetrics([]metrics.FlatValue{
		{Key: "metric1", Value: 1, Type: "int"},
		{Key: "metric2", Value: 3.14, Type: "float"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/metrics", http.MethodPost)
}

func TestCommonAgent_SendSystemInfo_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendSystemInfo([]metrics.FlatValue{
		{Key: "os", Value: "linux", Type: "string"},
		{Key: "cpu_count", Value: 4, Type: "int"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/system-info", http.MethodPut)
}

func TestCommonAgent_GetProposedConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	config, err := agent.GetProposedConfig()
	if assert.NoError(t, err) && assert.NotNil(t, config) {
		assert.Len(t, config.Config, 3)
		assert.Equal(t, "shared_buffers", config.Config[0].Name)
		assert.Equal(t, "work_mem", config.Config[1].Name)
		assert.Equal(t, "max_connections", config.Config[2].Name)
		assert.Equal(t, []string{"shared_buffers"}, config.KnobsOverrides)
		assert.Equal(t, "restart", config.KnobApplication)

		transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodGet)
	}
}
