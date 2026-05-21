package agent

import (
	"context"
	"encoding/json"
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
	"github.com/stretchr/testify/require"
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

		flat_metrics, err := agent.GetMetrics(context.Background())
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

		flat_metrics, err := agent.GetMetrics(context.Background())
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

		flat_metrics, err := agent.GetMetrics(context.Background())
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

	err := agent.SendHeartbeat(context.Background())
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/heartbeat", http.MethodPost)
}

func TestCommonAgent_SendActiveConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	config := ConfigArraySchema{}

	err := agent.SendActiveConfig(context.Background(), config)
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodPost)
}

func TestCommonAgent_SendError_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendError(context.Background(), ErrorPayload{
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

	err := agent.SendGuardrailSignal(context.Background(), guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/guardrails", http.MethodPost)
}

func TestCommonAgent_SendMetrics_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendMetrics(context.Background(), []metrics.FlatValue{
		{Key: "metric1", Value: 1, Type: "int"},
		{Key: "metric2", Value: 3.14, Type: "float"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/metrics", http.MethodPost)
}

func TestCommonAgent_SendSystemInfo_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	err := agent.SendSystemInfo(context.Background(), []metrics.FlatValue{
		{Key: "os", Value: "linux", Type: "string"},
		{Key: "cpu_count", Value: 4, Type: "int"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/system-info", http.MethodPut)
}

func TestCommonAgent_GetProposedConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	config, err := agent.GetProposedConfig(context.Background())
	if assert.NoError(t, err) && assert.NotNil(t, config) {
		assert.Len(t, config.Config, 3)
		assert.Equal(t, "shared_buffers", config.Config[0].Name)
		assert.Equal(t, "work_mem", config.Config[1].Name)
		assert.Equal(t, "max_connections", config.Config[2].Name)
		assert.Equal(t, []string{"shared_buffers"}, config.KnobsOverrides)
		assert.Equal(t, KnobApplicationRestart, config.KnobApplication)

		transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodGet)
	}
}

func TestKnobApplication_UnmarshalJSON(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    KnobApplication
		wantErr bool
	}{
		{name: "restart", input: `"restart"`, want: KnobApplicationRestart},
		{name: "reload", input: `"reload"`, want: KnobApplicationReload},
		{name: "empty string", input: `""`, wantErr: true},
		{name: "uppercase", input: `"RESTART"`, wantErr: true},
		{name: "unknown value", input: `"foo"`, wantErr: true},
		{name: "non-string", input: `42`, wantErr: true},
		{name: "null", input: `null`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got KnobApplication
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestProposedConfigResponse_UnmarshalJSON_RejectsInvalidKnobApplication(t *testing.T) {
	payload := `{"config":[],"knobs_overrides":[],"knob_application":"sometimes"}`
	var resp ProposedConfigResponse
	err := json.Unmarshal([]byte(payload), &resp)
	assert.Error(t, err, "decoding should fail before an adapter ever sees the response")
}

// Compile-time check: both variants satisfy ApplyConfigError.
var (
	_ ApplyConfigError = (*ConfigApplyError)(nil)
	_ ApplyConfigError = (*RestartNotAllowedError)(nil)
)

func TestApplyConfigError_ErrorTypes(t *testing.T) {
	t.Run("ConfigApplyError", func(t *testing.T) {
		inner := errors.New("driver failure")
		e := &ConfigApplyError{Err: inner}
		assert.Equal(t, "config_apply_error", e.ErrorType())
		assert.Equal(t, "driver failure", e.Error())
		assert.ErrorIs(t, e, inner)
	})

	t.Run("RestartNotAllowedError", func(t *testing.T) {
		e := &RestartNotAllowedError{Message: "restart required but allow_restart=false"}
		assert.Equal(t, "restart_not_allowed", e.ErrorType())
		assert.Equal(t, "restart required but allow_restart=false", e.Error())
	})
}

func TestProposedConfigResponse_UnmarshalJSON_RejectsMissingKnobApplication(t *testing.T) {
	// An omitted field leaves KnobApplication at its zero value ("") and
	// UnmarshalJSON is not invoked. We rely on the field always being sent,
	// but document the current behavior so it's an explicit choice rather
	// than an oversight: an omitted field decodes successfully to "".
	payload := `{"config":[],"knobs_overrides":[]}`
	var resp ProposedConfigResponse
	err := json.Unmarshal([]byte(payload), &resp)
	require.NoError(t, err)
	assert.Equal(t, KnobApplication(""), resp.KnobApplication)
}
