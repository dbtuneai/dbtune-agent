package backend

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/backend/backendtest"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/tuning"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestClient_SendHeartbeat_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	err := client.SendHeartbeat(context.Background())
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/heartbeat", http.MethodPost)
}

func TestClient_SendActiveConfig_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)
	config := tuning.ConfigArraySchema{}

	err := client.SendActiveConfig(context.Background(), config, time.Now())
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodPost)
}

func TestClient_SendError_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	err := client.SendError(context.Background(), ErrorPayload{
		ErrorMessage: "something went wrong",
		ErrorType:    "test_error",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/log-entries", http.MethodPost)
}

func TestClient_SendGuardrailSignal_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	err := client.SendGuardrailSignal(context.Background(), guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/guardrails", http.MethodPost)
}

func TestClient_SendMetrics_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	err := client.SendMetrics(context.Background(), []metrics.FlatValue{
		{Key: "metric1", Value: 1, Type: "int"},
		{Key: "metric2", Value: 3.14, Type: "float"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/metrics", http.MethodPost)
}

func TestClient_SendSystemInfo_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	err := client.SendSystemInfo(context.Background(), []metrics.FlatValue{
		{Key: "os", Value: "linux", Type: "string"},
		{Key: "cpu_count", Value: 4, Type: "int"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/system-info", http.MethodPut)
}

func TestClient_GetProposedConfig_Succeeds(t *testing.T) {
	transport := backendtest.CreateSuccessfulTrip()
	client := newTestClient(transport)

	config, err := client.GetProposedConfig(context.Background())
	if assert.NoError(t, err) && assert.NotNil(t, config) {
		assert.Len(t, config.Config, 3)
		assert.Equal(t, "shared_buffers", config.Config[0].Name)
		assert.Equal(t, "work_mem", config.Config[1].Name)
		assert.Equal(t, "max_connections", config.Config[2].Name)
		assert.Equal(t, []string{"shared_buffers"}, config.KnobsOverrides)
		assert.Equal(t, tuning.KnobApplicationRestart, config.KnobApplication)

		transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodGet)
	}
}

// newTestClient returns a Client that sends its requests through rt.
func newTestClient(rt http.RoundTripper) Client {
	httpClient := retryablehttp.NewClient()
	httpClient.HTTPClient = &http.Client{Transport: rt}

	return Client{
		AgentID:   "test-agent",
		APIClient: httpClient,
		logger:    logrus.New(),
		ServerURLs: ServerURLs{
			ServerUrl: "http://localhost:8080",
			ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			DbID:      "550e8400-e29b-41d4-a716-446655440000",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test-version",
	}
}
