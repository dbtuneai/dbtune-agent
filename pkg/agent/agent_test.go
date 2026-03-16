package agent

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
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

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

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
			logger: quietLogger(),
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
			logger: quietLogger(),
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
		assert.NoError(t, err) // The function should not return error even if collectors fail
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("context timeout - slow collectors are cancelled", func(t *testing.T) {
		agent := &CommonAgent{
			logger: quietLogger(),
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
		assert.NoError(t, err)
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
		logger:    quietLogger(),
		ServerURLs: dbtune.ServerURLs{
			ServerUrl: "http://localhost:8080",
			ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			DbID:      "550e8400-e29b-41d4-a716-446655440000",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test-version",
	}
}

// testPayload is a minimal struct used to test isNilPayload without importing catalog (which would cause a cycle).
type testPayload struct {
	Rows []string
}

func TestIsNilPayload(t *testing.T) {
	t.Run("interface nil", func(t *testing.T) {
		assert.True(t, isNilPayload(nil))
	})

	t.Run("typed nil pointer", func(t *testing.T) {
		var p *testPayload
		assert.True(t, isNilPayload(p))
	})

	t.Run("typed nil pointer - different type", func(t *testing.T) {
		var p *struct{ Data int }
		assert.True(t, isNilPayload(p))
	})

	t.Run("non-nil pointer with nil rows", func(t *testing.T) {
		p := &testPayload{Rows: nil}
		assert.False(t, isNilPayload(p))
	})

	t.Run("non-nil pointer with rows", func(t *testing.T) {
		p := &testPayload{Rows: []string{}}
		assert.False(t, isNilPayload(p))
	})

	t.Run("non-pointer value", func(t *testing.T) {
		assert.False(t, isNilPayload("hello"))
	})

	t.Run("non-pointer struct", func(t *testing.T) {
		assert.False(t, isNilPayload(testPayload{}))
	})
}

func TestCommonAgent_SendHeartbeat_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)

	ctx := context.Background()
	err := agent.SendHeartbeat(ctx)
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/heartbeat", http.MethodPost)
}

func TestCommonAgent_SendActiveConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	config := ConfigArraySchema{}
	ctx := context.Background()

	err := agent.SendActiveConfig(ctx, config)
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/configurations", http.MethodPost)
}

func TestCommonAgent_SendError_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	ctx := context.Background()

	err := agent.SendError(ctx, ErrorPayload{
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
	ctx := context.Background()

	err := agent.SendGuardrailSignal(ctx, guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/guardrails", http.MethodPost)
}

func TestCommonAgent_SendMetrics_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	ctx := context.Background()

	err := agent.SendMetrics(ctx, []metrics.FlatValue{
		{Key: "metric1", Value: 1, Type: "int"},
		{Key: "metric2", Value: 3.14, Type: "float"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/metrics", http.MethodPost)
}

func TestCommonAgent_SendSystemInfo_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	ctx := context.Background()

	err := agent.SendSystemInfo(ctx, []metrics.FlatValue{
		{Key: "os", Value: "linux", Type: "string"},
		{Key: "cpu_count", Value: 4, Type: "int"},
	})
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/system-info", http.MethodPut)
}

func TestCommonAgent_GetProposedConfig_Succeeds(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTrip()
	agent := CreateCommonAgentForTests(transport)
	ctx := context.Background()

	config, err := agent.GetProposedConfig(ctx)
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

func TestGzipJSON(t *testing.T) {
	t.Run("roundtrip produces valid gzip containing original JSON", func(t *testing.T) {
		input := map[string]any{"key": "value", "number": 42}

		jsonSize, buf, err := gzipJSON(input)
		require.NoError(t, err)

		// jsonSize should match a direct json.Marshal
		expected, _ := json.Marshal(input)
		assert.Equal(t, len(expected), jsonSize)

		// compressed should be smaller (or at least different) than raw JSON
		assert.Greater(t, buf.Len(), 0)

		// decompress and verify roundtrip
		gz, err := gzip.NewReader(buf)
		require.NoError(t, err)
		defer gz.Close()

		decompressed, err := io.ReadAll(gz)
		require.NoError(t, err)
		assert.JSONEq(t, string(expected), string(decompressed))
	})

	t.Run("large payload compresses significantly", func(t *testing.T) {
		// Simulate a large repetitive payload like a schema with many tables
		rows := make([]string, 1000)
		for i := range rows {
			rows[i] = "CREATE TABLE test_table_with_a_long_name (id serial PRIMARY KEY, name text NOT NULL, created_at timestamptz DEFAULT now())"
		}
		payload := map[string]any{"ddl": rows}

		jsonSize, buf, err := gzipJSON(payload)
		require.NoError(t, err)

		// Repetitive data should compress well — at least 5x
		assert.Less(t, buf.Len(), jsonSize/5, "expected at least 5x compression for repetitive data")
	})

	t.Run("empty struct", func(t *testing.T) {
		jsonSize, buf, err := gzipJSON(struct{}{})
		require.NoError(t, err)
		assert.Equal(t, 2, jsonSize) // "{}"

		gz, err := gzip.NewReader(buf)
		require.NoError(t, err)
		defer gz.Close()
		decompressed, err := io.ReadAll(gz)
		require.NoError(t, err)
		assert.Equal(t, "{}", string(decompressed))
	})

	t.Run("unmarshalable payload returns error", func(t *testing.T) {
		// channels cannot be marshaled to JSON
		_, _, err := gzipJSON(make(chan int))
		assert.Error(t, err)
	})
}

func TestSendCatalogPayload_SendsGzippedBody(t *testing.T) {
	transport := dbtunetest.CreateCatalogTrip("ddl")
	agent := CreateCommonAgentForTests(transport)

	payload := &testPayload{Rows: []string{"CREATE TABLE foo (id int)"}}
	err := agent.SendCatalogPayload(context.Background(), "ddl", payload)
	assert.NoError(t, err)

	transport.ActionWasCalled(t, "/api/v1/agent/ddl", http.MethodPost)
}

func TestSendCatalogPayload_NilPayloadReturnsError(t *testing.T) {
	transport := dbtunetest.CreateCatalogTrip("ddl")
	agent := CreateCommonAgentForTests(transport)

	var payload *testPayload
	err := agent.SendCatalogPayload(context.Background(), "ddl", payload)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "payload is nil")
}

func TestSendCatalogPayload_DecompressedBodyMatchesPayload(t *testing.T) {
	// Use a custom transport that captures the request body for inspection
	payload := &testPayload{Rows: []string{"CREATE TABLE bar (id int)", "CREATE TABLE baz (name text)"}}
	var capturedBody []byte
	var capturedHeaders http.Header

	customTransport := &captureTransport{
		handler: func(req *http.Request) (*http.Response, error) {
			capturedHeaders = req.Header.Clone()
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			capturedBody = body
			return &http.Response{
				Status:     "204 No Content",
				StatusCode: http.StatusNoContent,
			}, nil
		},
	}

	agent := CreateCommonAgentForTests(customTransport)
	err := agent.SendCatalogPayload(context.Background(), "ddl", payload)
	require.NoError(t, err)

	// Verify headers
	assert.Equal(t, "application/json", capturedHeaders.Get("Content-Type"))
	assert.Equal(t, "gzip", capturedHeaders.Get("Content-Encoding"))

	// Decompress and verify body matches original payload
	gz, err := gzip.NewReader(bytes.NewReader(capturedBody))
	require.NoError(t, err)
	defer gz.Close()

	decompressed, err := io.ReadAll(gz)
	require.NoError(t, err)

	expectedJSON, _ := json.Marshal(payload)
	assert.JSONEq(t, string(expectedJSON), string(decompressed))
}

// captureTransport is a simple RoundTripper that delegates to a handler function.
type captureTransport struct {
	handler func(*http.Request) (*http.Response, error)
}

func (ct *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return ct.handler(req)
}
