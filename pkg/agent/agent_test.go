package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/dbtune"
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
		assert.NoError(t, err) // The function should not return error even if collectors fail
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
		assert.NoError(t, err)
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
	})
}

// A RoundTripper that should return succesful responses when its conditions
// are met.
//
// This serves two purposes:
//   - documents the surface area of the DBtune API that we use and rely on
//   - allows for testing the DBtune API interactions that are part of the agent
//     so we can make changes with confidence that we are not breaking things
type SuccessfulTrip struct{}

func (st SuccessfulTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	switch [2]string{req.URL.Path, req.Method} {
	case [2]string{"/api/v1/agent/heartbeat", http.MethodPost}:
		return st.postHeartbeat(req)
	case [2]string{"/api/v1/agent/configurations", http.MethodGet}:
		return st.getConfigurations(req)
	case [2]string{"/api/v1/agent/configurations", http.MethodPost}:
		return st.postActiveConfig(req)
	case [2]string{"/api/v1/agent/log-entries", http.MethodPost}:
		return st.postLogEntries(req)
	case [2]string{"/api/v1/agent/guardrails", http.MethodPost}:
		return st.postGuardrails(req)
	case [2]string{"/api/v1/agent/metrics", http.MethodPost}:
		return st.postMetrics(req)
	case [2]string{"/api/v1/agent/system-info", http.MethodPut}:
		return st.putSystemInfo(req)
	default:
		return nil, fmt.Errorf("Unrecognised Path: %s Method: %s", req.URL.Path, req.Method)
	}
}

func (st SuccessfulTrip) postHeartbeat(req *http.Request) (*http.Response, error) {
	// Should have an API token, this is currently added in the intialiser
	// of the struct CreateCommonAgentWithVersion via a hook. So actually
	// cannot be tested currently. We leave this here as an example.
	// if req.Header.Get("DBTUNE-API-KEY") == "" {
	// 	return &http.Response{
	// 		Status:     "401 Unauthorized",
	// 		StatusCode: http.StatusUnauthorized,
	// 		Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Missing API key in Header: %s", req.Header))),
	// 	}, nil
	// }
	// Should have the `application/json` content type
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	// Should have a database uuid in the params
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 OK",
		StatusCode: http.StatusNoContent,
	}, nil
}

func (st SuccessfulTrip) postActiveConfig(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func (st SuccessfulTrip) getConfigurations(req *http.Request) (*http.Response, error) {
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}
	if req.URL.Query().Get("status") != "recommended" {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected status=recommended query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	body := `[{
		"config": [
			{"name": "shared_buffers", "setting": "256MB", "unit": "MB", "vartype": "string", "context": "postmaster"},
			{"name": "work_mem", "setting": "16MB", "unit": "MB", "vartype": "string", "context": "user"},
			{"name": "max_connections", "setting": 200, "unit": null, "vartype": "integer", "context": "postmaster"}
		],
		"knobs_overrides": ["shared_buffers"],
		"knob_application": "restart"
	}]`

	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}, nil
}

func (st SuccessfulTrip) postLogEntries(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func (st SuccessfulTrip) postGuardrails(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func (st SuccessfulTrip) postMetrics(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func (st SuccessfulTrip) putSystemInfo(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
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
			ApiKey:    "super-secret-key",
			DbID:      "test-database-id",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test-version",
	}
}

func TestCommonAgent_SendHeartbeat_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	err := agent.SendHeartbeat()
	assert.NoError(t, err)
}

func TestCommonAgent_SendActiveConfig_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})
	config := ConfigArraySchema{}

	err := agent.SendActiveConfig(config)
	assert.NoError(t, err)
}

func TestCommonAgent_SendError_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	err := agent.SendError(ErrorPayload{
		ErrorMessage: "something went wrong",
		ErrorType:    "test_error",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(t, err)
}

func TestCommonAgent_SendGuardrailSignal_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	err := agent.SendGuardrailSignal(guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	})
	assert.NoError(t, err)
}

func TestCommonAgent_SendMetrics_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	err := agent.SendMetrics([]metrics.FlatValue{
		{Key: "metric1", Value: 1, Type: "int"},
		{Key: "metric2", Value: 3.14, Type: "float"},
	})
	assert.NoError(t, err)
}

func TestCommonAgent_SendSystemInfo_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	err := agent.SendSystemInfo([]metrics.FlatValue{
		{Key: "os", Value: "linux", Type: "string"},
		{Key: "cpu_count", Value: 4, Type: "int"},
	})
	assert.NoError(t, err)
}

func TestCommonAgent_GetProposedConfig_Succeeds(t *testing.T) {
	agent := CreateCommonAgentForTests(SuccessfulTrip{})

	config, err := agent.GetProposedConfig()
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Len(t, config.Config, 3)
	assert.Equal(t, "shared_buffers", config.Config[0].Name)
	assert.Equal(t, "work_mem", config.Config[1].Name)
	assert.Equal(t, "max_connections", config.Config[2].Name)
	assert.Equal(t, []string{"shared_buffers"}, config.KnobsOverrides)
	assert.Equal(t, "restart", config.KnobApplication)
}
