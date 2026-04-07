package runner

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/dbtune/dbtunetest"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockAgentLooper implements utils.AgentLooper for testing
type MockAgentLooper struct {
	mock.Mock
}

func (m *MockAgentLooper) Logger() *logrus.Logger {
	args := m.Called()
	return args.Get(0).(*logrus.Logger)
}

func (m *MockAgentLooper) SendHeartbeat(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockAgentLooper) GetMetrics(ctx context.Context) ([]metrics.FlatValue, error) {
	args := m.Called(ctx)
	return args.Get(0).([]metrics.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendMetrics(ctx context.Context, metrics []metrics.FlatValue) error {
	args := m.Called(ctx, metrics)
	return args.Error(0)
}

func (m *MockAgentLooper) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	args := m.Called(ctx)
	return args.Get(0).([]metrics.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendSystemInfo(ctx context.Context, info []metrics.FlatValue) error {
	args := m.Called(ctx, info)
	return args.Error(0)
}

func (m *MockAgentLooper) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	args := m.Called(ctx)
	return args.Get(0).(agent.ConfigArraySchema), args.Error(1)
}

func (m *MockAgentLooper) SendActiveConfig(ctx context.Context, config agent.ConfigArraySchema) error {
	args := m.Called(ctx, config)
	return args.Error(0)
}

func (m *MockAgentLooper) GetProposedConfig(ctx context.Context) (*agent.ProposedConfigResponse, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.ProposedConfigResponse), args.Error(1)
}

func (m *MockAgentLooper) ApplyConfig(ctx context.Context, config *agent.ProposedConfigResponse) error {
	args := m.Called(ctx, config)
	return args.Error(0)
}

func (m *MockAgentLooper) Guardrails(ctx context.Context) *guardrails.Signal {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*guardrails.Signal)
}

func (m *MockAgentLooper) SendGuardrailSignal(ctx context.Context, signal guardrails.Signal) error {
	args := m.Called(ctx, signal)
	return args.Error(0)
}

func (m *MockAgentLooper) SendError(ctx context.Context, payload agent.ErrorPayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

// Test runWithTicker function
func TestRunWithTicker(t *testing.T) {
	logger := logrus.New()

	t.Run("successful execution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		counter := 0
		var mu sync.Mutex

		fn := func(_ context.Context) error {
			mu.Lock()
			counter++
			mu.Unlock()
			return nil
		}

		go runWithTicker(ctx, ticker, "test", logger, false, fn)

		// Wait for context to be done
		<-ctx.Done()

		mu.Lock()
		defer mu.Unlock()
		assert.Greater(t, counter, 1, "function should have been called multiple times")
		assert.Less(t, counter, 100, "function should not have been called more than 100 times")
	})

	t.Run("function returns error but still runs", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		counter := 0
		var mu sync.Mutex

		expectedErr := errors.New("test error")
		fn := func(_ context.Context) error {
			mu.Lock()
			counter++
			mu.Unlock()
			return expectedErr
		}

		go runWithTicker(ctx, ticker, "test", logger, false, fn)

		// Wait for context to be done
		<-ctx.Done()

		mu.Lock()
		defer mu.Unlock()
		assert.Greater(t, counter, 1, "function should have been called multiple times")
		assert.Less(t, counter, 100, "function should not have been called more than 100 times")
	})
}

// Test Runner function
func TestRunner(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	// Setup mock expectations
	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)

	// Wait for context to be done
	<-ctx.Done()

	// Small grace period for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner with error conditions
func TestRunnerWithErrors(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	// Setup mock expectations with errors
	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true.
	// withHealthGate no longer calls SendError for collection failures — it reports
	// to the health gate and returns the error to runWithTicker for logging.
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, errors.New("metrics error"))
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, errors.New("system info error"))
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, errors.New("config error"))
	mockAgent.On("Guardrails", mock.Anything).Return(nil)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)

	// Wait for context to be done
	<-ctx.Done()

	// Small grace period for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner when GetProposedConfig returns a config then ApplyConfig should be called
func TestRunnerWhenGetProposedConfigReturnsAConfigThenApplyConfigShouldBeCalled(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	mockRecommendation := &agent.ProposedConfigResponse{}

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(mockRecommendation, nil)
	mockAgent.On("ApplyConfig", mock.Anything, mockRecommendation).Return(nil)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)

	// Wait for context to be done
	<-ctx.Done()

	// Small grace period for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner when GetProposedConfig does not return a config then ApplyConfig should not be called
func TestRunnerWhenGetProposedConfigDoesNotReturnAConfigThenApplyConfigShouldNotBeCalled(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)

	// Wait for context to be done
	<-ctx.Done()

	// Small grace period for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	// Verify that ApplyConfig was never called
	mockAgent.AssertNotCalled(t, "ApplyConfig")

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// stubAgentLooper  is required because the existing MockAgent stubs way too much for
// what we are testing here. It embeds CommonAgent (for real HTTP implementations) and provides
// minimal stubs for the adapter-specific methods not implemented by CommonAgent.
type stubAgentLooper struct {
	agent.CommonAgent
}

// Stub out the database interactions here (but note: not the DBtune API)
func (s *stubAgentLooper) GetSystemInfo(_ context.Context) ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (s *stubAgentLooper) GetActiveConfig(_ context.Context) (agent.ConfigArraySchema, error) {
	return agent.ConfigArraySchema{}, nil
}

func (s *stubAgentLooper) ApplyConfig(_ context.Context, _ *agent.ProposedConfigResponse) error {
	return nil
}

func (s *stubAgentLooper) Guardrails(_ context.Context) *guardrails.Signal {
	return &guardrails.Signal{Level: guardrails.Critical, Type: guardrails.Memory}
}

func createStubAgentLooper(rt http.RoundTripper) *stubAgentLooper {
	underlying := &http.Client{Transport: rt}
	client := retryablehttp.NewClient()
	client.HTTPClient = underlying

	ca := agent.CommonAgent{
		AgentID:   "test-agent",
		APIClient: client,
		ServerURLs: dbtune.ServerURLs{
			ServerUrl: "http://localhost:8080",
			ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			DbID:      "550e8400-e29b-41d4-a716-446655440000",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Version:   "test-version",
	}
	ca.WithLogger(logrus.New())

	return &stubAgentLooper{CommonAgent: ca}
}

// TestRunnerCallsAllAPIActions verifies that Runner makes all expected HTTP API
// calls using a real CommonAgent wired to a SuccessfulTrip transport.
// Heartbeat (skipFirst: true, 15 s ticker) and log-entries (error-path only)
// are excluded because they do not fire in a short test window.
func TestRunnerCallsAllAPIActions(t *testing.T) {
	transport := dbtunetest.CreateSuccessfulTripWithRoutes([]dbtunetest.Route{
		{"/api/v1/agent/configurations", http.MethodGet},
		{"/api/v1/agent/configurations", http.MethodPost},
		{"/api/v1/agent/metrics", http.MethodPost},
		{"/api/v1/agent/system-info", http.MethodPut},
		{"/api/v1/agent/guardrails", http.MethodPost},
	})

	stub := createStubAgentLooper(transport)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, stub)

	// Wait for context to be done
	<-ctx.Done()

	// Small grace period for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	transport.AllActionsCalled(t)
}

func TestIsRecoveryError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// nil / unrelated
		{"nil error", nil, false},
		{"unrelated error", errors.New("something else"), false},

		// failover detected — exact, case-insensitive, embedded in larger message
		{"failover detected lowercase", errors.New("failover detected"), true},
		{"failover detected uppercase", errors.New("FAILOVER DETECTED"), true},
		{"failover detected mixed case", errors.New("Failover Detected"), true},
		{"failover detected in context", errors.New("pg error: failover detected during query execution"), true},

		// recovery in progress — exact, case-insensitive, embedded in larger message
		{"recovery in progress lowercase", errors.New("recovery in progress"), true},
		{"recovery in progress uppercase", errors.New("RECOVERY IN PROGRESS"), true},
		{"recovery in progress mixed case", errors.New("Recovery In Progress"), true},
		{"recovery in progress in context", errors.New("the server is in recovery in progress, try again later"), true},

		// partial matches that should NOT match
		{"failover without detected", errors.New("failover occurred"), false},
		{"recovery without in progress", errors.New("recovery completed"), false},
		{"detected without failover", errors.New("change detected"), false},
		{"progress without recovery", errors.New("in progress"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isRecoveryError(tt.err))
		})
	}
}
