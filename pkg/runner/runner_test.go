package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/dbtune/dbtunetest"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
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

func (m *MockAgentLooper) GetDDL(ctx context.Context) (*agent.DDLPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.DDLPayload), args.Error(1)
}

func (m *MockAgentLooper) SendDDL(ctx context.Context, payload *agent.DDLPayload) error {
	args := m.Called(ctx, payload)
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

func (m *MockAgentLooper) CatalogCollectors() []agent.CatalogCollector {
	args := m.Called()
	return args.Get(0).([]agent.CatalogCollector)
}

func (m *MockAgentLooper) SendCatalogPayload(ctx context.Context, name string, payload any) error {
	args := m.Called(ctx, name, payload)
	return args.Error(0)
}

func (m *MockAgentLooper) SendError(ctx context.Context, payload agent.ErrorPayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

// setupCatalogMocksSuccess sets up catalog collectors that return test data successfully.
func setupCatalogMocksSuccess(m *MockAgentLooper) {
	testData := map[string]bool{}
	m.On("CatalogCollectors").Return([]agent.CatalogCollector{
		{Name: "test_catalog", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			testData["collected"] = true
			return map[string]string{"test": "data"}, nil
		}},
	})
	m.On("SendCatalogPayload", mock.Anything, mock.Anything, mock.Anything).Return(nil)
}

// setupCatalogMocksError sets up catalog collectors that return errors.
func setupCatalogMocksError(m *MockAgentLooper) {
	m.On("CatalogCollectors").Return([]agent.CatalogCollector{
		{Name: "test_catalog", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			return nil, errors.New("error")
		}},
	})
}

// Test SkipUnchanged dedup behavior for catalog collectors
func TestCatalogSkipUnchanged(t *testing.T) {
	t.Run("identical data only sends once", func(t *testing.T) {
		logger := logrus.New()
		logger.SetOutput(io.Discard)

		var sendCount int
		var mu sync.Mutex

		mockAgent := new(MockAgentLooper)
		mockAgent.On("Logger").Return(logger)
		mockAgent.On("SendHeartbeat", mock.Anything).Return(nil)
		mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
		mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
		mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetDDL", mock.Anything).Return(&agent.DDLPayload{Hash: "abc123"}, nil)
		mockAgent.On("SendDDL", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
		mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
		mockAgent.On("Guardrails", mock.Anything).Return(nil)

		// Return identical data every tick, with SkipUnchanged enabled
		mockAgent.On("CatalogCollectors").Return([]agent.CatalogCollector{
			{
				Name:          "test_dedup",
				Interval:      10 * time.Millisecond,
				SkipUnchanged: true,
				Collect: func(ctx context.Context) (any, error) {
					return map[string]string{"key": "same_value"}, nil
				},
			},
		})
		mockAgent.On("SendCatalogPayload", mock.Anything, "test_dedup", mock.Anything).
			Run(func(args mock.Arguments) {
				mu.Lock()
				sendCount++
				mu.Unlock()
			}).Return(nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go Runner(ctx, mockAgent)
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 1, sendCount, "SendCatalogPayload should be called exactly once for identical data")
	})

	t.Run("changed data sends again", func(t *testing.T) {
		logger := logrus.New()
		logger.SetOutput(io.Discard)

		var sendCount int
		var callCount int
		var mu sync.Mutex

		mockAgent := new(MockAgentLooper)
		mockAgent.On("Logger").Return(logger)
		mockAgent.On("SendHeartbeat", mock.Anything).Return(nil)
		mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
		mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
		mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetDDL", mock.Anything).Return(&agent.DDLPayload{Hash: "abc123"}, nil)
		mockAgent.On("SendDDL", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
		mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
		mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
		mockAgent.On("Guardrails", mock.Anything).Return(nil)

		// Return different data on each call
		mockAgent.On("CatalogCollectors").Return([]agent.CatalogCollector{
			{
				Name:          "test_dedup_change",
				Interval:      10 * time.Millisecond,
				SkipUnchanged: true,
				Collect: func(ctx context.Context) (any, error) {
					mu.Lock()
					callCount++
					n := callCount
					mu.Unlock()
					return map[string]int{"counter": n}, nil
				},
			},
		})
		mockAgent.On("SendCatalogPayload", mock.Anything, "test_dedup_change", mock.Anything).
			Run(func(args mock.Arguments) {
				mu.Lock()
				sendCount++
				mu.Unlock()
			}).Return(nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go Runner(ctx, mockAgent)
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		assert.Greater(t, sendCount, 1, "SendCatalogPayload should be called multiple times when data changes")
	})
}

// Test runWithTicker function
func TestRunWithTicker(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	t.Run("successful execution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		counter := 0
		var mu sync.Mutex

		fn := func(ctx context.Context) error {
			mu.Lock()
			counter++
			mu.Unlock()
			return nil
		}

		go runWithTicker(ctx, 10*time.Millisecond, "test", logger, false, fn)

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

		counter := 0
		var mu sync.Mutex

		expectedErr := errors.New("test error")
		fn := func(ctx context.Context) error {
			mu.Lock()
			counter++
			mu.Unlock()
			return expectedErr
		}

		go runWithTicker(ctx, 10*time.Millisecond, "test", logger, false, fn)

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
	logger.SetOutput(io.Discard)

	// Setup mock expectations
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)

	mockAgent.On("GetDDL", mock.Anything).Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool)
	go func() {
		Runner(ctx, mockAgent)
		done <- true
	}()

	// Wait a short time to allow the Runner to execute a few cycles
	time.Sleep(200 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner with error conditions
func TestRunnerWithErrors(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	// Setup mock expectations with errors
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, errors.New("metrics error"))
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, errors.New("system info error"))

	mockAgent.On("GetDDL", mock.Anything).Return(nil, errors.New("ddl error"))
	mockAgent.On("SendError", mock.Anything, mock.AnythingOfType("agent.ErrorPayload")).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, errors.New("config error"))
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	setupCatalogMocksError(mockAgent)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool)
	go func() {
		Runner(ctx, mockAgent)
		done <- true
	}()

	// Wait a short time to allow the Runner to execute a few cycles
	time.Sleep(200 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner when GetProposedConfig returns a config then ApplyConfig should be called
func TestRunnerWhenGetProposedConfigReturnsAConfigThenApplyConfigShouldBeCalled(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	mockRecommendation := &agent.ProposedConfigResponse{}

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)

	mockAgent.On("GetDDL", mock.Anything).Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(mockRecommendation, nil)
	mockAgent.On("ApplyConfig", mock.Anything, mockRecommendation).Return(nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool)
	go func() {
		Runner(ctx, mockAgent)
		done <- true
	}()

	// Wait a short time to allow the Runner to execute a few cycles
	time.Sleep(200 * time.Millisecond)

	// Verify that the mock expectations were met
	mockAgent.AssertExpectations(t)
}

// Test Runner when GetProposedConfig does not return a config then ApplyConfig should not be called
func TestRunnerWhenGetProposedConfigDoesNotReturnAConfigThenApplyConfigShouldNotBeCalled(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)

	mockAgent.On("GetDDL", mock.Anything).Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool)
	go func() {
		Runner(ctx, mockAgent)
		done <- true
	}()

	// Wait a short time to allow the Runner to execute a few cycles
	time.Sleep(200 * time.Millisecond)

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
func (s *stubAgentLooper) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (s *stubAgentLooper) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	return agent.ConfigArraySchema{}, nil
}

func (s *stubAgentLooper) ApplyConfig(ctx context.Context, _ *agent.ProposedConfigResponse) error {
	return nil
}

func (s *stubAgentLooper) Guardrails(ctx context.Context) *guardrails.Signal {
	return &guardrails.Signal{Level: guardrails.Critical, Type: guardrails.Memory}
}

func (s *stubAgentLooper) GetDDL(ctx context.Context) (*agent.DDLPayload, error) {
	return &agent.DDLPayload{}, nil
}
func (s *stubAgentLooper) CatalogCollectors() []agent.CatalogCollector {
	return nil
}
func (s *stubAgentLooper) SendCatalogPayload(ctx context.Context, name string, payload any) error {
	return nil
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go Runner(ctx, stub)

	time.Sleep(200 * time.Millisecond)

	transport.AllActionsCalled(t)
}

// ---------------------------------------------------------------------------
// isRecoveryError — ErrDatabaseDown integration
// ---------------------------------------------------------------------------

func TestIsRecoveryError_ErrDatabaseDown(t *testing.T) {
	if !isRecoveryError(pg.ErrDatabaseDown) {
		t.Fatal("isRecoveryError should return true for ErrDatabaseDown")
	}
}

func TestIsRecoveryError_WrappedErrDatabaseDown(t *testing.T) {
	wrapped := fmt.Errorf("collector pg_stat_activity: %w", pg.ErrDatabaseDown)
	if !isRecoveryError(wrapped) {
		t.Fatal("isRecoveryError should return true for wrapped ErrDatabaseDown")
	}
}

func TestIsRecoveryError_OtherErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic", errors.New("something broke"), false},
		{"failover detected", errors.New("failover detected: new primary"), true},
		{"recovery in progress", errors.New("cannot execute: recovery in progress"), true},
		{"ErrDatabaseDown direct", pg.ErrDatabaseDown, true},
		{"ErrDatabaseDown wrapped", fmt.Errorf("outer: %w", pg.ErrDatabaseDown), true},
		{"ErrDatabaseDown double wrapped", fmt.Errorf("a: %w", fmt.Errorf("b: %w", pg.ErrDatabaseDown)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isRecoveryError(tc.err)
			if got != tc.want {
				t.Errorf("isRecoveryError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestHandleGetError_SuppressesErrDatabaseDown verifies that handleGetError
// does NOT call SendError when the error is ErrDatabaseDown.
func TestHandleGetError_SuppressesErrDatabaseDown(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	mockAgent := new(MockAgentLooper)
	mockAgent.On("Logger").Return(logger)
	// SendError should NOT be called.

	ctx := context.Background()
	err := handleGetError(ctx, mockAgent, logger, "test_collector", pg.ErrDatabaseDown)

	if err != nil {
		t.Fatalf("handleGetError should return nil for recovery errors, got %v", err)
	}
	mockAgent.AssertNotCalled(t, "SendError", mock.Anything, mock.Anything)
}

// TestHandleGetError_WrappedErrDatabaseDown_Suppressed verifies wrapped ErrDatabaseDown
// is also suppressed.
func TestHandleGetError_WrappedErrDatabaseDown_Suppressed(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	mockAgent := new(MockAgentLooper)
	mockAgent.On("Logger").Return(logger)

	ctx := context.Background()
	wrapped := fmt.Errorf("pg_stat_user_tables: %w", pg.ErrDatabaseDown)
	err := handleGetError(ctx, mockAgent, logger, "test_collector", wrapped)

	if err != nil {
		t.Fatalf("handleGetError should return nil for wrapped ErrDatabaseDown, got %v", err)
	}
	mockAgent.AssertNotCalled(t, "SendError", mock.Anything, mock.Anything)
}

// TestHandleGetError_NonRecoveryError_SendsError verifies non-recovery errors
// still trigger SendError.
func TestHandleGetError_NonRecoveryError_SendsError(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	mockAgent := new(MockAgentLooper)
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("SendError", mock.Anything, mock.AnythingOfType("agent.ErrorPayload")).Return(nil)

	ctx := context.Background()
	err := handleGetError(ctx, mockAgent, logger, "test_collector", errors.New("syntax error"))

	if err == nil {
		t.Fatal("handleGetError should return the error for non-recovery errors")
	}
	mockAgent.AssertCalled(t, "SendError", mock.Anything, mock.AnythingOfType("agent.ErrorPayload"))
}
