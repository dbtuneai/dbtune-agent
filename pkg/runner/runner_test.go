package runner

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/dbtune/dbtunetest"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

func (m *MockAgentLooper) ApplyConfig(ctx context.Context, config *agent.ProposedConfigResponse) agent.ApplyConfigError {
	args := m.Called(ctx, config)
	v := args.Get(0)
	if v == nil {
		return nil
	}
	return v.(agent.ApplyConfigError)
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

func (m *MockAgentLooper) CatalogCollectors() []queries.CatalogCollector {
	args := m.Called()
	if v := args.Get(0); v != nil {
		return v.([]queries.CatalogCollector)
	}
	return nil
}

func (m *MockAgentLooper) SendCatalogPayload(ctx context.Context, name string, payload []byte) error {
	args := m.Called(ctx, name, payload)
	return args.Error(0)
}

// expectNoCatalogCollectors registers the default expectation that a test
// does not exercise the catalog loop. New non-catalog tests should call this
// to avoid repeating the same setup line.
func (m *MockAgentLooper) expectNoCatalogCollectors() {
	m.On("CatalogCollectors").Return(nil)
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
	mockAgent.expectNoCatalogCollectors()

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
	mockAgent.expectNoCatalogCollectors()

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
	mockAgent.expectNoCatalogCollectors()
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
	mockAgent.expectNoCatalogCollectors()
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

// On ApplyConfig failure: SendError carries the variant's error_type, and
// SendActiveConfig still fires so the platform's view of running state stays
// in sync.
func TestRunnerApplyConfigFailure_SendsErrorAndActiveConfig(t *testing.T) {
	cases := []struct {
		name              string
		applyErr          agent.ApplyConfigError
		expectedErrorType string
	}{
		{
			name:              "config_apply_error",
			applyErr:          &agent.ConfigApplyError{Err: errors.New("driver blew up")},
			expectedErrorType: "config_apply_error",
		},
		{
			name:              "restart_not_allowed",
			applyErr:          &agent.RestartNotAllowedError{Message: "restart required but allow_restart=false"},
			expectedErrorType: "restart_not_allowed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockAgent := new(MockAgentLooper)
			logger := logrus.New()
			mockRecommendation := &agent.ProposedConfigResponse{}

			mockAgent.On("Logger").Return(logger)
			mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
			mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
			mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
			mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
			mockAgent.On("Guardrails", mock.Anything).Return(nil)
			mockAgent.expectNoCatalogCollectors()
			mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
			mockAgent.On("GetProposedConfig", mock.Anything).Return(mockRecommendation, nil)
			mockAgent.On("ApplyConfig", mock.Anything, mockRecommendation).Return(tc.applyErr)
			mockAgent.On("SendError", mock.Anything, mock.MatchedBy(func(p agent.ErrorPayload) bool {
				return p.ErrorType == tc.expectedErrorType &&
					strings.HasPrefix(p.ErrorMessage, "Failed to apply configuration: ")
			})).Return(nil)
			mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			go Runner(ctx, mockAgent)
			<-ctx.Done()
			time.Sleep(50 * time.Millisecond)

			mockAgent.AssertCalled(t, "SendError", mock.Anything, mock.MatchedBy(func(p agent.ErrorPayload) bool {
				return p.ErrorType == tc.expectedErrorType
			}))
			mockAgent.AssertCalled(t, "SendActiveConfig", mock.Anything, mock.Anything)
		})
	}
}

// stubAgentLooper  is required because the existing MockAgent stubs way too much for
// what we are testing here. It embeds CommonAgent (for real HTTP implementations) and provides
// minimal stubs for the adapter-specific methods not implemented by CommonAgent.
type stubAgentLooper struct {
	agent.CommonAgent
	agent.CatalogGetter
}

// Stub out the database interactions here (but note: not the DBtune API)
func (s *stubAgentLooper) GetSystemInfo(_ context.Context) ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (s *stubAgentLooper) GetActiveConfig(_ context.Context) (agent.ConfigArraySchema, error) {
	return agent.ConfigArraySchema{}, nil
}

func (s *stubAgentLooper) ApplyConfig(_ context.Context, _ *agent.ProposedConfigResponse) agent.ApplyConfigError {
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

func TestRunnerCatalogLoop(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	var sendCount, errCount int
	var mu sync.Mutex
	okCollector := queries.CatalogCollector{
		Name:     "ok_collector",
		Interval: 20 * time.Millisecond,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sendCount++
			return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
		},
	}
	errCollector := queries.CatalogCollector{
		Name:     "err_collector",
		Interval: 20 * time.Millisecond,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			mu.Lock()
			defer mu.Unlock()
			errCount++
			return nil, errors.New("boom")
		},
	}

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("CatalogCollectors").Return([]queries.CatalogCollector{okCollector, errCollector})
	mockAgent.On("SendCatalogPayload", mock.Anything, "ok_collector", mock.Anything).Return(nil)
	mockAgent.On("SendError", mock.Anything, mock.MatchedBy(func(p agent.ErrorPayload) bool {
		return p.ErrorType == "err_collector_error"
	})).Return(nil)

	// Window long enough for the stagger of the second collector plus a few ticks.
	ctx, cancel := context.WithTimeout(context.Background(), catalogStagger+1*time.Second)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, sendCount, 1, "ok collector should have ticked more than once")
	assert.Greater(t, errCount, 0, "err collector should have fired at least once after stagger")
	mockAgent.AssertCalled(t, "SendCatalogPayload", mock.Anything, "ok_collector", mock.Anything)
	mockAgent.AssertCalled(t, "SendError", mock.Anything, mock.Anything)
}

func TestRunnerCatalogLoop_NilDataSkipsSend(t *testing.T) {
	mockAgent := new(MockAgentLooper)
	logger := logrus.New()

	nilCollector := queries.CatalogCollector{
		Name:     "nil_collector",
		Interval: 20 * time.Millisecond,
		Collect:  func(_ context.Context) (*queries.CollectResult, error) { return nil, nil },
	}

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("CatalogCollectors").Return([]queries.CatalogCollector{nilCollector})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	mockAgent.AssertNotCalled(t, "SendCatalogPayload")
}

// setupMinimalAgent registers all non-catalog mock expectations so a
// catalog-loop test only has to declare its CatalogCollectors / SendCatalogPayload
// expectations.
func setupMinimalAgent(t *testing.T) (*MockAgentLooper, *logrus.Logger) {
	t.Helper()
	m := new(MockAgentLooper)
	logger := logrus.New()
	m.On("Logger").Return(logger)
	m.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, nil)
	m.On("SendMetrics", mock.Anything, mock.Anything).Return(nil)
	m.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, nil)
	m.On("SendSystemInfo", mock.Anything, mock.Anything).Return(nil)
	m.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, nil)
	m.On("SendActiveConfig", mock.Anything, mock.Anything).Return(nil)
	m.On("GetProposedConfig", mock.Anything).Return(nil, nil)
	m.On("Guardrails", mock.Anything).Return(nil)
	return m, logger
}

// withBootstrapNames swaps the package-level bootstrap list for the
// duration of a test. Returns a cleanup that restores the original.
func withBootstrapNames(t *testing.T, names ...string) {
	t.Helper()
	orig := bootstrapCollectorNames
	bootstrapCollectorNames = names
	t.Cleanup(func() { bootstrapCollectorNames = orig })
}

// TestRunnerCatalogLoop_BootstrapRunsBeforeOthers: every bootstrap
// collector's Collect+Send must finish before any non-bootstrap
// collector's first Collect starts.
func TestRunnerCatalogLoop_BootstrapRunsBeforeOthers(t *testing.T) {
	mockAgent, _ := setupMinimalAgent(t)
	withBootstrapNames(t, "bootstrap_inv")

	type event struct {
		name string
		kind string // "collect" | "send"
		t    time.Time
	}
	var (
		mu     sync.Mutex
		events []event
	)
	record := func(name, kind string) {
		mu.Lock()
		events = append(events, event{name: name, kind: kind, t: time.Now()})
		mu.Unlock()
	}

	bootstrap := queries.CatalogCollector{
		Name:     "bootstrap_inv",
		Interval: 1 * time.Hour, // long: don't interfere with the window
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			record("bootstrap_inv", "collect")
			time.Sleep(30 * time.Millisecond) // pretend a non-trivial query
			return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
		},
	}
	other := queries.CatalogCollector{
		Name:     "other",
		Interval: 1 * time.Hour,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			record("other", "collect")
			return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
		},
	}

	mockAgent.On("CatalogCollectors").Return([]queries.CatalogCollector{bootstrap, other})
	mockAgent.On("SendCatalogPayload", mock.Anything, "bootstrap_inv", mock.Anything).
		Run(func(_ mock.Arguments) { record("bootstrap_inv", "send") }).
		Return(nil)
	mockAgent.On("SendCatalogPayload", mock.Anything, "other", mock.Anything).
		Run(func(_ mock.Arguments) { record("other", "send") }).
		Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), catalogStagger+500*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Find the bootstrap Send and the first "other" Collect, assert ordering.
	var bootstrapSendAt, otherFirstCollectAt time.Time
	for _, e := range events {
		if e.name == "bootstrap_inv" && e.kind == "send" && bootstrapSendAt.IsZero() {
			bootstrapSendAt = e.t
		}
		if e.name == "other" && e.kind == "collect" && otherFirstCollectAt.IsZero() {
			otherFirstCollectAt = e.t
		}
	}
	assert.False(t, bootstrapSendAt.IsZero(), "bootstrap Send must have been recorded")
	assert.False(t, otherFirstCollectAt.IsZero(), "other Collect must have been recorded")
	assert.True(t, bootstrapSendAt.Before(otherFirstCollectAt),
		"bootstrap Send (%s) must precede other Collect (%s)", bootstrapSendAt, otherFirstCollectAt)
}

// TestRunnerCatalogLoop_BootstrapSkipsFirstTick: after the synchronous
// bootstrap pass, the collector's ticker goroutine must NOT re-fire Collect
// immediately — it should wait one full Interval. With a long Interval and a
// short test window, exactly one Collect call is expected.
func TestRunnerCatalogLoop_BootstrapSkipsFirstTick(t *testing.T) {
	mockAgent, _ := setupMinimalAgent(t)
	withBootstrapNames(t, "bootstrap_inv")

	var (
		mu           sync.Mutex
		collectCalls int
	)
	bootstrap := queries.CatalogCollector{
		Name:     "bootstrap_inv",
		Interval: 1 * time.Hour,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			mu.Lock()
			collectCalls++
			mu.Unlock()
			return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
		},
	}

	mockAgent.On("CatalogCollectors").Return([]queries.CatalogCollector{bootstrap})
	mockAgent.On("SendCatalogPayload", mock.Anything, "bootstrap_inv", mock.Anything).Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), catalogStagger+300*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, collectCalls,
		"bootstrap collector should Collect once (the bootstrap pass) and skip the ticker's first tick")
}

// TestRunnerCatalogLoop_BootstrapFailureDoesNotBlockOthers: a failing
// bootstrap is logged-and-continued; non-bootstrap collectors still run.
func TestRunnerCatalogLoop_BootstrapFailureDoesNotBlockOthers(t *testing.T) {
	mockAgent, _ := setupMinimalAgent(t)
	withBootstrapNames(t, "bootstrap_inv")

	var (
		mu             sync.Mutex
		otherCollected bool
	)
	bootstrap := queries.CatalogCollector{
		Name:     "bootstrap_inv",
		Interval: 1 * time.Hour,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			return nil, errors.New("bootstrap collect failed")
		},
	}
	other := queries.CatalogCollector{
		Name:     "other",
		Interval: 20 * time.Millisecond,
		Collect: func(_ context.Context) (*queries.CollectResult, error) {
			mu.Lock()
			otherCollected = true
			mu.Unlock()
			return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
		},
	}

	mockAgent.On("CatalogCollectors").Return([]queries.CatalogCollector{bootstrap, other})
	mockAgent.On("SendCatalogPayload", mock.Anything, "other", mock.Anything).Return(nil)
	mockAgent.On("SendError", mock.Anything, mock.MatchedBy(func(p agent.ErrorPayload) bool {
		return p.ErrorType == "bootstrap_inv_error"
	})).Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), catalogStagger+300*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, otherCollected, "non-bootstrap collector must still run after bootstrap fails")
}

// TestRunnerCatalogLoop_BootstrapRunsConcurrently: bootstrap collectors
// fire in parallel goroutines staggered by catalogStagger, not strictly
// sequentially. Each collector sleeps long enough that sequential
// execution would be obviously slower than concurrent + stagger.
func TestRunnerCatalogLoop_BootstrapRunsConcurrently(t *testing.T) {
	mockAgent, _ := setupMinimalAgent(t)
	withBootstrapNames(t, "boot_a", "boot_b", "boot_c")

	// sleep > catalogStagger so the difference between concurrent
	// (start-span ≈ (N-1)*catalogStagger) and sequential (start-span ≥
	// (N-1)*sleep) is unambiguous. With N=3:
	//   sequential start-span ≥ 2*sleep = 1000ms
	//   concurrent start-span ≈ 2*catalogStagger = 400ms
	const sleep = 500 * time.Millisecond

	type observation struct {
		name    string
		startAt time.Time
	}
	var (
		mu           sync.Mutex
		observations []observation
	)
	makeCollector := func(name string) queries.CatalogCollector {
		return queries.CatalogCollector{
			Name:     name,
			Interval: 1 * time.Hour,
			Collect: func(_ context.Context) (*queries.CollectResult, error) {
				mu.Lock()
				observations = append(observations, observation{name: name, startAt: time.Now()})
				mu.Unlock()
				time.Sleep(sleep)
				return &queries.CollectResult{JSON: []byte(`{"ok":true}`)}, nil
			},
		}
	}

	collectors := []queries.CatalogCollector{
		makeCollector("boot_a"),
		makeCollector("boot_b"),
		makeCollector("boot_c"),
	}
	mockAgent.On("CatalogCollectors").Return(collectors)
	for _, c := range collectors {
		mockAgent.On("SendCatalogPayload", mock.Anything, c.Name, mock.Anything).Return(nil)
	}

	// Window covers concurrent bootstrap (sleep + 2*stagger ≈ 900ms)
	// with margin; a sequential bootstrap would need 3*sleep = 1500ms
	// just to start the last collector, so the test would still detect
	// that mode of failure but via the start-span assertion rather than
	// missing observations.
	ctx, cancel := context.WithTimeout(context.Background(), sleep+3*catalogStagger+500*time.Millisecond)
	defer cancel()

	go Runner(ctx, mockAgent)
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, observations, 3, "all three bootstrap collectors must have started")

	// Bound: concurrent start-span ≈ 2*catalogStagger plus scheduler
	// jitter, which is well under one ``sleep``. Sequential would push
	// it to ≥ 2*sleep.
	first := observations[0].startAt
	last := observations[0].startAt
	for _, o := range observations[1:] {
		if o.startAt.Before(first) {
			first = o.startAt
		}
		if o.startAt.After(last) {
			last = o.startAt
		}
	}
	span := last.Sub(first)
	assert.Less(t, span, sleep,
		"bootstrap collectors must run concurrently: spread between first/last start was %s (per-collector sleep = %s, expected ≈ %s)",
		span, sleep, 2*catalogStagger)
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

func TestHandleCollectorError_RecoverySuppressed(t *testing.T) {
	m := new(MockAgentLooper)
	m.On("Logger").Return(logrus.New())

	err := handleCollectorError(context.Background(), m, "catalog_view", errors.New("recovery in progress"))

	assert.NoError(t, err, "recovery errors must be suppressed")
	m.AssertNotCalled(t, "SendError", mock.Anything, mock.Anything)
}

func TestHandleCollectorError_NonRecoveryReportsAndReturns(t *testing.T) {
	m := new(MockAgentLooper)
	m.On("Logger").Return(logrus.New())
	m.On("SendError", mock.Anything, mock.MatchedBy(func(p agent.ErrorPayload) bool {
		return p.ErrorType == "catalog_view_error" && strings.Contains(p.ErrorMessage, "boom")
	})).Return(nil)

	err := handleCollectorError(context.Background(), m, "catalog_view", errors.New("boom"))

	assert.Error(t, err, "non-recovery errors must propagate")
	m.AssertCalled(t, "SendError", mock.Anything, mock.Anything)
}

// Connection-level failures happen routinely during a database restart. They
// must be suppressed like recovery errors — otherwise every catalog collector
// fires a "<collector>_error" report that the backend cannot classify (A001).
func TestHandleCollectorError_ConnectionErrorSuppressed(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"admin shutdown 57P01", &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}},
		{"cannot connect now 57P03", &pgconn.PgError{Code: "57P03", Message: "the database system is shutting down"}},
		{"connection refused", errors.New("failed to connect to `host=db`: dial error: connection refused")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := new(MockAgentLooper)
			m.On("Logger").Return(logrus.New())

			err := handleCollectorError(context.Background(), m, "catalog_view", tt.err)

			assert.NoError(t, err, "connection errors must be suppressed during restart")
			m.AssertNotCalled(t, "SendError", mock.Anything, mock.Anything)
		})
	}
}
