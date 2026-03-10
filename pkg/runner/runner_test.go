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

func (m *MockAgentLooper) SendHeartbeat() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockAgentLooper) GetMetrics() ([]metrics.FlatValue, error) {
	args := m.Called()
	return args.Get(0).([]metrics.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendMetrics(metrics []metrics.FlatValue) error {
	args := m.Called(metrics)
	return args.Error(0)
}

func (m *MockAgentLooper) GetSystemInfo() ([]metrics.FlatValue, error) {
	args := m.Called()
	return args.Get(0).([]metrics.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendSystemInfo(info []metrics.FlatValue) error {
	args := m.Called(info)
	return args.Error(0)
}

func (m *MockAgentLooper) GetDDL() (*agent.DDLPayload, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.DDLPayload), args.Error(1)
}

func (m *MockAgentLooper) SendDDL(payload *agent.DDLPayload) error {
	args := m.Called(payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetActiveConfig() (agent.ConfigArraySchema, error) {
	args := m.Called()
	return args.Get(0).(agent.ConfigArraySchema), args.Error(1)
}

func (m *MockAgentLooper) SendActiveConfig(config agent.ConfigArraySchema) error {
	args := m.Called(config)
	return args.Error(0)
}

func (m *MockAgentLooper) GetProposedConfig() (*agent.ProposedConfigResponse, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.ProposedConfigResponse), args.Error(1)
}

func (m *MockAgentLooper) ApplyConfig(config *agent.ProposedConfigResponse) error {
	args := m.Called(config)
	return args.Error(0)
}

func (m *MockAgentLooper) Guardrails() *guardrails.Signal {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*guardrails.Signal)
}

func (m *MockAgentLooper) SendGuardrailSignal(signal guardrails.Signal) error {
	args := m.Called(signal)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgStatistic() (*agent.PgStatisticPayload, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgStatisticPayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgStatistic(payload *agent.PgStatisticPayload) error {
	args := m.Called(payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgStatUserTables() (*agent.PgStatUserTablePayload, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgStatUserTablePayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgStatUserTables(payload *agent.PgStatUserTablePayload) error {
	args := m.Called(payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgClass() (*agent.PgClassPayload, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgClassPayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgClass(payload *agent.PgClassPayload) error {
	args := m.Called(payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgStatActivity() (*agent.PgStatActivityPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatActivityPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatActivity(payload *agent.PgStatActivityPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatDatabaseAll() (*agent.PgStatDatabasePayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatDatabasePayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatDatabaseAll(payload *agent.PgStatDatabasePayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatDatabaseConflicts() (*agent.PgStatDatabaseConflictsPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatDatabaseConflictsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatDatabaseConflicts(payload *agent.PgStatDatabaseConflictsPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatArchiver() (*agent.PgStatArchiverPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatArchiverPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatArchiver(payload *agent.PgStatArchiverPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatBgwriterAll() (*agent.PgStatBgwriterPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatBgwriterPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatBgwriterAll(payload *agent.PgStatBgwriterPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatCheckpointerAll() (*agent.PgStatCheckpointerPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatCheckpointerPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatCheckpointerAll(payload *agent.PgStatCheckpointerPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatWalAll() (*agent.PgStatWalPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatWalPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatWalAll(payload *agent.PgStatWalPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatIO() (*agent.PgStatIOPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatIOPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatIO(payload *agent.PgStatIOPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatReplication() (*agent.PgStatReplicationPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatReplicationPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatReplication(payload *agent.PgStatReplicationPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatReplicationSlots() (*agent.PgStatReplicationSlotsPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatReplicationSlotsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatReplicationSlots(payload *agent.PgStatReplicationSlotsPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatSlru() (*agent.PgStatSlruPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatSlruPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatSlru(payload *agent.PgStatSlruPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatUserIndexes() (*agent.PgStatUserIndexesPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatUserIndexesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatUserIndexes(payload *agent.PgStatUserIndexesPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatioUserTables() (*agent.PgStatioUserTablesPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatioUserTablesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatioUserTables(payload *agent.PgStatioUserTablesPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatioUserIndexes() (*agent.PgStatioUserIndexesPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatioUserIndexesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatioUserIndexes(payload *agent.PgStatioUserIndexesPayload) error {
	return m.Called(payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatUserFunctions() (*agent.PgStatUserFunctionsPayload, error) {
	args := m.Called()
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatUserFunctionsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatUserFunctions(payload *agent.PgStatUserFunctionsPayload) error {
	return m.Called(payload).Error(0)
}

func (m *MockAgentLooper) SendError(payload agent.ErrorPayload) error {
	args := m.Called(payload)
	return args.Error(0)
}

// setupCatalogMocksSuccess sets up mock expectations for all 15 new catalog views (success case)
func setupCatalogMocksSuccess(m *MockAgentLooper) {
	m.On("GetPgStatActivity").Return(&agent.PgStatActivityPayload{}, nil)
	m.On("SendPgStatActivity", mock.Anything).Return(nil)
	m.On("GetPgStatDatabaseAll").Return(&agent.PgStatDatabasePayload{}, nil)
	m.On("SendPgStatDatabaseAll", mock.Anything).Return(nil)
	m.On("GetPgStatDatabaseConflicts").Return(&agent.PgStatDatabaseConflictsPayload{}, nil)
	m.On("SendPgStatDatabaseConflicts", mock.Anything).Return(nil)
	m.On("GetPgStatArchiver").Return(&agent.PgStatArchiverPayload{}, nil)
	m.On("SendPgStatArchiver", mock.Anything).Return(nil)
	m.On("GetPgStatBgwriterAll").Return(&agent.PgStatBgwriterPayload{}, nil)
	m.On("SendPgStatBgwriterAll", mock.Anything).Return(nil)
	m.On("GetPgStatCheckpointerAll").Return(&agent.PgStatCheckpointerPayload{}, nil)
	m.On("SendPgStatCheckpointerAll", mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatWalAll").Return(&agent.PgStatWalPayload{}, nil)
	m.On("SendPgStatWalAll", mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatIO").Return(&agent.PgStatIOPayload{}, nil)
	m.On("SendPgStatIO", mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatReplication").Return(&agent.PgStatReplicationPayload{}, nil)
	m.On("SendPgStatReplication", mock.Anything).Return(nil)
	m.On("GetPgStatReplicationSlots").Return(&agent.PgStatReplicationSlotsPayload{}, nil)
	m.On("SendPgStatReplicationSlots", mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatSlru").Return(&agent.PgStatSlruPayload{}, nil)
	m.On("SendPgStatSlru", mock.Anything).Return(nil)
	m.On("GetPgStatUserIndexes").Return(&agent.PgStatUserIndexesPayload{}, nil)
	m.On("SendPgStatUserIndexes", mock.Anything).Return(nil)
	m.On("GetPgStatioUserTables").Return(&agent.PgStatioUserTablesPayload{}, nil)
	m.On("SendPgStatioUserTables", mock.Anything).Return(nil)
	m.On("GetPgStatioUserIndexes").Return(&agent.PgStatioUserIndexesPayload{}, nil)
	m.On("SendPgStatioUserIndexes", mock.Anything).Return(nil)
	m.On("GetPgStatUserFunctions").Return(&agent.PgStatUserFunctionsPayload{}, nil)
	m.On("SendPgStatUserFunctions", mock.Anything).Return(nil)
}

// setupCatalogMocksError sets up mock expectations for all 15 new catalog views (error case)
func setupCatalogMocksError(m *MockAgentLooper) {
	m.On("GetPgStatActivity").Return(nil, errors.New("error"))
	m.On("GetPgStatDatabaseAll").Return(nil, errors.New("error"))
	m.On("GetPgStatDatabaseConflicts").Return(nil, errors.New("error"))
	m.On("GetPgStatArchiver").Return(nil, errors.New("error"))
	m.On("GetPgStatBgwriterAll").Return(nil, errors.New("error"))
	m.On("GetPgStatCheckpointerAll").Return(nil, errors.New("error"))
	m.On("GetPgStatWalAll").Return(nil, errors.New("error"))
	m.On("GetPgStatIO").Return(nil, errors.New("error"))
	m.On("GetPgStatReplication").Return(nil, errors.New("error"))
	m.On("GetPgStatReplicationSlots").Return(nil, errors.New("error"))
	m.On("GetPgStatSlru").Return(nil, errors.New("error"))
	m.On("GetPgStatUserIndexes").Return(nil, errors.New("error"))
	m.On("GetPgStatioUserTables").Return(nil, errors.New("error"))
	m.On("GetPgStatioUserIndexes").Return(nil, errors.New("error"))
	m.On("GetPgStatUserFunctions").Return(nil, errors.New("error"))
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

		fn := func() error {
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
		fn := func() error {
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
	mockAgent.On("GetMetrics").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)

	mockAgent.On("GetDDL").Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(nil, nil)
	mockAgent.On("Guardrails").Return(nil)
	mockAgent.On("GetPgStatistic").Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables").Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything).Return(nil)
	mockAgent.On("GetPgClass").Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything).Return(nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	done := make(chan bool)
	go func() {
		Runner(mockAgent)
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

	// Setup mock expectations with errors
	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics").Return([]metrics.FlatValue{}, errors.New("metrics error"))
	mockAgent.On("GetSystemInfo").Return([]metrics.FlatValue{}, errors.New("system info error"))

	mockAgent.On("GetDDL").Return(nil, errors.New("ddl error"))
	mockAgent.On("SendError", mock.AnythingOfType("agent.ErrorPayload")).Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, errors.New("config error"))
	mockAgent.On("Guardrails").Return(nil)
	mockAgent.On("GetPgStatistic").Return(nil, errors.New("pg_statistic error"))
	mockAgent.On("GetPgStatUserTables").Return(nil, errors.New("pg_stat_user_tables error"))
	mockAgent.On("GetPgClass").Return(nil, errors.New("pg_class error"))
	setupCatalogMocksError(mockAgent)

	// Run the Runner in a goroutine with a timeout
	done := make(chan bool)
	go func() {
		Runner(mockAgent)
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

	mockRecommendation := &agent.ProposedConfigResponse{}

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)

	mockAgent.On("GetDDL").Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything).Return(nil)
	mockAgent.On("Guardrails").Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(mockRecommendation, nil)
	mockAgent.On("ApplyConfig", mockRecommendation).Return(nil)
	mockAgent.On("GetPgStatistic").Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables").Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything).Return(nil)
	mockAgent.On("GetPgClass").Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything).Return(nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	done := make(chan bool)
	go func() {
		Runner(mockAgent)
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

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]metrics.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)

	mockAgent.On("GetDDL").Return(&agent.DDLPayload{Hash: "abc123"}, nil)
	mockAgent.On("SendDDL", mock.Anything).Return(nil)
	mockAgent.On("Guardrails").Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(nil, nil)
	mockAgent.On("GetPgStatistic").Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables").Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything).Return(nil)
	mockAgent.On("GetPgClass").Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything).Return(nil)
	setupCatalogMocksSuccess(mockAgent)

	// Run the Runner in a goroutine with a timeout
	done := make(chan bool)
	go func() {
		Runner(mockAgent)
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
func (s *stubAgentLooper) GetSystemInfo() ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (s *stubAgentLooper) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return agent.ConfigArraySchema{}, nil
}

func (s *stubAgentLooper) ApplyConfig(_ *agent.ProposedConfigResponse) error {
	return nil
}

func (s *stubAgentLooper) Guardrails() *guardrails.Signal {
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

	// Runner blocks forever, so we start it in a goroutine so
	// that we can later check that it has done something.
	// It might be a good idea if Runner takes a context
	// because the cancelable context in Runner doesn't seem
	// to be able to be accessed outside of it? So how do we
	// cancel?
	go Runner(stub)

	time.Sleep(200 * time.Millisecond)

	transport.AllActionsCalled(t)
}
