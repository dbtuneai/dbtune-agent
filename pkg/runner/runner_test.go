package runner

import (
	"context"
	"errors"
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

func (m *MockAgentLooper) GetPgStatistic(ctx context.Context) (*agent.PgStatisticPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgStatisticPayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgStatistic(ctx context.Context, payload *agent.PgStatisticPayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgStatUserTables(ctx context.Context) (*agent.PgStatUserTablePayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgStatUserTablePayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgStatUserTables(ctx context.Context, payload *agent.PgStatUserTablePayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgClass(ctx context.Context) (*agent.PgClassPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*agent.PgClassPayload), args.Error(1)
}

func (m *MockAgentLooper) SendPgClass(ctx context.Context, payload *agent.PgClassPayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

func (m *MockAgentLooper) GetPgStatActivity(ctx context.Context) (*agent.PgStatActivityPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatActivityPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatActivity(ctx context.Context, payload *agent.PgStatActivityPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatDatabaseAll(ctx context.Context) (*agent.PgStatDatabasePayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatDatabasePayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatDatabaseAll(ctx context.Context, payload *agent.PgStatDatabasePayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatDatabaseConflicts(ctx context.Context) (*agent.PgStatDatabaseConflictsPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatDatabaseConflictsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatDatabaseConflicts(ctx context.Context, payload *agent.PgStatDatabaseConflictsPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatArchiver(ctx context.Context) (*agent.PgStatArchiverPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatArchiverPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatArchiver(ctx context.Context, payload *agent.PgStatArchiverPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatBgwriterAll(ctx context.Context) (*agent.PgStatBgwriterPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatBgwriterPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatBgwriterAll(ctx context.Context, payload *agent.PgStatBgwriterPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatCheckpointerAll(ctx context.Context) (*agent.PgStatCheckpointerPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatCheckpointerPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatCheckpointerAll(ctx context.Context, payload *agent.PgStatCheckpointerPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatWalAll(ctx context.Context) (*agent.PgStatWalPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatWalPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatWalAll(ctx context.Context, payload *agent.PgStatWalPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatIO(ctx context.Context) (*agent.PgStatIOPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatIOPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatIO(ctx context.Context, payload *agent.PgStatIOPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatReplication(ctx context.Context) (*agent.PgStatReplicationPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatReplicationPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatReplication(ctx context.Context, payload *agent.PgStatReplicationPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatReplicationSlots(ctx context.Context) (*agent.PgStatReplicationSlotsPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatReplicationSlotsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatReplicationSlots(ctx context.Context, payload *agent.PgStatReplicationSlotsPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatSlru(ctx context.Context) (*agent.PgStatSlruPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatSlruPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatSlru(ctx context.Context, payload *agent.PgStatSlruPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatUserIndexes(ctx context.Context) (*agent.PgStatUserIndexesPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatUserIndexesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatUserIndexes(ctx context.Context, payload *agent.PgStatUserIndexesPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatioUserTables(ctx context.Context) (*agent.PgStatioUserTablesPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatioUserTablesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatioUserTables(ctx context.Context, payload *agent.PgStatioUserTablesPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatioUserIndexes(ctx context.Context) (*agent.PgStatioUserIndexesPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatioUserIndexesPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatioUserIndexes(ctx context.Context, payload *agent.PgStatioUserIndexesPayload) error {
	return m.Called(ctx, payload).Error(0)
}
func (m *MockAgentLooper) GetPgStatUserFunctions(ctx context.Context) (*agent.PgStatUserFunctionsPayload, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*agent.PgStatUserFunctionsPayload), args.Error(1)
}
func (m *MockAgentLooper) SendPgStatUserFunctions(ctx context.Context, payload *agent.PgStatUserFunctionsPayload) error {
	return m.Called(ctx, payload).Error(0)
}

func (m *MockAgentLooper) SendError(ctx context.Context, payload agent.ErrorPayload) error {
	args := m.Called(ctx, payload)
	return args.Error(0)
}

// setupCatalogMocksSuccess sets up mock expectations for all 15 new catalog views (success case)
func setupCatalogMocksSuccess(m *MockAgentLooper) {
	m.On("GetPgStatActivity", mock.Anything).Return(&agent.PgStatActivityPayload{}, nil)
	m.On("SendPgStatActivity", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatDatabaseAll", mock.Anything).Return(&agent.PgStatDatabasePayload{}, nil)
	m.On("SendPgStatDatabaseAll", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatDatabaseConflicts", mock.Anything).Return(&agent.PgStatDatabaseConflictsPayload{}, nil)
	m.On("SendPgStatDatabaseConflicts", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatArchiver", mock.Anything).Return(&agent.PgStatArchiverPayload{}, nil)
	m.On("SendPgStatArchiver", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatBgwriterAll", mock.Anything).Return(&agent.PgStatBgwriterPayload{}, nil)
	m.On("SendPgStatBgwriterAll", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatCheckpointerAll", mock.Anything).Return(&agent.PgStatCheckpointerPayload{}, nil)
	m.On("SendPgStatCheckpointerAll", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatWalAll", mock.Anything).Return(&agent.PgStatWalPayload{}, nil)
	m.On("SendPgStatWalAll", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatIO", mock.Anything).Return(&agent.PgStatIOPayload{}, nil)
	m.On("SendPgStatIO", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatReplication", mock.Anything).Return(&agent.PgStatReplicationPayload{}, nil)
	m.On("SendPgStatReplication", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatReplicationSlots", mock.Anything).Return(&agent.PgStatReplicationSlotsPayload{}, nil)
	m.On("SendPgStatReplicationSlots", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.On("GetPgStatSlru", mock.Anything).Return(&agent.PgStatSlruPayload{}, nil)
	m.On("SendPgStatSlru", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatUserIndexes", mock.Anything).Return(&agent.PgStatUserIndexesPayload{}, nil)
	m.On("SendPgStatUserIndexes", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatioUserTables", mock.Anything).Return(&agent.PgStatioUserTablesPayload{}, nil)
	m.On("SendPgStatioUserTables", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatioUserIndexes", mock.Anything).Return(&agent.PgStatioUserIndexesPayload{}, nil)
	m.On("SendPgStatioUserIndexes", mock.Anything, mock.Anything).Return(nil)
	m.On("GetPgStatUserFunctions", mock.Anything).Return(&agent.PgStatUserFunctionsPayload{}, nil)
	m.On("SendPgStatUserFunctions", mock.Anything, mock.Anything).Return(nil)
}

// setupCatalogMocksError sets up mock expectations for all 15 new catalog views (error case)
func setupCatalogMocksError(m *MockAgentLooper) {
	m.On("GetPgStatActivity", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatDatabaseAll", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatDatabaseConflicts", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatArchiver", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatBgwriterAll", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatCheckpointerAll", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatWalAll", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatIO", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatReplication", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatReplicationSlots", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatSlru", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatUserIndexes", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatioUserTables", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatioUserIndexes", mock.Anything).Return(nil, errors.New("error"))
	m.On("GetPgStatUserFunctions", mock.Anything).Return(nil, errors.New("error"))
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
	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
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
	mockAgent.On("GetPgStatistic", mock.Anything).Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables", mock.Anything).Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgClass", mock.Anything).Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything, mock.Anything).Return(nil)
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
	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("GetMetrics", mock.Anything).Return([]metrics.FlatValue{}, errors.New("metrics error"))
	mockAgent.On("GetSystemInfo", mock.Anything).Return([]metrics.FlatValue{}, errors.New("system info error"))

	mockAgent.On("GetDDL", mock.Anything).Return(nil, errors.New("ddl error"))
	mockAgent.On("SendError", mock.Anything, mock.AnythingOfType("agent.ErrorPayload")).Return(nil)
	mockAgent.On("GetActiveConfig", mock.Anything).Return(agent.ConfigArraySchema{}, errors.New("config error"))
	mockAgent.On("Guardrails", mock.Anything).Return(nil)
	mockAgent.On("GetPgStatistic", mock.Anything).Return(nil, errors.New("pg_statistic error"))
	mockAgent.On("GetPgStatUserTables", mock.Anything).Return(nil, errors.New("pg_stat_user_tables error"))
	mockAgent.On("GetPgClass", mock.Anything).Return(nil, errors.New("pg_class error"))
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

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
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
	mockAgent.On("GetPgStatistic", mock.Anything).Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables", mock.Anything).Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgClass", mock.Anything).Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything, mock.Anything).Return(nil)
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

	// Note: SendHeartbeat is not expected to be called during the short test window
	// because the heartbeat ticker has skipFirst: true
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
	mockAgent.On("GetPgStatistic", mock.Anything).Return(&agent.PgStatisticPayload{}, nil)
	mockAgent.On("SendPgStatistic", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgStatUserTables", mock.Anything).Return(&agent.PgStatUserTablePayload{}, nil)
	mockAgent.On("SendPgStatUserTables", mock.Anything, mock.Anything).Return(nil)
	mockAgent.On("GetPgClass", mock.Anything).Return(&agent.PgClassPayload{}, nil)
	mockAgent.On("SendPgClass", mock.Anything, mock.Anything).Return(nil)
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
func (s *stubAgentLooper) GetPgStatistic(ctx context.Context) (*agent.PgStatisticPayload, error) {
	return &agent.PgStatisticPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatUserTables(ctx context.Context) (*agent.PgStatUserTablePayload, error) {
	return &agent.PgStatUserTablePayload{}, nil
}
func (s *stubAgentLooper) GetPgClass(ctx context.Context) (*agent.PgClassPayload, error) {
	return &agent.PgClassPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatActivity(ctx context.Context) (*agent.PgStatActivityPayload, error) {
	return &agent.PgStatActivityPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatDatabaseAll(ctx context.Context) (*agent.PgStatDatabasePayload, error) {
	return &agent.PgStatDatabasePayload{}, nil
}
func (s *stubAgentLooper) GetPgStatDatabaseConflicts(ctx context.Context) (*agent.PgStatDatabaseConflictsPayload, error) {
	return &agent.PgStatDatabaseConflictsPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatArchiver(ctx context.Context) (*agent.PgStatArchiverPayload, error) {
	return &agent.PgStatArchiverPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatBgwriterAll(ctx context.Context) (*agent.PgStatBgwriterPayload, error) {
	return &agent.PgStatBgwriterPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatCheckpointerAll(ctx context.Context) (*agent.PgStatCheckpointerPayload, error) {
	return &agent.PgStatCheckpointerPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatWalAll(ctx context.Context) (*agent.PgStatWalPayload, error) {
	return &agent.PgStatWalPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatIO(ctx context.Context) (*agent.PgStatIOPayload, error) {
	return &agent.PgStatIOPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatReplication(ctx context.Context) (*agent.PgStatReplicationPayload, error) {
	return &agent.PgStatReplicationPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatReplicationSlots(ctx context.Context) (*agent.PgStatReplicationSlotsPayload, error) {
	return &agent.PgStatReplicationSlotsPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatSlru(ctx context.Context) (*agent.PgStatSlruPayload, error) {
	return &agent.PgStatSlruPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatUserIndexes(ctx context.Context) (*agent.PgStatUserIndexesPayload, error) {
	return &agent.PgStatUserIndexesPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatioUserTables(ctx context.Context) (*agent.PgStatioUserTablesPayload, error) {
	return &agent.PgStatioUserTablesPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatioUserIndexes(ctx context.Context) (*agent.PgStatioUserIndexesPayload, error) {
	return &agent.PgStatioUserIndexesPayload{}, nil
}
func (s *stubAgentLooper) GetPgStatUserFunctions(ctx context.Context) (*agent.PgStatUserFunctionsPayload, error) {
	return &agent.PgStatUserFunctionsPayload{}, nil
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
