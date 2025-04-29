package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"

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

func (m *MockAgentLooper) GetMetrics() ([]utils.FlatValue, error) {
	args := m.Called()
	return args.Get(0).([]utils.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendMetrics(metrics []utils.FlatValue) error {
	args := m.Called(metrics)
	return args.Error(0)
}

func (m *MockAgentLooper) GetSystemInfo() ([]utils.FlatValue, error) {
	args := m.Called()
	return args.Get(0).([]utils.FlatValue), args.Error(1)
}

func (m *MockAgentLooper) SendSystemInfo(info []utils.FlatValue) error {
	args := m.Called(info)
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

func (m *MockAgentLooper) Guardrails() *agent.GuardrailSignal {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*agent.GuardrailSignal)
}

func (m *MockAgentLooper) SendGuardrailSignal(signal agent.GuardrailSignal) error {
	args := m.Called(signal)
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

		fn := func() error {
			mu.Lock()
			counter++
			mu.Unlock()
			return nil
		}

		go runWithTicker(ctx, ticker, "test", logger, fn)

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

		go runWithTicker(ctx, ticker, "test", logger, fn)

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
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("SendHeartbeat").Return(nil)
	mockAgent.On("GetMetrics").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(nil, nil)
	mockAgent.On("Guardrails").Return(nil)

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
	mockAgent.On("Logger").Return(logger)
	mockAgent.On("SendHeartbeat").Return(errors.New("heartbeat error"))
	mockAgent.On("GetMetrics").Return([]utils.FlatValue{}, errors.New("metrics error"))
	mockAgent.On("GetSystemInfo").Return([]utils.FlatValue{}, errors.New("system info error"))
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, errors.New("config error"))
	mockAgent.On("Guardrails").Return(nil)

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

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("SendHeartbeat").Return(nil)
	mockAgent.On("GetMetrics").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)
	mockAgent.On("Guardrails").Return(nil, nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(mockRecommendation, nil)
	mockAgent.On("ApplyConfig", mockRecommendation).Return(nil)

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

	mockAgent.On("Logger").Return(logger)
	mockAgent.On("SendHeartbeat").Return(nil)
	mockAgent.On("GetMetrics").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendMetrics", mock.Anything).Return(nil)
	mockAgent.On("GetSystemInfo").Return([]utils.FlatValue{}, nil)
	mockAgent.On("SendSystemInfo", mock.Anything).Return(nil)
	mockAgent.On("Guardrails").Return(nil)
	mockAgent.On("GetActiveConfig").Return(agent.ConfigArraySchema{}, nil)
	mockAgent.On("SendActiveConfig", mock.Anything).Return(nil)
	mockAgent.On("GetProposedConfig").Return(nil, nil)

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
