package utils

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendHookLevels(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "test-key")
	levels := hook.Levels()

	assert.Contains(t, levels, logrus.WarnLevel)
	assert.Contains(t, levels, logrus.ErrorLevel)
	assert.Contains(t, levels, logrus.FatalLevel)
	assert.Contains(t, levels, logrus.PanicLevel)
	assert.NotContains(t, levels, logrus.InfoLevel)
	assert.NotContains(t, levels, logrus.DebugLevel)
}

func TestBackendHookBuffersEntries(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "test-key")

	err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.ErrorLevel,
		Message: "test error",
		Time:    time.Now(),
		Data:    logrus.Fields{},
	})
	require.NoError(t, err)

	hook.mu.Lock()
	assert.Len(t, hook.buffer, 1)
	assert.Equal(t, "error", hook.buffer[0].Level)
	assert.Contains(t, hook.buffer[0].Message, "test error")
	hook.mu.Unlock()
}

func TestBackendHookBufferCap(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "test-key")

	for i := 0; i < maxBufferSize+10; i++ {
		err := hook.Fire(&logrus.Entry{
			Logger:  logrus.New(),
			Level:   logrus.WarnLevel,
			Message: "warn",
			Time:    time.Now(),
			Data:    logrus.Fields{},
		})
		require.NoError(t, err)
	}

	hook.mu.Lock()
	assert.Len(t, hook.buffer, maxBufferSize)
	hook.mu.Unlock()
}

func TestBackendHookRecursionGuard(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "test-key")

	hook.mu.Lock()
	hook.sending = true
	hook.mu.Unlock()

	err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.ErrorLevel,
		Message: "should be dropped",
		Time:    time.Now(),
		Data:    logrus.Fields{},
	})
	require.NoError(t, err)

	hook.mu.Lock()
	assert.Empty(t, hook.buffer)
	hook.mu.Unlock()
}

func TestBackendHookFlushSendsBatch(t *testing.T) {
	var mu sync.Mutex
	var batches [][]backendLogEntry

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-api-key", r.Header.Get("DBTUNE-API-KEY"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var entries []backendLogEntry
		require.NoError(t, json.Unmarshal(body, &entries))

		mu.Lock()
		batches = append(batches, entries)
		mu.Unlock()

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	hook := CreateBackendHook(server.URL, "test-api-key")

	for _, e := range []struct {
		level   logrus.Level
		message string
	}{
		{logrus.WarnLevel, "first warning"},
		{logrus.ErrorLevel, "an error occurred"},
		{logrus.WarnLevel, "second warning"},
	} {
		err := hook.Fire(&logrus.Entry{
			Logger:  logrus.New(),
			Level:   e.level,
			Message: e.message,
			Time:    time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			Data:    logrus.Fields{},
		})
		require.NoError(t, err)
	}

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()

	// Single batch POST, not 3 individual ones
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 3)
	assert.Equal(t, "first warning", batches[0][0].Message)
	assert.Equal(t, "warning", batches[0][0].Level)
	assert.Equal(t, "an error occurred", batches[0][1].Message)
	assert.Equal(t, "error", batches[0][1].Level)
	assert.Equal(t, "second warning", batches[0][2].Message)
	assert.Equal(t, "warning", batches[0][2].Level)
}

func TestBackendHookFlushClearsBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	hook := CreateBackendHook(server.URL, "key")

	err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.ErrorLevel,
		Message: "test",
		Time:    time.Now(),
		Data:    logrus.Fields{},
	})
	require.NoError(t, err)

	hook.Flush()

	hook.mu.Lock()
	assert.Empty(t, hook.buffer)
	hook.mu.Unlock()
}

func TestBackendHookIncludesFields(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "key")

	err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.ErrorLevel,
		Message: "db connection failed",
		Time:    time.Now(),
		Data: logrus.Fields{
			"host":  "localhost",
			"port":  5432,
			"error": "connection refused",
		},
	})
	require.NoError(t, err)

	hook.mu.Lock()
	defer hook.mu.Unlock()

	require.Len(t, hook.buffer, 1)
	assert.Contains(t, hook.buffer[0].Message, "db connection failed")
	assert.Contains(t, hook.buffer[0].Message, "localhost")
	assert.Contains(t, hook.buffer[0].Message, "5432")
	assert.Contains(t, hook.buffer[0].Message, "connection refused")
}

func TestBackendHookFlushNoopWhenEmpty(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "key")
	hook.Flush()

	hook.mu.Lock()
	assert.Empty(t, hook.buffer)
	hook.mu.Unlock()
}

func TestBackendHookIntegrationWithLogger(t *testing.T) {
	var mu sync.Mutex
	var received []backendLogEntry

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var entries []backendLogEntry
		_ = json.Unmarshal(body, &entries)

		mu.Lock()
		received = append(received, entries...)
		mu.Unlock()

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := logrus.New()
	hook := CreateBackendHook(server.URL, "key")
	logger.AddHook(hook)

	logger.Warn("a warning")
	logger.Error("an error")
	logger.Info("just info")
	logger.Debug("debug stuff")

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, received, 2)
	assert.Contains(t, received[0].Message, "a warning")
	assert.Contains(t, received[1].Message, "an error")
}
