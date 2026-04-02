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

func newTestServer(t *testing.T) (*httptest.Server, *[]backendLogEntry, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var received []backendLogEntry

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var entries []backendLogEntry
		require.NoError(t, json.Unmarshal(body, &entries))

		mu.Lock()
		received = append(received, entries...)
		mu.Unlock()

		w.WriteHeader(http.StatusNoContent)
	}))

	t.Cleanup(server.Close)
	return server, &received, &mu
}

func fireEntry(t *testing.T, hook *BackendHook, level logrus.Level, message string) {
	t.Helper()
	err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   level,
		Message: message,
		Time:    time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Data:    logrus.Fields{},
	})
	require.NoError(t, err)
}

func TestBackendHookLevels(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "test-key")
	defer hook.Close()

	levels := hook.Levels()

	assert.Contains(t, levels, logrus.WarnLevel)
	assert.Contains(t, levels, logrus.ErrorLevel)
	assert.Contains(t, levels, logrus.FatalLevel)
	assert.Contains(t, levels, logrus.PanicLevel)
	assert.NotContains(t, levels, logrus.InfoLevel)
	assert.NotContains(t, levels, logrus.DebugLevel)
}

func TestBackendHookBuffersEntries(t *testing.T) {
	server, received, mu := newTestServer(t)

	hook := CreateBackendHook(server.URL, "test-key")
	defer hook.Close()

	fireEntry(t, hook, logrus.ErrorLevel, "test error")
	hook.Flush()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *received, 1)
	assert.Equal(t, "error", (*received)[0].Level)
	assert.Contains(t, (*received)[0].Message, "test error")
}

func TestBackendHookDropsOnFullChannel(t *testing.T) {
	server, received, mu := newTestServer(t)

	bufSize := 5
	hook := CreateBackendHookWithConfig(server.URL, "test-key", time.Hour, bufSize)
	defer hook.Close()

	// Fire many more entries than the channel can hold. Some may be read by
	// the flush loop before the channel fills, so we can't assert an exact count.
	for i := 0; i < bufSize*10; i++ {
		fireEntry(t, hook, logrus.WarnLevel, "warn")
	}

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()
	// We should have received some entries, but not necessarily all of them.
	assert.Greater(t, len(*received), 0)
}

func TestBackendHookFlushSendsBatch(t *testing.T) {
	var mu sync.Mutex
	var batchCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-api-key", r.Header.Get("DBTUNE-API-KEY"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var entries []backendLogEntry
		require.NoError(t, json.Unmarshal(body, &entries))

		mu.Lock()
		batchCount++
		mu.Unlock()

		assert.Len(t, entries, 3)
		assert.Equal(t, "first warning", entries[0].Message)
		assert.Equal(t, "warning", entries[0].Level)
		assert.Equal(t, "an error occurred", entries[1].Message)
		assert.Equal(t, "error", entries[1].Level)
		assert.Equal(t, "second warning", entries[2].Message)
		assert.Equal(t, "warning", entries[2].Level)

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	hook := CreateBackendHook(server.URL, "test-api-key")
	defer hook.Close()

	for _, e := range []struct {
		level   logrus.Level
		message string
	}{
		{logrus.WarnLevel, "first warning"},
		{logrus.ErrorLevel, "an error occurred"},
		{logrus.WarnLevel, "second warning"},
	} {
		fireEntry(t, hook, e.level, e.message)
	}

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, batchCount, "should send a single batch, not individual entries")
}

func TestBackendHookFlushClearsBuffer(t *testing.T) {
	var mu sync.Mutex
	var batchCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		batchCount++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	hook := CreateBackendHook(server.URL, "key")
	defer hook.Close()

	fireEntry(t, hook, logrus.ErrorLevel, "test")
	hook.Flush()

	mu.Lock()
	count := batchCount
	mu.Unlock()
	assert.Equal(t, 1, count)

	// Second flush should not send anything.
	hook.Flush()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, batchCount)
}

func TestBackendHookIncludesFields(t *testing.T) {
	server, received, mu := newTestServer(t)

	hook := CreateBackendHook(server.URL, "key")
	defer hook.Close()

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

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *received, 1)
	assert.Contains(t, (*received)[0].Message, "db connection failed")
	assert.Contains(t, (*received)[0].Message, "localhost")
	assert.Contains(t, (*received)[0].Message, "5432")
	assert.Contains(t, (*received)[0].Message, "connection refused")
}

func TestBackendHookFlushNoopWhenEmpty(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "key")
	defer hook.Close()

	// Should not panic or block.
	hook.Flush()
}

func TestBackendHookCloseDrainsBuffer(t *testing.T) {
	server, received, mu := newTestServer(t)

	hook := CreateBackendHook(server.URL, "key")

	fireEntry(t, hook, logrus.ErrorLevel, "should be flushed on close")

	hook.Close()

	// Verify stop channel is closed (flushLoop exited).
	select {
	case <-hook.stop:
		// expected
	default:
		t.Fatal("stop channel should be closed after Close()")
	}

	// Verify the entry was sent to the backend.
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, *received, 1)
	assert.Contains(t, (*received)[0].Message, "should be flushed on close")
}

func TestBackendHookCloseIsIdempotent(t *testing.T) {
	hook := CreateBackendHook("http://localhost", "key")

	// Calling Close multiple times must not panic.
	hook.Close()
	hook.Close()
}

func TestBackendHookIntegrationWithLogger(t *testing.T) {
	server, received, mu := newTestServer(t)

	logger := logrus.New()
	hook := CreateBackendHook(server.URL, "key")
	defer hook.Close()
	logger.AddHook(hook)

	logger.Warn("a warning")
	logger.Error("an error")
	logger.Info("just info")
	logger.Debug("debug stuff")

	hook.Flush()

	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, *received, 2)
	assert.Contains(t, (*received)[0].Message, "a warning")
	assert.Contains(t, (*received)[1].Message, "an error")
}
