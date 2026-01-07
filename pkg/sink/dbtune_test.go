package sink

import (
	"context"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
)

func createTestSink() *DBTunePlatformSink {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	client := retryablehttp.NewClient()
	client.RetryMax = 1

	serverURL := dbtune.ServerURLs{
		ServerUrl: "https://test.example.com",
		ApiKey:    "test-key",
		DbID:      "test-db",
	}

	return NewDBTunePlatformSink(client, serverURL, logger)
}

func TestNewDBTunePlatformSink(t *testing.T) {
	sink := createTestSink()

	if sink.Name() != "dbtune-platform" {
		t.Errorf("expected name 'dbtune-platform', got %s", sink.Name())
	}

	if sink.metricsBuffer == nil {
		t.Error("expected metrics buffer to be initialized")
	}
}

func TestHandleMetrics_Buffering(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	// Create multiple metrics events
	metrics1 := []metrics.FlatValue{
		{Key: "metric1", Value: int64(1), Type: metrics.Int},
	}
	metrics2 := []metrics.FlatValue{
		{Key: "metric2", Value: int64(2), Type: metrics.Int},
	}

	event1 := events.NewMetricsEvent("collector1", metrics1)
	event2 := events.NewMetricsEvent("collector2", metrics2)

	// Process events
	err := sink.handleMetrics(ctx, event1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = sink.handleMetrics(ctx, event2)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Check buffer
	sink.bufferMu.Lock()
	bufferLen := len(sink.metricsBuffer)
	sink.bufferMu.Unlock()

	if bufferLen != 2 {
		t.Errorf("expected 2 metrics in buffer, got %d", bufferLen)
	}
}

func TestProcessEvent_MetricsEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	testMetrics := []metrics.FlatValue{
		{Key: "test", Value: int64(42), Type: metrics.Int},
	}
	event := events.NewMetricsEvent("test_collector", testMetrics)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify buffered
	sink.bufferMu.Lock()
	bufferLen := len(sink.metricsBuffer)
	sink.bufferMu.Unlock()

	if bufferLen != 1 {
		t.Errorf("expected 1 metric in buffer, got %d", bufferLen)
	}
}

func TestProcessEvent_HeartbeatEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	event := events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))

	// This will fail because we're not actually connecting to a server
	// but we can verify the method is called without panicking
	_ = sink.Process(ctx, event)
}

func TestProcessEvent_SystemInfoEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	info := []metrics.FlatValue{
		{Key: "cpu_count", Value: int64(4), Type: metrics.Int},
	}
	event := events.NewSystemInfoEvent(info)

	// This will fail because we're not actually connecting to a server
	// but we can verify the method is called without panicking
	_ = sink.Process(ctx, event)
}

func TestProcessEvent_ConfigEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	activeConfig := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "max_connections", Setting: "100"},
	}
	event := events.NewConfigEvent(activeConfig, nil)

	// This will fail because we're not actually connecting to a server
	// but we can verify the method is called without panicking
	_ = sink.Process(ctx, event)
}

func TestProcessEvent_GuardrailEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	signal := &guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	}
	event := events.NewGuardrailEvent(signal)

	// This will fail because we're not actually connecting to a server
	// but we can verify the method is called without panicking
	_ = sink.Process(ctx, event)
}

func TestProcessEvent_GuardrailEventNilSignal(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	event := events.NewGuardrailEvent(nil)

	// Should not error for nil signal
	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error for nil signal: %v", err)
	}
}

func TestProcessEvent_ErrorEvent(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	payload := agent.ErrorPayload{
		ErrorMessage: "test error",
		ErrorType:    "test",
		Timestamp:    time.Now().Format(time.RFC3339),
	}
	event := events.NewErrorEvent(payload)

	// This will fail because we're not actually connecting to a server
	// but we can verify the method is called without panicking
	_ = sink.Process(ctx, event)
}

func TestFlushMetrics_EmptyBuffer(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	// Flushing empty buffer should not error
	err := sink.FlushMetrics(ctx)
	if err != nil {
		t.Errorf("unexpected error flushing empty buffer: %v", err)
	}
}

func TestFlushMetrics_ClearsBuffer(t *testing.T) {
	sink := createTestSink()
	ctx := context.Background()

	// Add metrics to buffer
	testMetrics := []metrics.FlatValue{
		{Key: "test", Value: int64(42), Type: metrics.Int},
	}
	event := events.NewMetricsEvent("test_collector", testMetrics)
	sink.handleMetrics(ctx, event)

	// Verify buffer has data
	sink.bufferMu.Lock()
	bufferLen := len(sink.metricsBuffer)
	sink.bufferMu.Unlock()

	if bufferLen != 1 {
		t.Errorf("expected 1 metric in buffer before flush, got %d", bufferLen)
	}

	// Flush will fail due to no server, but buffer should still be cleared
	_ = sink.FlushMetrics(ctx)

	// Verify buffer is cleared
	sink.bufferMu.Lock()
	bufferLen = len(sink.metricsBuffer)
	sink.bufferMu.Unlock()

	if bufferLen != 0 {
		t.Errorf("expected buffer to be cleared after flush, got %d metrics", bufferLen)
	}
}

func TestClose(t *testing.T) {
	sink := createTestSink()

	// Add some metrics
	testMetrics := []metrics.FlatValue{
		{Key: "test", Value: int64(42), Type: metrics.Int},
	}
	event := events.NewMetricsEvent("test_collector", testMetrics)
	sink.handleMetrics(context.Background(), event)

	// Close should flush
	_ = sink.Close()

	// Buffer should be empty after close
	sink.bufferMu.Lock()
	bufferLen := len(sink.metricsBuffer)
	sink.bufferMu.Unlock()

	if bufferLen != 0 {
		t.Errorf("expected buffer to be empty after close, got %d metrics", bufferLen)
	}
}
