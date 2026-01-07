package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

func createTestFileSink(t *testing.T) (*FileSink, string) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-sink-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	path := tmpFile.Name()
	tmpFile.Close()

	sink, err := NewFileSink(path, logger)
	if err != nil {
		t.Fatalf("Failed to create file sink: %v", err)
	}

	return sink, path
}

func TestNewFileSink(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	if sink.Name() != "file" {
		t.Errorf("expected name 'file', got %s", sink.Name())
	}

	if sink.file == nil {
		t.Error("expected file to be initialized")
	}

	if sink.writer == nil {
		t.Error("expected writer to be initialized")
	}
}

func TestFileSink_ProcessMetricsEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	testMetrics := []metrics.FlatValue{
		{Key: "test_metric", Value: int64(42), Type: metrics.Int},
	}
	event := events.NewMetricsEvent("test_collector", testMetrics)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Flush to ensure data is written
	sink.Flush()

	// Read the file and verify content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Parse JSON
	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "metrics" {
		t.Errorf("expected type 'metrics', got %v", eventData["type"])
	}

	if eventData["collector_key"] != "test_collector" {
		t.Errorf("expected collector_key 'test_collector', got %v", eventData["collector_key"])
	}
}

func TestFileSink_ProcessHeartbeatEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	event := events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	sink.Flush()

	// Verify file contains data
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "heartbeat" {
		t.Errorf("expected type 'heartbeat', got %v", eventData["type"])
	}

	if eventData["version"] != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %v", eventData["version"])
	}
}

func TestFileSink_ProcessSystemInfoEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	info := []metrics.FlatValue{
		{Key: "cpu_count", Value: int64(4), Type: metrics.Int},
	}
	event := events.NewSystemInfoEvent(info)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	sink.Flush()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "system_info" {
		t.Errorf("expected type 'system_info', got %v", eventData["type"])
	}
}

func TestFileSink_ProcessConfigEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	activeConfig := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "max_connections", Setting: "100"},
	}
	event := events.NewConfigEvent(activeConfig, nil)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	sink.Flush()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "config" {
		t.Errorf("expected type 'config', got %v", eventData["type"])
	}
}

func TestFileSink_ProcessGuardrailEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	signal := &guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	}
	event := events.NewGuardrailEvent(signal)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	sink.Flush()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "guardrail" {
		t.Errorf("expected type 'guardrail', got %v", eventData["type"])
	}
}

func TestFileSink_ProcessErrorEvent(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()
	payload := agent.ErrorPayload{
		ErrorMessage: "test error",
		ErrorType:    "test",
		Timestamp:    time.Now().Format(time.RFC3339),
	}
	event := events.NewErrorEvent(payload)

	err := sink.Process(ctx, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	sink.Flush()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var eventData map[string]interface{}
	if err := json.Unmarshal(data, &eventData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if eventData["type"] != "error" {
		t.Errorf("expected type 'error', got %v", eventData["type"])
	}
}

func TestFileSink_MultipleEvents(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)
	defer sink.Close()

	ctx := context.Background()

	// Write multiple events
	events := []events.Event{
		events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339)),
		events.NewMetricsEvent("test", []metrics.FlatValue{
			{Key: "test", Value: int64(1), Type: metrics.Int},
		}),
		events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339)),
	}

	for _, event := range events {
		if err := sink.Process(ctx, event); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	sink.Flush()

	// Read file and count lines
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		// Verify each line is valid JSON
		var eventData map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &eventData); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", lineCount, err)
		}
	}

	if lineCount != 3 {
		t.Errorf("expected 3 lines, got %d", lineCount)
	}
}

func TestFileSink_AppendMode(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-append-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	path := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(path)

	ctx := context.Background()

	// Write first event
	sink1, err := NewFileSink(path, logger)
	if err != nil {
		t.Fatalf("Failed to create sink1: %v", err)
	}
	event1 := events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
	sink1.Process(ctx, event1)
	sink1.Close()

	// Write second event (should append, not overwrite)
	sink2, err := NewFileSink(path, logger)
	if err != nil {
		t.Fatalf("Failed to create sink2: %v", err)
	}
	event2 := events.NewHeartbeatEvent("2.0.0", time.Now().Format(time.RFC3339))
	sink2.Process(ctx, event2)
	sink2.Close()

	// Verify file has both events
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if lineCount != 2 {
		t.Errorf("expected 2 lines (append mode), got %d", lineCount)
	}
}

func TestFileSink_Close(t *testing.T) {
	sink, path := createTestFileSink(t)
	defer os.Remove(path)

	ctx := context.Background()
	event := events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
	sink.Process(ctx, event)

	// Close should flush and close file
	if err := sink.Close(); err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	// Verify data was written
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected file to contain data after close")
	}
}
