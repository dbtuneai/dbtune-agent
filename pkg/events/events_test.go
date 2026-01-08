package events

import (
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
)

func TestMetricsEvent(t *testing.T) {
	key := "test_collector"
	testMetrics := []metrics.FlatValue{
		{Key: "test_metric", Value: int64(42), Type: metrics.Int},
	}

	event := NewMetricsEvent(key, testMetrics)

	if event.Type() != EventTypeMetrics {
		t.Errorf("expected type %s, got %s", EventTypeMetrics, event.Type())
	}

	if event.CollectorKey != key {
		t.Errorf("expected collector key %s, got %s", key, event.CollectorKey)
	}

	if len(event.Metrics) != 1 {
		t.Errorf("expected 1 metric, got %d", len(event.Metrics))
	}

	if time.Since(event.Timestamp()) > time.Second {
		t.Errorf("timestamp too old: %v", event.Timestamp())
	}
}

func TestHeartbeatEvent(t *testing.T) {
	version := "1.0.0"
	startTime := time.Now().UTC().Format(time.RFC3339)

	event := NewHeartbeatEvent(version, startTime)

	if event.Type() != EventTypeHeartbeat {
		t.Errorf("expected type %s, got %s", EventTypeHeartbeat, event.Type())
	}

	if event.Version != version {
		t.Errorf("expected version %s, got %s", version, event.Version)
	}

	if event.StartTime != startTime {
		t.Errorf("expected start time %s, got %s", startTime, event.StartTime)
	}
}

func TestSystemInfoEvent(t *testing.T) {
	info := []metrics.FlatValue{
		{Key: "cpu_count", Value: int64(4), Type: metrics.Int},
		{Key: "memory_total", Value: int64(1024), Type: metrics.Bytes},
	}

	event := NewSystemInfoEvent(info)

	if event.Type() != EventTypeSystemInfo {
		t.Errorf("expected type %s, got %s", EventTypeSystemInfo, event.Type())
	}

	if len(event.Info) != 2 {
		t.Errorf("expected 2 info items, got %d", len(event.Info))
	}
}

func TestConfigEvent(t *testing.T) {
	activeConfig := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "max_connections", Setting: "100"},
	}
	proposedConfig := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "max_connections", Setting: "200"},
		},
	}

	event := NewConfigEvent(activeConfig, proposedConfig)

	if event.Type() != EventTypeConfig {
		t.Errorf("expected type %s, got %s", EventTypeConfig, event.Type())
	}

	if len(event.ActiveConfig) != 1 {
		t.Errorf("expected 1 active config item, got %d", len(event.ActiveConfig))
	}

	if event.ProposedConfig == nil {
		t.Error("expected proposed config to not be nil")
	}
}

func TestConfigEventWithNilProposed(t *testing.T) {
	activeConfig := agent.ConfigArraySchema{
		agent.PGConfigRow{Name: "max_connections", Setting: "100"},
	}

	event := NewConfigEvent(activeConfig, nil)

	if event.Type() != EventTypeConfig {
		t.Errorf("expected type %s, got %s", EventTypeConfig, event.Type())
	}

	if event.ProposedConfig != nil {
		t.Error("expected proposed config to be nil")
	}
}

func TestGuardrailEvent(t *testing.T) {
	signal := &guardrails.Signal{
		Level: guardrails.Critical,
		Type:  guardrails.Memory,
	}

	event := NewGuardrailEvent(signal)

	if event.Type() != EventTypeGuardrail {
		t.Errorf("expected type %s, got %s", EventTypeGuardrail, event.Type())
	}

	if event.Signal == nil {
		t.Error("expected signal to not be nil")
	}

	if event.Signal.Level != guardrails.Critical {
		t.Errorf("expected critical level, got %s", event.Signal.Level)
	}
}

func TestGuardrailEventWithNilSignal(t *testing.T) {
	event := NewGuardrailEvent(nil)

	if event.Type() != EventTypeGuardrail {
		t.Errorf("expected type %s, got %s", EventTypeGuardrail, event.Type())
	}

	if event.Signal != nil {
		t.Error("expected signal to be nil")
	}
}

func TestErrorEvent(t *testing.T) {
	payload := agent.ErrorPayload{
		ErrorMessage: "test error",
		ErrorType:    "test_type",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}

	event := NewErrorEvent(payload)

	if event.Type() != EventTypeError {
		t.Errorf("expected type %s, got %s", EventTypeError, event.Type())
	}

	if event.Payload.ErrorMessage != "test error" {
		t.Errorf("expected error message 'test error', got %s", event.Payload.ErrorMessage)
	}
}
