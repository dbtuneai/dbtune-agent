package events

import (
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
)

// Event is the base interface for all events
type Event interface {
	Type() EventType
	Timestamp() time.Time
}

// EventType represents the type of event
type EventType string

const (
	EventTypeMetrics    EventType = "metrics"
	EventTypeHeartbeat  EventType = "heartbeat"
	EventTypeSystemInfo EventType = "system_info"
	EventTypeConfig     EventType = "config"
	EventTypeGuardrail  EventType = "guardrail"
	EventTypeError      EventType = "error"
)

// MetricsEvent carries metrics from a single collector
type MetricsEvent struct {
	timestamp    time.Time
	collectorKey string
	metrics      []metrics.FlatValue
}

// NewMetricsEvent creates a new MetricsEvent
func NewMetricsEvent(collectorKey string, m []metrics.FlatValue) MetricsEvent {
	return MetricsEvent{
		timestamp:    time.Now(),
		collectorKey: collectorKey,
		metrics:      m,
	}
}

func (e MetricsEvent) Type() EventType {
	return EventTypeMetrics
}

func (e MetricsEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e MetricsEvent) CollectorKey() string {
	return e.collectorKey
}

func (e MetricsEvent) Metrics() []metrics.FlatValue {
	return e.metrics
}

// HeartbeatEvent for agent heartbeat
type HeartbeatEvent struct {
	timestamp time.Time
	version   string
	startTime string
}

// NewHeartbeatEvent creates a new HeartbeatEvent
func NewHeartbeatEvent(version string, startTime string) HeartbeatEvent {
	return HeartbeatEvent{
		timestamp: time.Now(),
		version:   version,
		startTime: startTime,
	}
}

func (e HeartbeatEvent) Type() EventType {
	return EventTypeHeartbeat
}

func (e HeartbeatEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e HeartbeatEvent) Version() string {
	return e.version
}

func (e HeartbeatEvent) StartTime() string {
	return e.startTime
}

// SystemInfoEvent for system information
type SystemInfoEvent struct {
	timestamp time.Time
	info      []metrics.FlatValue
}

// NewSystemInfoEvent creates a new SystemInfoEvent
func NewSystemInfoEvent(info []metrics.FlatValue) SystemInfoEvent {
	return SystemInfoEvent{
		timestamp: time.Now(),
		info:      info,
	}
}

func (e SystemInfoEvent) Type() EventType {
	return EventTypeSystemInfo
}

func (e SystemInfoEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e SystemInfoEvent) Info() []metrics.FlatValue {
	return e.info
}

// ConfigEvent for configuration updates
type ConfigEvent struct {
	timestamp      time.Time
	activeConfig   agent.ConfigArraySchema
	proposedConfig *agent.ProposedConfigResponse
}

// NewConfigEvent creates a new ConfigEvent
func NewConfigEvent(activeConfig agent.ConfigArraySchema, proposedConfig *agent.ProposedConfigResponse) ConfigEvent {
	return ConfigEvent{
		timestamp:      time.Now(),
		activeConfig:   activeConfig,
		proposedConfig: proposedConfig,
	}
}

func (e ConfigEvent) Type() EventType {
	return EventTypeConfig
}

func (e ConfigEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e ConfigEvent) ActiveConfig() agent.ConfigArraySchema {
	return e.activeConfig
}

func (e ConfigEvent) ProposedConfig() *agent.ProposedConfigResponse {
	return e.proposedConfig
}

// GuardrailEvent for guardrail signals
type GuardrailEvent struct {
	timestamp time.Time
	signal    *guardrails.Signal // nil if no signal triggered
}

// NewGuardrailEvent creates a new GuardrailEvent
func NewGuardrailEvent(signal *guardrails.Signal) GuardrailEvent {
	return GuardrailEvent{
		timestamp: time.Now(),
		signal:    signal,
	}
}

func (e GuardrailEvent) Type() EventType {
	return EventTypeGuardrail
}

func (e GuardrailEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e GuardrailEvent) Signal() *guardrails.Signal {
	return e.signal
}

// ErrorEvent for error reporting
type ErrorEvent struct {
	timestamp time.Time
	payload   agent.ErrorPayload
}

// NewErrorEvent creates a new ErrorEvent
func NewErrorEvent(payload agent.ErrorPayload) ErrorEvent {
	return ErrorEvent{
		timestamp: time.Now(),
		payload:   payload,
	}
}

func (e ErrorEvent) Type() EventType {
	return EventTypeError
}

func (e ErrorEvent) Timestamp() time.Time {
	return e.timestamp
}

func (e ErrorEvent) Payload() agent.ErrorPayload {
	return e.payload
}
