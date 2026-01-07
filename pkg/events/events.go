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

// BaseEvent provides common event fields
type BaseEvent struct {
	EventTimestamp time.Time
	EventType      EventType
}

func (e BaseEvent) Timestamp() time.Time {
	return e.EventTimestamp
}

func (e BaseEvent) Type() EventType {
	return e.EventType
}

// MetricsEvent carries metrics from a single collector
type MetricsEvent struct {
	BaseEvent
	CollectorKey string
	Metrics      []metrics.FlatValue
}

func NewMetricsEvent(collectorKey string, m []metrics.FlatValue) MetricsEvent {
	return MetricsEvent{
		BaseEvent:    BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeMetrics},
		CollectorKey: collectorKey,
		Metrics:      m,
	}
}

// HeartbeatEvent for agent heartbeat
type HeartbeatEvent struct {
	BaseEvent
	Version   string
	StartTime string
}

func NewHeartbeatEvent(version string, startTime string) HeartbeatEvent {
	return HeartbeatEvent{
		BaseEvent: BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeHeartbeat},
		Version:   version,
		StartTime: startTime,
	}
}

// SystemInfoEvent for system information
type SystemInfoEvent struct {
	BaseEvent
	Info []metrics.FlatValue
}

func NewSystemInfoEvent(info []metrics.FlatValue) SystemInfoEvent {
	return SystemInfoEvent{
		BaseEvent: BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeSystemInfo},
		Info:      info,
	}
}

// ConfigEvent for configuration updates
type ConfigEvent struct {
	BaseEvent
	ActiveConfig   agent.ConfigArraySchema
	ProposedConfig *agent.ProposedConfigResponse
}

func NewConfigEvent(activeConfig agent.ConfigArraySchema, proposedConfig *agent.ProposedConfigResponse) ConfigEvent {
	return ConfigEvent{
		BaseEvent:      BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeConfig},
		ActiveConfig:   activeConfig,
		ProposedConfig: proposedConfig,
	}
}

// GuardrailEvent for guardrail signals
type GuardrailEvent struct {
	BaseEvent
	Signal *guardrails.Signal // nil if no signal triggered
}

func NewGuardrailEvent(signal *guardrails.Signal) GuardrailEvent {
	return GuardrailEvent{
		BaseEvent: BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeGuardrail},
		Signal:    signal,
	}
}

// ErrorEvent for error reporting
type ErrorEvent struct {
	BaseEvent
	Payload agent.ErrorPayload
}

func NewErrorEvent(payload agent.ErrorPayload) ErrorEvent {
	return ErrorEvent{
		BaseEvent: BaseEvent{EventTimestamp: time.Now(), EventType: EventTypeError},
		Payload:   payload,
	}
}
