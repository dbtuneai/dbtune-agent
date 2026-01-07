package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/sink"
	"github.com/dbtuneai/agent/pkg/source"
	log "github.com/sirupsen/logrus"
)

// MockSource for testing
type MockSource struct {
	name      string
	interval  time.Duration
	eventFunc func() events.Event
}

func (m *MockSource) Name() string {
	return m.name
}

func (m *MockSource) Interval() time.Duration {
	return m.interval
}

func (m *MockSource) Start(ctx context.Context, out chan<- events.Event) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	// Send one event immediately
	if m.eventFunc != nil {
		event := m.eventFunc()
		select {
		case out <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Then wait for ticker or context
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if m.eventFunc != nil {
				event := m.eventFunc()
				select {
				case out <- event:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// MockSink for testing
type MockSink struct {
	name           string
	processedCount int
	flushedCount   int
	mu             sync.Mutex
	events         []events.Event
}

func (m *MockSink) Name() string {
	return m.name
}

func (m *MockSink) Process(ctx context.Context, event events.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processedCount++
	m.events = append(m.events, event)
	return nil
}

func (m *MockSink) FlushMetrics(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushedCount++
	return nil
}

func (m *MockSink) Close() error {
	return nil
}

func (m *MockSink) GetProcessedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processedCount
}

func (m *MockSink) GetFlushedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushedCount
}

func (m *MockSink) GetEvents() []events.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events
}

func TestNewRouter(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	src := &MockSource{
		name:     "test-source",
		interval: 100 * time.Millisecond,
	}
	snk := &MockSink{name: "test-sink"}

	sources := []source.Source{src}
	sinks := []sink.Sink{snk}
	config := Config{
		BufferSize:    100,
		FlushInterval: 5 * time.Second,
	}
	router := New(sources, sinks, logger, config)

	if router == nil {
		t.Fatal("expected router to be created")
	}

	if len(router.sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(router.sources))
	}

	if len(router.sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(router.sinks))
	}
}

func TestRouter_BasicOperation(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	src := &MockSource{
		name:     "test-source",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
		},
	}
	snk := &MockSink{name: "test-sink"}

	sources := []source.Source{src}
	sinks := []sink.Sink{snk}
	config := Config{BufferSize: 1000, FlushInterval: 5 * time.Second}
	router := New(sources, sinks, logger, config)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := router.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}

	// Give it a moment to process
	time.Sleep(50 * time.Millisecond)

	processedCount := snk.GetProcessedCount()
	if processedCount < 3 {
		t.Errorf("expected at least 3 events processed, got %d", processedCount)
	}
}

func TestRouter_MultipleSources(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	source1 := &MockSource{
		name:     "source1",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
		},
	}

	source2 := &MockSource{
		name:     "source2",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			testMetrics := []metrics.FlatValue{
				{Key: "test", Value: int64(42), Type: metrics.Int},
			}
			return events.NewMetricsEvent("test_collector", testMetrics)
		},
	}

	snk := &MockSink{name: "test-sink"}

	sources := []source.Source{source1, source2}
	sinks := []sink.Sink{snk}
	config := Config{BufferSize: 1000, FlushInterval: 5 * time.Second}
	router := New(sources, sinks, logger, config)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := router.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}

	// Give it a moment to process
	time.Sleep(50 * time.Millisecond)

	processedCount := snk.GetProcessedCount()
	// Both sources send immediately + periodic, so should have at least 6 events (3 from each)
	if processedCount < 6 {
		t.Errorf("expected at least 6 events from 2 sources, got %d", processedCount)
	}

	// Verify we have both types of events
	evts := snk.GetEvents()
	hasHeartbeat := false
	hasMetrics := false
	for _, e := range evts {
		if e.Type() == events.EventTypeHeartbeat {
			hasHeartbeat = true
		}
		if e.Type() == events.EventTypeMetrics {
			hasMetrics = true
		}
	}

	if !hasHeartbeat {
		t.Error("expected at least one heartbeat event")
	}
	if !hasMetrics {
		t.Error("expected at least one metrics event")
	}
}

func TestRouter_FlushInterval(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	src := &MockSource{
		name:     "test-source",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
		},
	}
	snk := &MockSink{name: "test-sink"}

	config := Config{
		BufferSize:    100,
		FlushInterval: 100 * time.Millisecond,
	}
	sources := []source.Source{src}
	sinks := []sink.Sink{snk}
	router := New(sources, sinks, logger, config)

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	err := router.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}

	// Give it a moment to process
	time.Sleep(50 * time.Millisecond)

	flushedCount := snk.GetFlushedCount()
	// Should flush at 100ms, 200ms, 300ms
	if flushedCount < 3 {
		t.Errorf("expected at least 3 flushes, got %d", flushedCount)
	}
}

func TestRouter_EventTypes(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	// Create a source that emits different event types
	eventTypes := []events.EventType{
		events.EventTypeHeartbeat,
		events.EventTypeMetrics,
		events.EventTypeSystemInfo,
	}
	currentType := 0

	src := &MockSource{
		name:     "test-source",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			var event events.Event
			switch eventTypes[currentType%len(eventTypes)] {
			case events.EventTypeHeartbeat:
				event = events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
			case events.EventTypeMetrics:
				testMetrics := []metrics.FlatValue{
					{Key: "test", Value: int64(42), Type: metrics.Int},
				}
				event = events.NewMetricsEvent("test", testMetrics)
			case events.EventTypeSystemInfo:
				info := []metrics.FlatValue{
					{Key: "cpu", Value: int64(4), Type: metrics.Int},
				}
				event = events.NewSystemInfoEvent(info)
			}
			currentType++
			return event
		},
	}

	snk := &MockSink{name: "test-sink"}
	sources := []source.Source{src}
	sinks := []sink.Sink{snk}
	config := Config{BufferSize: 1000, FlushInterval: 5 * time.Second}
	router := New(sources, sinks, logger, config)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	router.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	evts := snk.GetEvents()
	if len(evts) < 3 {
		t.Errorf("expected at least 3 events, got %d", len(evts))
	}

	// Verify different event types
	typesFound := make(map[events.EventType]bool)
	for _, e := range evts {
		typesFound[e.Type()] = true
	}

	if len(typesFound) < 2 {
		t.Error("expected multiple event types")
	}
}

func TestRouter_MultipleSinks(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	src := &MockSource{
		name:     "test-source",
		interval: 50 * time.Millisecond,
		eventFunc: func() events.Event {
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339))
		},
	}

	sink1 := &MockSink{name: "sink1"}
	sink2 := &MockSink{name: "sink2"}

	sources := []source.Source{src}
	sinks := []sink.Sink{sink1, sink2}
	config := Config{BufferSize: 1000, FlushInterval: 5 * time.Second}
	router := New(sources, sinks, logger, config)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := router.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}

	// Give it a moment to process
	time.Sleep(50 * time.Millisecond)

	// Both sinks should receive the same events
	count1 := sink1.GetProcessedCount()
	count2 := sink2.GetProcessedCount()

	if count1 < 3 {
		t.Errorf("sink1 expected at least 3 events, got %d", count1)
	}

	if count2 < 3 {
		t.Errorf("sink2 expected at least 3 events, got %d", count2)
	}

	// Both should have approximately the same count
	if count1 != count2 {
		t.Errorf("expected both sinks to receive same number of events, got %d and %d", count1, count2)
	}
}
