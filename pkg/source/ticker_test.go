package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

func TestRunWithTicker_BasicOperation(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	eventChan := make(chan events.Event, 10)
	callCount := 0

	config := TickerConfig{
		Name:      "test-source",
		Interval:  100 * time.Millisecond,
		SkipFirst: false,
		Logger:    logger,
		Collect: func(ctx context.Context) (events.Event, error) {
			callCount++
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339)), nil
		},
	}

	err := RunWithTicker(ctx, eventChan, config)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}

	// Should run immediately + at least 3 times from ticker (0, 100, 200, 300ms)
	if callCount < 4 {
		t.Errorf("expected at least 4 calls, got %d", callCount)
	}

	close(eventChan)

	// Verify events were sent
	eventCount := 0
	for range eventChan {
		eventCount++
	}

	if eventCount != callCount {
		t.Errorf("expected %d events, got %d", callCount, eventCount)
	}
}

func TestRunWithTicker_SkipFirst(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	eventChan := make(chan events.Event, 10)
	callCount := 0

	config := TickerConfig{
		Name:      "test-source",
		Interval:  100 * time.Millisecond,
		SkipFirst: true,
		Logger:    logger,
		Collect: func(ctx context.Context) (events.Event, error) {
			callCount++
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339)), nil
		},
	}

	_ = RunWithTicker(ctx, eventChan, config)

	// Should skip first and run at 100, 200ms
	if callCount < 2 || callCount > 3 {
		t.Errorf("expected 2-3 calls with SkipFirst, got %d", callCount)
	}
}

func TestRunWithTicker_ErrorHandling(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	eventChan := make(chan events.Event, 10)
	callCount := 0

	config := TickerConfig{
		Name:      "test-source",
		Interval:  100 * time.Millisecond,
		SkipFirst: false,
		Logger:    logger,
		Collect: func(ctx context.Context) (events.Event, error) {
			callCount++
			return nil, errors.New("test error")
		},
	}

	_ = RunWithTicker(ctx, eventChan, config)
	close(eventChan)

	// Should still be called despite errors
	if callCount < 2 {
		t.Errorf("expected at least 2 calls despite errors, got %d", callCount)
	}

	// Should have error events
	errorEventCount := 0
	for event := range eventChan {
		if event.Type() == events.EventTypeError {
			errorEventCount++
		}
	}

	if errorEventCount != callCount {
		t.Errorf("expected %d error events, got %d", callCount, errorEventCount)
	}
}

func TestRunWithTicker_NilEvents(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	eventChan := make(chan events.Event, 10)
	callCount := 0

	config := TickerConfig{
		Name:      "test-source",
		Interval:  100 * time.Millisecond,
		SkipFirst: false,
		Logger:    logger,
		Collect: func(ctx context.Context) (events.Event, error) {
			callCount++
			return nil, nil // Nil event, no error
		},
	}

	_ = RunWithTicker(ctx, eventChan, config)
	close(eventChan)

	// Should be called
	if callCount < 2 {
		t.Errorf("expected at least 2 calls, got %d", callCount)
	}

	// But no events should be sent
	eventCount := 0
	for range eventChan {
		eventCount++
	}

	if eventCount != 0 {
		t.Errorf("expected 0 events for nil returns, got %d", eventCount)
	}
}

func TestRunWithTicker_ContextCancellation(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	eventChan := make(chan events.Event, 10)

	// Cancel immediately after first execution
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	config := TickerConfig{
		Name:      "test-source",
		Interval:  100 * time.Millisecond,
		SkipFirst: false,
		Logger:    logger,
		Collect: func(ctx context.Context) (events.Event, error) {
			return events.NewHeartbeatEvent("1.0.0", time.Now().Format(time.RFC3339)), nil
		},
	}

	err := RunWithTicker(ctx, eventChan, config)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
