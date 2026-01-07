package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// TickerConfig holds configuration for RunWithTicker
type TickerConfig struct {
	Name      string
	Interval  time.Duration
	SkipFirst bool
	Logger    *log.Logger
	Collect   func(context.Context) (events.Event, error)
}

// RunWithTicker is a pure function that runs a collect function on an interval
// It handles ticker setup, error events, and graceful shutdown
func RunWithTicker(ctx context.Context, out chan<- events.Event, config TickerConfig) error {
	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	// Execute immediately unless SkipFirst is set
	if !config.SkipFirst {
		if err := executeAndSend(ctx, out, config.Collect); err != nil {
			config.Logger.Debugf("[%s] initial execution error: %v", config.Name, err)
		}
	}

	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := executeAndSend(ctx, out, config.Collect); err != nil {
				config.Logger.Debugf("[%s] execution error: %v", config.Name, err)
				// Continue running even on error
			}
		}
	}
}

// executeAndSend executes the collect function and sends the event
func executeAndSend(
	ctx context.Context,
	out chan<- events.Event,
	collect func(context.Context) (events.Event, error),
) error {
	event, err := collect(ctx)
	if err != nil {
		// Send error event if collection fails
		errorPayload := events.NewErrorEvent(agent.ErrorPayload{
			ErrorMessage: err.Error(),
			ErrorType:    "source_error",
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
		})
		select {
		case out <- errorPayload:
		case <-ctx.Done():
			return ctx.Err()
		}
		return err
	}

	// Send the produced event (if not nil)
	if event != nil {
		select {
		case out <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
