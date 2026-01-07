package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// RunWithTicker is a pure function that runs a collect function on an interval
// It handles ticker setup, error events, and graceful shutdown
func RunWithTicker(
	ctx context.Context,
	out chan<- events.Event,
	interval time.Duration,
	skipFirst bool,
	logger *log.Logger,
	name string,
	collect func(context.Context) (events.Event, error),
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Execute immediately unless SkipFirst is set
	if !skipFirst {
		if err := executeAndSend(ctx, out, collect); err != nil {
			logger.Debugf("[%s] initial execution error: %v", name, err)
		}
	}

	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := executeAndSend(ctx, out, collect); err != nil {
				logger.Debugf("[%s] execution error: %v", name, err)
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
