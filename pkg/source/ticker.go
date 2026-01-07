package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// TickerSource provides common ticker functionality for interval-based sources
type TickerSource struct {
	config Config
	logger *log.Logger
}

// NewTickerSource creates a new TickerSource
func NewTickerSource(config Config, logger *log.Logger) *TickerSource {
	return &TickerSource{
		config: config,
		logger: logger,
	}
}

// Name returns the source name
func (s *TickerSource) Name() string {
	return s.config.Name
}

// Interval returns the source interval
func (s *TickerSource) Interval() time.Duration {
	return s.config.Interval
}

// Start runs the produce function on an interval
// The produce function should return an event or nil (nil events are skipped)
func (s *TickerSource) Start(
	ctx context.Context,
	out chan<- events.Event,
	produce func(context.Context) (events.Event, error),
) error {
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()

	// Execute immediately unless SkipFirst is set
	if !s.config.SkipFirst {
		if err := s.executeAndSend(ctx, out, produce); err != nil {
			s.logger.Debugf("[%s] initial execution error: %v", s.Name(), err)
		}
	}

	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.executeAndSend(ctx, out, produce); err != nil {
				s.logger.Debugf("[%s] execution error: %v", s.Name(), err)
				// Continue running even on error
			}
		}
	}
}

// executeAndSend executes the produce function and sends the event
func (s *TickerSource) executeAndSend(
	ctx context.Context,
	out chan<- events.Event,
	produce func(context.Context) (events.Event, error),
) error {
	event, err := produce(ctx)
	if err != nil {
		// Send error event if production fails
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
