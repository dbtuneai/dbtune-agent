package router

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/sink"
	"github.com/dbtuneai/agent/pkg/source"
	log "github.com/sirupsen/logrus"
)

// Router connects sources to multiple sinks
type Router struct {
	sources []source.Source
	sinks   []sink.Sink

	eventChan chan events.Event
	errChan   chan error

	bufferSize    int
	flushInterval time.Duration
	logger        *log.Logger
}

// Config holds router configuration
type Config struct {
	BufferSize    int
	FlushInterval time.Duration
}

// New creates a new router
func New(sources []source.Source, sinks []sink.Sink, logger *log.Logger, config Config) *Router {
	return &Router{
		sources:       sources,
		sinks:         sinks,
		eventChan:     make(chan events.Event, config.BufferSize),
		errChan:       make(chan error, len(sources)),
		bufferSize:    config.BufferSize,
		flushInterval: config.FlushInterval,
		logger:        logger,
	}
}

// Run starts the router and all sources
func (r *Router) Run(ctx context.Context) error {
	r.logger.Infof("Starting router with %d sources and %d sinks", len(r.sources), len(r.sinks))

	// Start all sources
	for _, src := range r.sources {
		go func(s source.Source) {
			r.logger.Debugf("Starting source: %s (interval: %v)", s.Name(), s.Interval())
			if err := s.Start(ctx, r.eventChan); err != nil && err != context.Canceled {
				r.errChan <- fmt.Errorf("source %s: %w", s.Name(), err)
			}
		}(src)
	}

	// Start metrics flush ticker (for aggregated metrics)
	flushTicker := time.NewTicker(r.flushInterval)
	defer flushTicker.Stop()

	// Event processing loop
	for {
		select {
		case <-ctx.Done():
			r.logger.Println("Router shutting down")
			return r.shutdown()

		case event := <-r.eventChan:
			// Process event through all sinks
			for _, snk := range r.sinks {
				if err := snk.Process(ctx, event); err != nil {
					r.logger.Debugf("Sink %s error processing %s event: %v", snk.Name(), event.Type(), err)
				}
			}

		case <-flushTicker.C:
			// Flush aggregated metrics for all flushable sinks
			for _, snk := range r.sinks {
				if flusher, ok := snk.(sink.Flusher); ok {
					if err := flusher.FlushMetrics(ctx); err != nil {
						r.logger.Debugf("Sink %s flush error: %v", snk.Name(), err)
					}
				}
			}

		case err := <-r.errChan:
			r.logger.Errorf("Source error: %v", err)
			// Continue running even if a source fails
		}
	}
}

// shutdown closes all sinks
func (r *Router) shutdown() error {
	r.logger.Println("Closing all sinks")
	var firstErr error
	for _, snk := range r.sinks {
		if closer, ok := snk.(sink.Closer); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
