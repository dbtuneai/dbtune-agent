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

const (
	// DefaultBufferSize is the default size of the event channel buffer
	DefaultBufferSize = 1000
	// DefaultFlushInterval is the default interval for flushing buffered metrics
	DefaultFlushInterval = 5 * time.Second
)

// Router connects sources to multiple sinks
type Router struct {
	sources []source.SourceRunner
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
func New(sources []source.SourceRunner, sinks []sink.Sink, logger *log.Logger, config Config) *Router {
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

	for _, src := range r.sources {
		go func(s source.SourceRunner) {
			r.logger.Debugf("Starting source: %s (interval: %v)", s.Name, s.Interval)
			if err := s.Start(ctx, r.eventChan); err != nil && err != context.Canceled {
				r.errChan <- fmt.Errorf("source %s: %w", s.Name, err)
			}
		}(src)
	}

	flushTicker := time.NewTicker(r.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Println("Router shutting down")
			return r.shutdown()

		case event := <-r.eventChan:
			for _, snk := range r.sinks {
				if err := snk.Process(ctx, event); err != nil {
					r.logger.Debugf("Sink %s error processing %s event: %v", snk.Name(), event.Type(), err)
				}
			}

		case <-flushTicker.C:
			for _, snk := range r.sinks {
				if err := snk.FlushMetrics(ctx); err != nil {
					r.logger.Debugf("Sink %s flush error: %v", snk.Name(), err)
				}
			}

		case err := <-r.errChan:
			r.logger.Errorf("Source error: %v", err)
		}
	}
}

// shutdown closes all sinks
func (r *Router) shutdown() error {
	r.logger.Println("Closing all sinks")
	var firstErr error
	for _, snk := range r.sinks {
		if err := snk.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
