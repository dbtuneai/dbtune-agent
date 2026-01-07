package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultCollectorTimeout is the maximum time allowed for a single collector execution
	DefaultCollectorTimeout = 10 * time.Second
)

// NewCollectorSource creates a new source from a MetricCollector
func NewCollectorSource(
	key string,
	interval time.Duration,
	collector func(ctx context.Context, state *agent.MetricsState) error,
	state *agent.MetricsState,
	logger *log.Logger,
) SourceRunner {

	return SourceRunner{
		Name:     key,
		Interval: interval,
		Start: func(ctx context.Context, out chan<- events.Event) error {
			return RunWithTicker(ctx, out, TickerConfig{
				Name:      key,
				Interval:  interval,
				SkipFirst: false,
				Logger:    logger,
				Collect: func(ctx context.Context) (events.Event, error) {
					// Create timeout context for this collection
					collectCtx, cancel := context.WithTimeout(ctx, DefaultCollectorTimeout)
					defer cancel()

					// Clear metrics for this collection
					// We need to be careful here - we'll temporarily set metrics to a new slice,
					// run the collector, then capture what it added
					state.Mutex.Lock()
					collectedMetrics := make([]metrics.FlatValue, 0)
					// Save old metrics (in case there are any from other collectors running concurrently)
					oldMetrics := state.Metrics
					state.Metrics = collectedMetrics
					state.Mutex.Unlock()

					// Run the collector
					err := collector(collectCtx, state)

					// Restore and capture metrics
					state.Mutex.Lock()
					collectedMetrics = state.Metrics
					state.Metrics = oldMetrics
					state.Mutex.Unlock()

					if err != nil {
						return nil, err
					}

					// Return metrics event
					return events.NewMetricsEvent(key, collectedMetrics), nil
				},
			})
		},
	}
}
