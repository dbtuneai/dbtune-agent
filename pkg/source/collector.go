package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

// NewCollectorSource creates a new source from a MetricCollector
func NewCollectorSource(
	key string,
	interval time.Duration,
	timeout time.Duration,
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
					collectCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()

					// Temporarily swap metrics slice to isolate this collector's output
					state.Mutex.Lock()
					collectedMetrics := make([]metrics.FlatValue, 0)
					oldMetrics := state.Metrics
					state.Metrics = collectedMetrics
					state.Mutex.Unlock()

					err := collector(collectCtx, state)

					state.Mutex.Lock()
					collectedMetrics = state.Metrics
					state.Metrics = oldMetrics
					state.Mutex.Unlock()

					if err != nil {
						return nil, err
					}

					return events.NewMetricsEvent(key, collectedMetrics), nil
				},
			})
		},
	}
}
