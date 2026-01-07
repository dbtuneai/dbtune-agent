package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

// CollectorSource wraps a MetricCollector and runs it as a source
type CollectorSource struct {
	*TickerSource
	collector      func(ctx context.Context, state *agent.MetricsState) error
	state          *agent.MetricsState
	key            string
	collectorTimeout time.Duration
}

// NewCollectorSource creates a new source from a MetricCollector
func NewCollectorSource(
	key string,
	interval time.Duration,
	collector func(ctx context.Context, state *agent.MetricsState) error,
	state *agent.MetricsState,
	logger *log.Logger,
) *CollectorSource {
	return &CollectorSource{
		TickerSource: NewTickerSource(Config{
			Name:      key,
			Interval:  interval,
			SkipFirst: false,
		}, logger),
		collector:        collector,
		state:            state,
		key:              key,
		collectorTimeout: 10 * time.Second,
	}
}

// Start begins producing metrics events
func (s *CollectorSource) Start(ctx context.Context, out chan<- events.Event) error {
	return s.TickerSource.Start(ctx, out, s.collect)
}

// collect runs the collector and returns a MetricsEvent
func (s *CollectorSource) collect(ctx context.Context) (events.Event, error) {
	// Create timeout context for this collection
	collectCtx, cancel := context.WithTimeout(ctx, s.collectorTimeout)
	defer cancel()

	// Clear metrics for this collection
	// We need to be careful here - we'll temporarily set metrics to a new slice,
	// run the collector, then capture what it added
	s.state.Mutex.Lock()
	collectedMetrics := make([]metrics.FlatValue, 0)
	// Save old metrics (in case there are any from other collectors running concurrently)
	oldMetrics := s.state.Metrics
	s.state.Metrics = collectedMetrics
	s.state.Mutex.Unlock()

	// Run the collector
	err := s.collector(collectCtx, s.state)

	// Restore and capture metrics
	s.state.Mutex.Lock()
	collectedMetrics = s.state.Metrics
	s.state.Metrics = oldMetrics
	s.state.Mutex.Unlock()

	if err != nil {
		return nil, err
	}

	// Return metrics event
	return events.NewMetricsEvent(s.key, collectedMetrics), nil
}
