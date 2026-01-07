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
	SourceName       string
	SourceInterval   time.Duration
	Collector        func(ctx context.Context, state *agent.MetricsState) error
	State            *agent.MetricsState
	Key              string
	Logger           *log.Logger
	SkipFirst        bool
	CollectorTimeout time.Duration
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
		SourceName:       key,
		SourceInterval:   interval,
		Collector:        collector,
		State:            state,
		Key:              key,
		Logger:           logger,
		SkipFirst:        false,
		CollectorTimeout: 10 * time.Second,
	}
}

// Name returns the source name
func (s *CollectorSource) Name() string {
	return s.SourceName
}

// Interval returns the source interval
func (s *CollectorSource) Interval() time.Duration {
	return s.SourceInterval
}

// Start begins producing metrics events
func (s *CollectorSource) Start(ctx context.Context, out chan<- events.Event) error {
	return RunWithTicker(ctx, out, s.SourceInterval, s.SkipFirst, s.Logger, s.SourceName, func(ctx context.Context) (events.Event, error) {
		// Create timeout context for this collection
		collectCtx, cancel := context.WithTimeout(ctx, s.CollectorTimeout)
		defer cancel()

		// Clear metrics for this collection
		// We need to be careful here - we'll temporarily set metrics to a new slice,
		// run the collector, then capture what it added
		s.State.Mutex.Lock()
		collectedMetrics := make([]metrics.FlatValue, 0)
		// Save old metrics (in case there are any from other collectors running concurrently)
		oldMetrics := s.State.Metrics
		s.State.Metrics = collectedMetrics
		s.State.Mutex.Unlock()

		// Run the collector
		err := s.Collector(collectCtx, s.State)

		// Restore and capture metrics
		s.State.Mutex.Lock()
		collectedMetrics = s.State.Metrics
		s.State.Metrics = oldMetrics
		s.State.Mutex.Unlock()

		if err != nil {
			return nil, err
		}

		// Return metrics event
		return events.NewMetricsEvent(s.Key, collectedMetrics), nil
	})
}
