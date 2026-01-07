package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// HeartbeatSource sends periodic heartbeat events
type HeartbeatSource struct {
	SourceName     string
	SourceInterval time.Duration
	Version        string
	StartTime      string
	Logger         *log.Logger
	SkipFirst      bool
}

// NewHeartbeatSource creates a new heartbeat source
func NewHeartbeatSource(version string, startTime string, interval time.Duration, logger *log.Logger) *HeartbeatSource {
	return &HeartbeatSource{
		SourceName:     "heartbeat",
		SourceInterval: interval,
		Version:        version,
		StartTime:      startTime,
		Logger:         logger,
		SkipFirst:      true, // Skip first execution like in original runner
	}
}

// Name returns the source name
func (s *HeartbeatSource) Name() string {
	return s.SourceName
}

// Interval returns the source interval
func (s *HeartbeatSource) Interval() time.Duration {
	return s.SourceInterval
}

// Start begins producing heartbeat events
func (s *HeartbeatSource) Start(ctx context.Context, out chan<- events.Event) error {
	return RunWithTicker(ctx, out, s.SourceInterval, s.SkipFirst, s.Logger, s.SourceName, func(ctx context.Context) (events.Event, error) {
		return events.NewHeartbeatEvent(s.Version, s.StartTime), nil
	})
}
