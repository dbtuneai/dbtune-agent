package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// HeartbeatSource sends periodic heartbeat events
type HeartbeatSource struct {
	*TickerSource
	version   string
	startTime string
}

// NewHeartbeatSource creates a new heartbeat source
func NewHeartbeatSource(version string, startTime string, interval time.Duration, logger *log.Logger) *HeartbeatSource {
	return &HeartbeatSource{
		TickerSource: NewTickerSource(Config{
			Name:      "heartbeat",
			Interval:  interval,
			SkipFirst: true, // Skip first execution like in original runner
		}, logger),
		version:   version,
		startTime: startTime,
	}
}

// Start begins producing heartbeat events
func (s *HeartbeatSource) Start(ctx context.Context, out chan<- events.Event) error {
	return s.TickerSource.Start(ctx, out, s.produce)
}

// produce creates a heartbeat event
func (s *HeartbeatSource) produce(ctx context.Context) (events.Event, error) {
	return events.NewHeartbeatEvent(s.version, s.startTime), nil
}
