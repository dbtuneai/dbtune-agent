package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// NewHeartbeatSource creates a new heartbeat source
func NewHeartbeatSource(version string, startTime string, interval time.Duration, logger *log.Logger) SourceRunner {
	return SourceRunner{
		Name:     "heartbeat",
		Interval: interval,
		Start: func(ctx context.Context, out chan<- events.Event) error {
			return RunWithTicker(ctx, out, TickerConfig{
				Name:      "heartbeat",
				Interval:  interval,
				SkipFirst: true,
				Logger:    logger,
				Collect: func(ctx context.Context) (events.Event, error) {
					return events.NewHeartbeatEvent(version, startTime), nil
				},
			})
		},
	}
}
