package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// NewSystemInfoSource creates a new system info source
func NewSystemInfoSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) SourceRunner {
	return SourceRunner{
		Name:     "system_info",
		Interval: interval,
		Start: func(ctx context.Context, out chan<- events.Event) error {
			return RunWithTicker(ctx, out, TickerConfig{
				Name:      "system_info",
				Interval:  interval,
				SkipFirst: false,
				Logger:    logger,
				Collect: func(ctx context.Context) (events.Event, error) {
					info, err := adapter.GetSystemInfo()
					if err != nil {
						return nil, err
					}
					return events.NewSystemInfoEvent(info), nil
				},
			})
		},
	}
}
