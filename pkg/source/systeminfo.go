package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// SystemInfoSource collects system information
type SystemInfoSource struct {
	*TickerSource
	adapter agent.AgentLooper
}

// NewSystemInfoSource creates a new system info source
func NewSystemInfoSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) *SystemInfoSource {
	return &SystemInfoSource{
		TickerSource: NewTickerSource(Config{
			Name:      "system_info",
			Interval:  interval,
			SkipFirst: false,
		}, logger),
		adapter: adapter,
	}
}

// Start begins producing system info events
func (s *SystemInfoSource) Start(ctx context.Context, out chan<- events.Event) error {
	return s.TickerSource.Start(ctx, out, s.collect)
}

// collect gets system info from adapter
func (s *SystemInfoSource) collect(ctx context.Context) (events.Event, error) {
	info, err := s.adapter.GetSystemInfo()
	if err != nil {
		return nil, err
	}
	return events.NewSystemInfoEvent(info), nil
}
