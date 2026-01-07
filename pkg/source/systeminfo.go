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
	SourceName     string
	SourceInterval time.Duration
	Adapter        agent.AgentLooper
	Logger         *log.Logger
	SkipFirst      bool
}

// NewSystemInfoSource creates a new system info source
func NewSystemInfoSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) *SystemInfoSource {
	return &SystemInfoSource{
		SourceName:     "system_info",
		SourceInterval: interval,
		Adapter:        adapter,
		Logger:         logger,
		SkipFirst:      false,
	}
}

// Name returns the source name
func (s *SystemInfoSource) Name() string {
	return s.SourceName
}

// Interval returns the source interval
func (s *SystemInfoSource) Interval() time.Duration {
	return s.SourceInterval
}

// Start begins producing system info events
func (s *SystemInfoSource) Start(ctx context.Context, out chan<- events.Event) error {
	return RunWithTicker(ctx, out, s.SourceInterval, s.SkipFirst, s.Logger, s.SourceName, func(ctx context.Context) (events.Event, error) {
		info, err := s.Adapter.GetSystemInfo()
		if err != nil {
			return nil, err
		}
		return events.NewSystemInfoEvent(info), nil
	})
}
