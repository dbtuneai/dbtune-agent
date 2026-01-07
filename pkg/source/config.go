package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// ConfigSource monitors and applies configuration changes
type ConfigSource struct {
	SourceName     string
	SourceInterval time.Duration
	Adapter        agent.AgentLooper
	Logger         *log.Logger
	SkipFirst      bool
}

// NewConfigSource creates a new config source
func NewConfigSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) *ConfigSource {
	return &ConfigSource{
		SourceName:     "config",
		SourceInterval: interval,
		Adapter:        adapter,
		Logger:         logger,
		SkipFirst:      false,
	}
}

// Name returns the source name
func (s *ConfigSource) Name() string {
	return s.SourceName
}

// Interval returns the source interval
func (s *ConfigSource) Interval() time.Duration {
	return s.SourceInterval
}

// Start begins producing config events
func (s *ConfigSource) Start(ctx context.Context, out chan<- events.Event) error {
	return RunWithTicker(ctx, out, s.SourceInterval, s.SkipFirst, s.Logger, s.SourceName, func(ctx context.Context) (events.Event, error) {
		activeConfig, err := s.Adapter.GetActiveConfig()
		if err != nil {
			return nil, err
		}

		proposedConfig, err := s.Adapter.GetProposedConfig()
		if err != nil {
			s.Logger.Debugf("Failed to get proposed config: %v", err)
		}

		if proposedConfig != nil && proposedConfig.Config != nil {
			if err := s.Adapter.ApplyConfig(proposedConfig); err != nil {
				s.Logger.Errorf("Failed to apply config: %v", err)
			}
		}

		return events.NewConfigEvent(activeConfig, proposedConfig), nil
	})
}
