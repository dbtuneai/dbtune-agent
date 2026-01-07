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
	*TickerSource
	adapter agent.AgentLooper
}

// NewConfigSource creates a new config source
func NewConfigSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) *ConfigSource {
	return &ConfigSource{
		TickerSource: NewTickerSource(Config{
			Name:      "config",
			Interval:  interval,
			SkipFirst: false,
		}, logger),
		adapter: adapter,
	}
}

// Start begins producing config events
func (s *ConfigSource) Start(ctx context.Context, out chan<- events.Event) error {
	return s.TickerSource.Start(ctx, out, s.collect)
}

// collect gets active config and checks for proposed config
func (s *ConfigSource) collect(ctx context.Context) (events.Event, error) {
	// Get active config
	activeConfig, err := s.adapter.GetActiveConfig()
	if err != nil {
		return nil, err
	}

	// Get proposed config
	proposedConfig, err := s.adapter.GetProposedConfig()
	if err != nil {
		// Log error but don't fail - we can still send active config
		s.TickerSource.logger.Debugf("Failed to get proposed config: %v", err)
	}

	// Apply if available
	if proposedConfig != nil && proposedConfig.Config != nil {
		if err := s.adapter.ApplyConfig(proposedConfig); err != nil {
			// Log error but don't fail the event
			s.TickerSource.logger.Errorf("Failed to apply config: %v", err)
		}
	}

	return events.NewConfigEvent(activeConfig, proposedConfig), nil
}
