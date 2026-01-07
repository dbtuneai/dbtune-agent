package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// NewConfigSource creates a new config source
func NewConfigSource(adapter agent.AgentLooper, interval time.Duration, logger *log.Logger) SourceRunner {
	return SourceRunner{
		Name:     "config",
		Interval: interval,
		Start: func(ctx context.Context, out chan<- events.Event) error {
			return RunWithTicker(ctx, out, interval, false, logger, "config", func(ctx context.Context) (events.Event, error) {
				activeConfig, err := adapter.GetActiveConfig()
				if err != nil {
					return nil, err
				}

				proposedConfig, err := adapter.GetProposedConfig()
				if err != nil {
					logger.Debugf("Failed to get proposed config: %v", err)
				}

				if proposedConfig != nil && proposedConfig.Config != nil {
					if err := adapter.ApplyConfig(proposedConfig); err != nil {
						logger.Errorf("Failed to apply config: %v", err)
					}
				}

				return events.NewConfigEvent(activeConfig, proposedConfig), nil
			})
		},
	}
}
