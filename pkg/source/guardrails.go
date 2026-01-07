package source

import (
	"context"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// NewGuardrailsSource creates a new guardrails source
func NewGuardrailsSource(adapter agent.AgentLooper, checkInterval time.Duration, signalInterval time.Duration, logger *log.Logger) SourceRunner {
	// State for rate limiting
	var lastSignal time.Time
	var mu sync.Mutex

	return SourceRunner{
		Name:     "guardrails",
		Interval: checkInterval,
		Start: func(ctx context.Context, out chan<- events.Event) error {
			return RunWithTicker(ctx, out, TickerConfig{
				Name:      "guardrails",
				Interval:  checkInterval,
				SkipFirst: false,
				Logger:    logger,
				Collect: func(ctx context.Context) (events.Event, error) {
					signal := adapter.Guardrails()

					// Rate limiting: only emit signal if enough time has passed
					mu.Lock()
					shouldEmit := signal != nil && time.Since(lastSignal) >= signalInterval
					if shouldEmit {
						lastSignal = time.Now()
					}
					mu.Unlock()

					if shouldEmit {
						return events.NewGuardrailEvent(signal), nil
					}

					// Return nil event if no signal or rate-limited
					return nil, nil
				},
			})
		},
	}
}
