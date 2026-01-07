package source

import (
	"context"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// GuardrailsSource monitors safety conditions
type GuardrailsSource struct {
	*TickerSource
	adapter      agent.AgentLooper
	lastSignal   time.Time
	minInterval  time.Duration // Rate limit: minimum time between signals
	mu           sync.Mutex
}

// NewGuardrailsSource creates a new guardrails source
func NewGuardrailsSource(adapter agent.AgentLooper, checkInterval time.Duration, signalInterval time.Duration, logger *log.Logger) *GuardrailsSource {
	return &GuardrailsSource{
		TickerSource: NewTickerSource(Config{
			Name:      "guardrails",
			Interval:  checkInterval,
			SkipFirst: false,
		}, logger),
		adapter:     adapter,
		minInterval: signalInterval,
	}
}

// Start begins checking guardrails
func (s *GuardrailsSource) Start(ctx context.Context, out chan<- events.Event) error {
	return s.TickerSource.Start(ctx, out, s.check)
}

// check monitors guardrails and emits signals if needed
func (s *GuardrailsSource) check(ctx context.Context) (events.Event, error) {
	signal := s.adapter.Guardrails()

	// Rate limiting: only emit signal if enough time has passed
	s.mu.Lock()
	shouldEmit := signal != nil && time.Since(s.lastSignal) >= s.minInterval
	if shouldEmit {
		s.lastSignal = time.Now()
	}
	s.mu.Unlock()

	if shouldEmit {
		return events.NewGuardrailEvent(signal), nil
	}

	// Return nil event if no signal or rate-limited
	return nil, nil
}
