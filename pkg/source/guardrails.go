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
	SourceName     string
	SourceInterval time.Duration
	Adapter        agent.AgentLooper
	Logger         *log.Logger
	SkipFirst      bool
	MinInterval    time.Duration // Rate limit: minimum time between signals
	lastSignal     time.Time
	mu             sync.Mutex
}

// NewGuardrailsSource creates a new guardrails source
func NewGuardrailsSource(adapter agent.AgentLooper, checkInterval time.Duration, signalInterval time.Duration, logger *log.Logger) *GuardrailsSource {
	return &GuardrailsSource{
		SourceName:     "guardrails",
		SourceInterval: checkInterval,
		Adapter:        adapter,
		Logger:         logger,
		SkipFirst:      false,
		MinInterval:    signalInterval,
	}
}

// Name returns the source name
func (s *GuardrailsSource) Name() string {
	return s.SourceName
}

// Interval returns the source interval
func (s *GuardrailsSource) Interval() time.Duration {
	return s.SourceInterval
}

// Start begins checking guardrails
func (s *GuardrailsSource) Start(ctx context.Context, out chan<- events.Event) error {
	return RunWithTicker(ctx, out, s.SourceInterval, s.SkipFirst, s.Logger, s.SourceName, func(ctx context.Context) (events.Event, error) {
		signal := s.Adapter.Guardrails()

		// Rate limiting: only emit signal if enough time has passed
		s.mu.Lock()
		shouldEmit := signal != nil && time.Since(s.lastSignal) >= s.MinInterval
		if shouldEmit {
			s.lastSignal = time.Now()
		}
		s.mu.Unlock()

		if shouldEmit {
			return events.NewGuardrailEvent(signal), nil
		}

		// Return nil event if no signal or rate-limited
		return nil, nil
	})
}
