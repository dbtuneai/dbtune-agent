package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
)

// Source produces events on a channel
type Source interface {
	// Start begins producing events on the output channel
	// Returns when ctx is cancelled
	Start(ctx context.Context, out chan<- events.Event) error

	// Name returns the source identifier
	Name() string

	// Interval returns how often this source produces events
	Interval() time.Duration
}
