package sink

import (
	"context"

	"github.com/dbtuneai/agent/pkg/events"
)

// Sink processes events
type Sink interface {
	// Process handles an incoming event
	Process(ctx context.Context, event events.Event) error

	// Name returns the sink identifier
	Name() string
}

// Flusher is implemented by sinks that support flushing buffered data
type Flusher interface {
	FlushMetrics(ctx context.Context) error
}

// Closer is implemented by sinks that need cleanup
type Closer interface {
	Close() error
}
