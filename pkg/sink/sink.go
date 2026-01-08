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

	// Flush any buffered metrics the sink may have
	FlushMetrics(ctx context.Context) error

	// Close any resources that need cleanup
	Close() error
}