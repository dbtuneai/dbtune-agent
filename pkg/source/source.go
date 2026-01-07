package source

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
)

// SourceRunner is a simple struct that holds everything needed to run a source
// This avoids the need for interfaces and boilerplate methods
type SourceRunner struct {
	Name     string
	Interval time.Duration
	Start    func(ctx context.Context, out chan<- events.Event) error
}
