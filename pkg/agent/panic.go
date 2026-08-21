package agent

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// panicStackLimit bounds the stack captured in a panic error. The trace is
// forwarded to the backend as a log message and an error payload, so it has to
// stay small enough not to blow up those requests.
const panicStackLimit = 4096

// PanicError converts a recovered panic value into an error carrying the stack
// of the panicking goroutine. It must be called from within the deferred
// function that recovered, otherwise the stack no longer points at the panic.
func PanicError(name string, recovered any) error {
	stack := debug.Stack()
	if len(stack) > panicStackLimit {
		stack = stack[:panicStackLimit]
	}
	return fmt.Errorf("panic in %s: %v\n%s", name, recovered, stack)
}

// PanicReporter is the part of an agent a PanicGuard needs to surface a
// recovered panic.
type PanicReporter interface {
	Logger() *log.Logger
	SendError(ctx context.Context, payload ErrorPayload) error
}

// PanicGuard makes the agent's periodic work panic-proof: a panicking collector
// cannot take the process down, everything else keeps running, and the
// panicking unit retries on its next tick.
//
// A collector that panics once almost always panics on every subsequent tick,
// so reports are throttled per name to the 1st, 10th, 100th, ... consecutive
// panic. Each carries its occurrence count, which is what conveys how long the
// unit has been broken: on the 5s metrics ticker that is a report at first
// failure, then ~50s, ~8m, ~1h20m. A run that does not panic clears the count.
type PanicGuard struct {
	reporter PanicReporter
	mu       sync.Mutex
	panics   map[string]uint64
}

func NewPanicGuard(reporter PanicReporter) *PanicGuard {
	return &PanicGuard{reporter: reporter, panics: make(map[string]uint64)}
}

// reportAt lists the consecutive-panic counts that produce a report. A unit
// panicking past the last entry has been failing for years at any tick rate.
var reportAt = []uint64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000}

// record counts a panic for name and reports whether this occurrence is due.
func (g *PanicGuard) record(name string) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.panics[name]++
	count := g.panics[name]
	return count, slices.Contains(reportAt, count)
}

func (g *PanicGuard) clear(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.panics, name)
}

// Wrap attaches panic recovery to a unit of periodic work. A due occurrence is
// sent to the platform as an error payload and returned to the caller, which
// logs it. Occurrences in between return nil and are logged at debug level
// only, so the panic loop neither floods the backend nor hides.
func (g *PanicGuard) Wrap(name string, fn func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) (err error) {
		defer func() {
			r := recover()
			if r == nil {
				g.clear(name)
				return
			}
			count, due := g.record(name)
			panicErr := PanicError(fmt.Sprintf("%s (consecutive panics: %d)", name, count), r)
			if !due {
				g.reporter.Logger().Debugf("%v", panicErr)
				return
			}
			err = panicErr
			g.report(ctx, name, panicErr)
		}()
		return fn(ctx)
	}
}

// reportTimeout bounds a panic report.
const reportTimeout = 5 * time.Second

// report sends the panic to the platform.
//
// The report is detached from the context the panicking work ran under: a
// metric collector panics inside its own per-collector timeout, which may be
// nearly spent or already cancelled by the time the panic surfaces, and that
// must not swallow the report.
//
// It also recovers its own panics: the report path runs inside the guard's
// deferred function, where a second panic would escape recovery and take down
// the very process this guard protects.
func (g *PanicGuard) report(ctx context.Context, name string, panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			g.reporter.Logger().Errorf("panic while reporting panic in %s: %v", name, r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	_ = g.reporter.SendError(ctx, ErrorPayload{
		ErrorMessage: panicErr.Error(),
		ErrorType:    name + "_panic",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	})
}
