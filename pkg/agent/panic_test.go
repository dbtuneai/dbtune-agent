package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingReporter captures what a PanicGuard sends to the platform.
type recordingReporter struct {
	logger   *logrus.Logger
	mu       sync.Mutex
	payloads []ErrorPayload
	sendCtx  []error // ctx.Err() seen by each send
	sendErr  error
	panicOn  bool // panic instead of sending, mimicking a broken send path
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{logger: logrus.New()}
}

func (r *recordingReporter) Logger() *logrus.Logger { return r.logger }

func (r *recordingReporter) SendError(ctx context.Context, payload ErrorPayload) error {
	if r.panicOn {
		panic("send failed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.payloads = append(r.payloads, payload)
	r.sendCtx = append(r.sendCtx, ctx.Err())
	return r.sendErr
}

func (r *recordingReporter) sent() []ErrorPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ErrorPayload(nil), r.payloads...)
}

// A panicking collector must not take the process down. The panic becomes an
// error payload the platform can alert on, and the returned error is logged by
// the caller like any other failure.
func TestPanicGuard(t *testing.T) {
	panics := func(_ context.Context) error { panic("boom") }

	t.Run("panic becomes an error and is reported", func(t *testing.T) {
		r := newRecordingReporter()

		err := NewPanicGuard(r).Wrap("pg_stats", panics)(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic in pg_stats (consecutive panics: 1): boom")
		assert.Contains(t, err.Error(), "agent.TestPanicGuard", "error must carry the stack trace")

		sent := r.sent()
		require.Len(t, sent, 1)
		assert.Equal(t, "pg_stats_panic", sent[0].ErrorType)
		assert.Contains(t, sent[0].ErrorMessage, "panic in pg_stats")
		assert.Contains(t, sent[0].ErrorMessage, "boom")
	})

	// Runtime faults carry their cause in the recovered value, so the report
	// names the fault rather than just saying a panic happened.
	t.Run("reports the cause of a runtime fault", func(t *testing.T) {
		causes := []struct {
			name string
			fn   func(ctx context.Context) error
			want string
		}{
			{
				name: "nil pointer dereference",
				fn: func(_ context.Context) error {
					type row struct{ n int }
					var r *row
					return fmt.Errorf("%d", r.n)
				},
				want: "runtime error: invalid memory address or nil pointer dereference",
			},
			{
				name: "index out of range",
				fn: func(_ context.Context) error {
					rows := []int{}
					next := len(rows) + 3
					return fmt.Errorf("%d", rows[next])
				},
				want: "runtime error: index out of range [3] with length 0",
			},
			{
				name: "nil map write",
				fn: func(_ context.Context) error {
					var counts map[string]int
					counts["boom"] = 1
					return nil
				},
				want: "assignment to entry in nil map",
			},
		}

		for _, c := range causes {
			t.Run(c.name, func(t *testing.T) {
				r := newRecordingReporter()

				err := NewPanicGuard(r).Wrap("pg_stats", c.fn)(context.Background())

				require.Error(t, err)
				assert.Contains(t, err.Error(), c.want)
				sent := r.sent()
				require.Len(t, sent, 1)
				assert.Contains(t, sent[0].ErrorMessage, c.want, "the platform must see the cause, not just \"panicked\"")
			})
		}
	})

	t.Run("passes results through when there is no panic", func(t *testing.T) {
		r := newRecordingReporter()
		guard := NewPanicGuard(r)
		expectedErr := errors.New("collect failed")

		assert.NoError(t, guard.Wrap("pg_stats", func(_ context.Context) error {
			return nil
		})(context.Background()))

		assert.Equal(t, expectedErr, guard.Wrap("pg_stats", func(_ context.Context) error {
			return expectedErr
		})(context.Background()))

		assert.Empty(t, r.sent())
	})

	t.Run("report schedule is ascending powers of ten", func(t *testing.T) {
		require.Equal(t, uint64(1), reportAt[0], "the first panic must always report")
		for i, count := range reportAt[1:] {
			assert.Equal(t, reportAt[i]*10, count)
		}
	})

	// A task that panics once keeps panicking on every tick, so reports back
	// off to powers of ten and carry the count instead of firing every time.
	t.Run("reports back off exponentially", func(t *testing.T) {
		r := newRecordingReporter()
		guarded := NewPanicGuard(r).Wrap("pg_stats", panics)

		var reported []uint64
		for i := uint64(1); i <= 100; i++ {
			if err := guarded(context.Background()); err != nil {
				reported = append(reported, i)
				assert.Contains(t, err.Error(), fmt.Sprintf("consecutive panics: %d)", i))
			}
		}

		assert.Equal(t, []uint64{1, 10, 100}, reported)
		assert.Len(t, r.sent(), 3)
	})

	t.Run("a panic-free run clears the count", func(t *testing.T) {
		r := newRecordingReporter()
		guard := NewPanicGuard(r)
		guarded := guard.Wrap("pg_stats", panics)

		require.Error(t, guarded(context.Background())) // 1st, reported
		require.NoError(t, guarded(context.Background()) /* 2nd */, "suppressed occurrences must not surface")
		require.NoError(t, guarded(context.Background()) /* 3rd */)

		require.NoError(t, guard.Wrap("pg_stats", func(_ context.Context) error {
			return nil
		})(context.Background()))

		err := guarded(context.Background())
		require.Error(t, err, "the count restarts after a panic-free run")
		assert.Contains(t, err.Error(), "consecutive panics: 1)")
	})

	t.Run("counts are per name", func(t *testing.T) {
		r := newRecordingReporter()
		guard := NewPanicGuard(r)
		pgStats := guard.Wrap("pg_stats", panics)
		pgClass := guard.Wrap("pg_class", panics)

		for range 9 {
			_ = pgStats(context.Background())
			_ = pgClass(context.Background())
			_ = pgClass(context.Background())
		}

		err := pgStats(context.Background())
		require.Error(t, err, "pg_stats reports on its own 10th panic")
		assert.Contains(t, err.Error(), "consecutive panics: 10)", "pg_class must not advance pg_stats")
	})

	// A metric collector panics inside its own per-collector timeout, which
	// may be spent by the time the panic surfaces.
	t.Run("reports even when the caller's context is done", func(t *testing.T) {
		r := newRecordingReporter()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := NewPanicGuard(r).Wrap("hardware", panics)(ctx)

		require.Error(t, err)
		require.Len(t, r.sent(), 1, "a cancelled collector context must not swallow the report")
		r.mu.Lock()
		defer r.mu.Unlock()
		assert.NoError(t, r.sendCtx[0], "the report must run on a live context")
	})

	// Reporting runs inside the deferred recover, where a second panic would
	// escape and kill the process the guard exists to keep alive.
	t.Run("a panicking report path does not escape", func(t *testing.T) {
		r := newRecordingReporter()
		r.panicOn = true

		err := NewPanicGuard(r).Wrap("pg_stats", panics)(context.Background())

		require.Error(t, err, "the original panic still surfaces to the caller")
		assert.Contains(t, err.Error(), "panic in pg_stats")
	})
}
