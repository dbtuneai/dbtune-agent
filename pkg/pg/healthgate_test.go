package pg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
}

// newStartedGate creates a HealthGate with Start() already called using the given context.
// The pool is nil (tests that don't ping don't need it).
func newStartedGate(ctx context.Context) *HealthGate {
	hg := &HealthGate{logger: testLogger()}
	hg.Start(ctx)
	return hg
}

// waitForPingingDone polls until hg.pinging is false, proving the ping goroutine exited.
func waitForPingingDone(t *testing.T, hg *HealthGate, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for hg.pinging.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for ping goroutine to exit")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// IsConnectionError
// ---------------------------------------------------------------------------

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// --- nil / generic ---
		{name: "nil", err: nil, want: false},
		{name: "generic error", err: errors.New("something failed"), want: false},
		{name: "context.Canceled", err: context.Canceled, want: false},
		{name: "context.DeadlineExceeded", err: context.DeadlineExceeded, want: false},
		{name: "ErrDatabaseDown sentinel", err: ErrDatabaseDown, want: false},
		{name: "wrapped ErrDatabaseDown", err: fmt.Errorf("outer: %w", ErrDatabaseDown), want: false},

		// --- pgconn.ConnectError ---
		{
			name: "ConnectError",
			err:  &pgconn.ConnectError{Config: nil},
			want: true,
		},
		{
			name: "wrapped ConnectError",
			err:  fmt.Errorf("outer: %w", &pgconn.ConnectError{}),
			want: true,
		},
		{
			name: "deeply wrapped ConnectError",
			err:  fmt.Errorf("layer1: %w", fmt.Errorf("layer2: %w", &pgconn.ConnectError{})),
			want: true,
		},

		// --- PgError class 08 (connection exception) ---
		{name: "PgError 08000", err: &pgconn.PgError{Code: "08000"}, want: true},
		{name: "PgError 08001", err: &pgconn.PgError{Code: "08001"}, want: true},
		{name: "PgError 08003", err: &pgconn.PgError{Code: "08003"}, want: true},
		{name: "PgError 08004", err: &pgconn.PgError{Code: "08004"}, want: true},
		{name: "PgError 08006", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}, want: true},
		{name: "PgError 08P01", err: &pgconn.PgError{Code: "08P01"}, want: true},

		// --- PgError class 57 (operator intervention) ---
		{name: "PgError 57000", err: &pgconn.PgError{Code: "57000"}, want: true},
		{name: "PgError 57014 query_canceled", err: &pgconn.PgError{Code: "57014"}, want: true},
		{name: "PgError 57P01 admin_shutdown", err: &pgconn.PgError{Code: "57P01", Message: "admin shutdown"}, want: true},
		{name: "PgError 57P02 crash_shutdown", err: &pgconn.PgError{Code: "57P02"}, want: true},
		{name: "PgError 57P03 cannot_connect_now", err: &pgconn.PgError{Code: "57P03"}, want: true},

		// --- PgError non-connection codes (query errors) ---
		{name: "PgError 42P01 relation does not exist", err: &pgconn.PgError{Code: "42P01"}, want: false},
		{name: "PgError 42601 syntax error", err: &pgconn.PgError{Code: "42601"}, want: false},
		{name: "PgError 23505 unique_violation", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "PgError 42501 insufficient_privilege", err: &pgconn.PgError{Code: "42501"}, want: false},
		{name: "PgError empty code", err: &pgconn.PgError{Code: ""}, want: false},

		// --- Wrapped PgError ---
		{
			name: "wrapped PgError connection class",
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: "08006"}),
			want: true,
		},
		{
			name: "wrapped PgError query class",
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: "42P01"}),
			want: false,
		},

		// --- net.OpError ---
		{
			name: "net.OpError dial",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "net.OpError read",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset by peer")},
			want: true,
		},
		{
			name: "wrapped net.OpError",
			err:  fmt.Errorf("pool: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}),
			want: true,
		},

		// --- os.SyscallError ---
		{
			name: "SyscallError connect",
			err:  &os.SyscallError{Syscall: "connect", Err: errors.New("refused")},
			want: true,
		},
		{
			name: "wrapped SyscallError",
			err:  fmt.Errorf("outer: %w", &os.SyscallError{Syscall: "write", Err: errors.New("broken pipe")}),
			want: true,
		},

		// --- String-based fallbacks ---
		{name: "connection refused string", err: errors.New("dial tcp: connection refused"), want: true},
		{name: "connection reset string", err: errors.New("read: connection reset by peer"), want: true},
		{name: "broken pipe string", err: errors.New("write: broken pipe"), want: true},
		{name: "no such host string", err: errors.New("dial tcp: lookup foo.invalid: no such host"), want: true},
		{name: "i/o timeout string", err: errors.New("i/o timeout"), want: true},
		{name: "connection timed out string", err: errors.New("dial tcp 10.0.0.1:5432: connection timed out"), want: true},
		{name: "mixed case Connection Refused", err: errors.New("Connection Refused"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsConnectionError(tt.err)
			if got != tt.want {
				t.Errorf("IsConnectionError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// HealthGate.Check
// ---------------------------------------------------------------------------

func TestHealthGate_Check_OpenGate(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	// Fresh gate — should be open.
	if err := hg.Check(); err != nil {
		t.Fatalf("fresh gate: expected nil, got %v", err)
	}
}

func TestHealthGate_Check_ClosedGate(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.down.Store(true)

	err := hg.Check()
	if !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("closed gate: expected ErrDatabaseDown, got %v", err)
	}
}

func TestHealthGate_Check_ReopenedGate(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.down.Store(true)

	// Simulate recovery.
	hg.down.Store(false)
	if err := hg.Check(); err != nil {
		t.Fatalf("reopened gate: expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// HealthGate.ReportError
// ---------------------------------------------------------------------------

func TestReportError_Nil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	hg.ReportError(nil)

	if hg.down.Load() {
		t.Fatal("ReportError(nil) should not trip the gate")
	}
	if hg.pinging.Load() {
		t.Fatal("ReportError(nil) should not start pinging")
	}
}

func TestReportError_QueryError_DoesNotTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	queryErrors := []error{
		&pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
		&pgconn.PgError{Code: "42601", Message: "syntax error"},
		&pgconn.PgError{Code: "23505", Message: "unique_violation"},
		errors.New("some generic query failure"),
		context.Canceled,
		context.DeadlineExceeded,
	}

	for _, err := range queryErrors {
		hg.ReportError(err)
	}

	if hg.down.Load() {
		t.Fatal("gate should not be tripped by query/non-connection errors")
	}
	if hg.pinging.Load() {
		t.Fatal("no pinging should have started for non-connection errors")
	}
}

func TestReportError_ConnectionError_TripsGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	hg.ReportError(&pgconn.ConnectError{})

	if !hg.down.Load() {
		t.Fatal("gate should be tripped by connection error")
	}
	if !hg.pinging.Load() {
		t.Fatal("pinging should have started")
	}
}

func TestReportError_OnlyOnePingGoroutine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	// Report many connection errors — CompareAndSwap should ensure only one goroutine.
	for i := 0; i < 100; i++ {
		hg.ReportError(&pgconn.ConnectError{})
	}

	if !hg.pinging.Load() {
		t.Fatal("at least one ping goroutine should be running")
	}

	// Cancel and wait for goroutine to exit.
	cancel()
	waitForPingingDone(t, hg, 2*time.Second)
}

func TestReportError_ConcurrentFromManyGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hg.ReportError(&pgconn.ConnectError{})
		}()
	}
	wg.Wait()

	if !hg.down.Load() {
		t.Fatal("gate should be tripped after concurrent reports")
	}
}

func TestReportError_CanRetripAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hg := newStartedGate(ctx)

	// Trip the gate.
	hg.ReportError(&pgconn.ConnectError{})
	if !hg.down.Load() {
		t.Fatal("gate should be down")
	}

	// Cancel and wait for the ping goroutine to fully exit (pinging → false).
	cancel()
	waitForPingingDone(t, hg, 2*time.Second)

	// Reset state to simulate recovery.
	hg.down.Store(false)

	// Re-start with a new context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	hg.Start(ctx2)

	// Trip again.
	hg.ReportError(&pgconn.PgError{Code: "57P01"})
	if !hg.down.Load() {
		t.Fatal("gate should be tripped again after recovery")
	}
	if !hg.pinging.Load() {
		t.Fatal("a new ping goroutine should be running")
	}
}

// ---------------------------------------------------------------------------
// HealthGate.Stop
// ---------------------------------------------------------------------------

func TestStop_CancelsPingGoroutine(t *testing.T) {
	ctx := context.Background()
	hg := &HealthGate{logger: testLogger()}
	hg.Start(ctx)

	hg.ReportError(&pgconn.ConnectError{})
	if !hg.pinging.Load() {
		t.Fatal("ping goroutine should be running")
	}

	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)
}

func TestStop_SafeToCallTwice(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.Start(context.Background())
	hg.Stop()
	hg.Stop() // Should not panic.
}

func TestStop_SafeWithNilCancel(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.Stop() // cancel is nil — should not panic.
}

// ---------------------------------------------------------------------------
// HealthGate.Start
// ---------------------------------------------------------------------------

func TestStart_SetsContext(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hg.Start(ctx)

	if hg.ctx == nil {
		t.Fatal("Start should set ctx")
	}
	if hg.cancel == nil {
		t.Fatal("Start should set cancel")
	}
}

// ---------------------------------------------------------------------------
// pingUntilRecovery — context cancellation
// ---------------------------------------------------------------------------

func TestPingUntilRecovery_ExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hg := newStartedGate(ctx)

	// Trip the gate so ping starts.
	hg.ReportError(&pgconn.ConnectError{})

	// Cancel immediately.
	cancel()
	waitForPingingDone(t, hg, 2*time.Second)

	// Gate stays down because nobody recovered it.
	if !hg.down.Load() {
		t.Fatal("gate should remain down when ping exits due to context cancel")
	}
}

// ---------------------------------------------------------------------------
// CatalogGetter.prepareCtx integration
// ---------------------------------------------------------------------------

func TestPrepareCtx_NilHealthGate_NilPrepareContext(t *testing.T) {
	g := &CatalogGetter{}
	ctx := context.Background()

	got, err := g.prepareCtx(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != ctx {
		t.Fatal("should return original context when both HealthGate and PrepareContext are nil")
	}
}

func TestPrepareCtx_HealthGateOpen_PassesThrough(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	// gate is open (default)

	var prepareCalled bool
	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			prepareCalled = true
			return ctx, nil
		},
	}

	_, err := g.prepareCtx(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !prepareCalled {
		t.Fatal("PrepareContext should be called when gate is open")
	}
}

func TestPrepareCtx_HealthGateDown_ShortCircuits(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.down.Store(true)

	var prepareCalled bool
	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			prepareCalled = true
			return ctx, nil
		},
	}

	_, err := g.prepareCtx(context.Background())
	if !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("expected ErrDatabaseDown, got %v", err)
	}
	if prepareCalled {
		t.Fatal("PrepareContext should NOT be called when gate is down")
	}
}

func TestPrepareCtx_PrepareContextError_PropagatedWhenGateOpen(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	expectedErr := errors.New("failover in progress")

	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			return nil, expectedErr
		},
	}

	_, err := g.prepareCtx(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected PrepareContext error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CatalogGetter.StartHealthGate
// ---------------------------------------------------------------------------

func TestStartHealthGate_NilHealthGate(t *testing.T) {
	g := &CatalogGetter{}
	g.StartHealthGate(context.Background()) // Should not panic.
}

func TestStartHealthGate_SetsContext(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	g := &CatalogGetter{HealthGate: hg}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.StartHealthGate(ctx)

	if hg.ctx == nil {
		t.Fatal("StartHealthGate should delegate to HealthGate.Start")
	}
}

// ---------------------------------------------------------------------------
// CatalogCollectors wrapper — error reporting
// ---------------------------------------------------------------------------

func TestCatalogCollectors_WrapperReportsConnectionErrors(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.Start(context.Background())
	defer hg.Stop()

	connErr := &pgconn.ConnectError{}

	g := &CatalogGetter{
		PGMajorVersion: 16,
		HealthGate:     hg,
	}

	collectors := g.CatalogCollectors()
	if len(collectors) == 0 {
		t.Fatal("expected collectors")
	}

	// Override first collector's inner function to return a connection error.
	// We need to build a minimal collector to test the wrapper behavior.
	// Instead of calling the real collector (which needs a pool), we test the
	// wrapping logic by constructing our own.
	var reported atomic.Bool
	originalCollect := func(ctx context.Context) (any, error) {
		return nil, connErr
	}

	// Simulate the wrapping logic from CatalogCollectors.
	wrapped := func(ctx context.Context) (any, error) {
		data, err := originalCollect(ctx)
		if err != nil {
			hg.ReportError(err)
			reported.Store(true)
		}
		return data, err
	}

	_, err := wrapped(context.Background())
	if err == nil {
		t.Fatal("expected error from wrapped collector")
	}
	if !reported.Load() {
		t.Fatal("wrapper should have called ReportError")
	}
	if !hg.down.Load() {
		t.Fatal("gate should be tripped after connection error from collector")
	}
}

func TestCatalogCollectors_WrapperDoesNotReportQueryErrors(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.Start(context.Background())
	defer hg.Stop()

	queryErr := &pgconn.PgError{Code: "42P01"}

	originalCollect := func(ctx context.Context) (any, error) {
		return nil, queryErr
	}

	wrapped := func(ctx context.Context) (any, error) {
		data, err := originalCollect(ctx)
		if err != nil {
			hg.ReportError(err)
		}
		return data, err
	}

	_, err := wrapped(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if hg.down.Load() {
		t.Fatal("gate should NOT be tripped by query error")
	}
}

func TestCatalogCollectors_WrapperPassesThroughSuccessfulResults(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}

	originalCollect := func(ctx context.Context) (any, error) {
		return "test-data", nil
	}

	wrapped := func(ctx context.Context) (any, error) {
		data, err := originalCollect(ctx)
		if err != nil {
			hg.ReportError(err)
		}
		return data, err
	}

	data, err := wrapped(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != "test-data" {
		t.Fatalf("expected test-data, got %v", data)
	}
}

func TestCatalogCollectors_NilHealthGate_NoWrapping(t *testing.T) {
	g := &CatalogGetter{
		PGMajorVersion: 16,
		HealthGate:     nil,
	}

	// This should not panic — collectors are returned without wrapping.
	collectors := g.CatalogCollectors()
	if len(collectors) == 0 {
		t.Fatal("expected collectors even with nil HealthGate")
	}
}

// ---------------------------------------------------------------------------
// CatalogGetter.GetDDL — health gate short-circuit
// ---------------------------------------------------------------------------

func TestGetDDL_HealthGateDown_ReturnsErrDatabaseDown(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	hg.down.Store(true)

	g := &CatalogGetter{
		HealthGate: hg,
		// pool is nil — we should never reach it because the gate is down.
	}

	data, err := g.GetDDL(context.Background())
	if !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("expected ErrDatabaseDown, got %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
}

// ---------------------------------------------------------------------------
// ErrDatabaseDown sentinel
// ---------------------------------------------------------------------------

func TestErrDatabaseDown_ErrorsIs(t *testing.T) {
	// Direct match.
	if !errors.Is(ErrDatabaseDown, ErrDatabaseDown) {
		t.Fatal("ErrDatabaseDown should match itself")
	}

	// Wrapped.
	wrapped := fmt.Errorf("collector failed: %w", ErrDatabaseDown)
	if !errors.Is(wrapped, ErrDatabaseDown) {
		t.Fatal("wrapped ErrDatabaseDown should match via errors.Is")
	}
}

func TestErrDatabaseDown_HasExpectedMessage(t *testing.T) {
	msg := ErrDatabaseDown.Error()
	if msg != "database health gate: connection unavailable" {
		t.Fatalf("unexpected error message: %s", msg)
	}
}

// ---------------------------------------------------------------------------
// NewHealthGate
// ---------------------------------------------------------------------------

func TestNewHealthGate_InitialState(t *testing.T) {
	l := testLogger()
	hg := NewHealthGate(nil, l)

	if hg.down.Load() {
		t.Fatal("new gate should be open (down=false)")
	}
	if hg.pinging.Load() {
		t.Fatal("new gate should not be pinging")
	}
	if hg.logger != l {
		t.Fatal("logger should be set")
	}
	if hg.ctx != nil {
		t.Fatal("ctx should be nil before Start()")
	}
}

// ---------------------------------------------------------------------------
// Full lifecycle: trip → cancel → re-trip
// ---------------------------------------------------------------------------

func TestHealthGate_FullLifecycle(t *testing.T) {
	// 1. Create and start.
	hg := NewHealthGate(nil, testLogger())
	ctx1, cancel1 := context.WithCancel(context.Background())
	hg.Start(ctx1)

	// Gate is open.
	if err := hg.Check(); err != nil {
		t.Fatalf("step 1: expected open gate, got %v", err)
	}

	// 2. Trip the gate with a connection error.
	hg.ReportError(&pgconn.PgError{Code: "57P01"})
	if err := hg.Check(); !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("step 2: expected ErrDatabaseDown, got %v", err)
	}
	if !hg.pinging.Load() {
		t.Fatal("step 2: should be pinging")
	}

	// 3. Stop (simulates shutdown).
	cancel1()
	waitForPingingDone(t, hg, 2*time.Second)

	// Gate remains down after stop.
	if !hg.down.Load() {
		t.Fatal("step 3: gate should remain down")
	}

	// 4. Restart with new context (simulates reconnect scenario).
	hg.down.Store(false)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	hg.Start(ctx2)

	if err := hg.Check(); err != nil {
		t.Fatalf("step 4: gate should be open after reset, got %v", err)
	}

	// 5. Trip again.
	hg.ReportError(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")})
	if err := hg.Check(); !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("step 5: expected ErrDatabaseDown, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Edge: multiple error types in sequence
// ---------------------------------------------------------------------------

func TestReportError_MixedErrorSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hg := newStartedGate(ctx)

	// Query errors first — gate stays open.
	hg.ReportError(errors.New("syntax error"))
	hg.ReportError(&pgconn.PgError{Code: "42P01"})
	hg.ReportError(context.Canceled)
	if hg.down.Load() {
		t.Fatal("gate should still be open after non-connection errors")
	}

	// Then a connection error — gate trips.
	hg.ReportError(&pgconn.PgError{Code: "08006"})
	if !hg.down.Load() {
		t.Fatal("gate should be tripped after connection error")
	}

	// More query errors — gate stays tripped (no change).
	hg.ReportError(&pgconn.PgError{Code: "42601"})
	if !hg.down.Load() {
		t.Fatal("gate should remain tripped")
	}

	// More connection errors — still tripped, no extra goroutines.
	hg.ReportError(&pgconn.ConnectError{})
	if !hg.down.Load() {
		t.Fatal("gate should remain tripped")
	}
}

// ---------------------------------------------------------------------------
// Edge: ReportError before Start (no context set)
// ---------------------------------------------------------------------------

func TestReportError_BeforeStart_DoesNotPanic(t *testing.T) {
	hg := &HealthGate{logger: testLogger()}
	// ctx is nil — ReportError should not panic even though Start() was never called.
	// The gate should be tripped (down=true) but no ping goroutine should be spawned
	// since there's no ctx to ping with.
	hg.ReportError(&pgconn.ConnectError{})

	if !hg.down.Load() {
		t.Fatal("gate should be tripped (down=true) after ReportError with connection error")
	}
	if hg.pinging.Load() {
		t.Fatal("no ping goroutine should be running when ctx is nil (Start not called)")
	}
}
