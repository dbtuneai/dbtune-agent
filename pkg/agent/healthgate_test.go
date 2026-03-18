package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
}

// alwaysConnectionError treats every non-nil error as a connection error.
func alwaysConnectionError(err error) bool { return err != nil }

// neverConnectionError never treats errors as connection errors.
func neverConnectionError(_ error) bool { return false }

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
// Nil-receiver safety
// ---------------------------------------------------------------------------

func TestHealthGate_NilReceiver_CheckReturnsNil(t *testing.T) {
	var hg *HealthGate
	if err := hg.Check(); err != nil {
		t.Fatalf("nil receiver Check() should return nil, got %v", err)
	}
}

func TestHealthGate_NilReceiver_ReportErrorDoesNotPanic(_ *testing.T) {
	var hg *HealthGate
	hg.ReportError(errors.New("should not panic"))
}

func TestHealthGate_NilReceiver_StopDoesNotPanic(_ *testing.T) {
	var hg *HealthGate
	hg.Stop()
}

// ---------------------------------------------------------------------------
// HealthGate.Check
// ---------------------------------------------------------------------------

func TestHealthGate_Check_OpenGate(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()
	if err := hg.Check(); err != nil {
		t.Fatalf("fresh gate: expected nil, got %v", err)
	}
}

func TestHealthGate_Check_ClosedGate(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()
	hg.down.Store(true)

	err := hg.Check()
	if !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("closed gate: expected ErrDatabaseDown, got %v", err)
	}
}

func TestHealthGate_Check_ReopenedGate(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()
	hg.down.Store(true)
	hg.down.Store(false)
	if err := hg.Check(); err != nil {
		t.Fatalf("reopened gate: expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// HealthGate.ReportError
// ---------------------------------------------------------------------------

func TestReportError_Nil(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()

	hg.ReportError(nil)

	if hg.down.Load() {
		t.Fatal("ReportError(nil) should not trip the gate")
	}
	if hg.pinging.Load() {
		t.Fatal("ReportError(nil) should not start pinging")
	}
}

func TestReportError_NonConnectionError_DoesNotTrip(t *testing.T) {
	hg := NewHealthGate(nil, neverConnectionError, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("some query failure"))
	hg.ReportError(context.Canceled)
	hg.ReportError(context.DeadlineExceeded)

	if hg.down.Load() {
		t.Fatal("gate should not be tripped by non-connection errors")
	}
	if hg.pinging.Load() {
		t.Fatal("no pinging should have started for non-connection errors")
	}
}

func TestReportError_ConnectionError_TripsGate(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("connection refused"))

	if !hg.down.Load() {
		t.Fatal("gate should be tripped by connection error")
	}
	// No pinger → no ping goroutine, but gate is still tripped.
}

func TestReportError_OnlyOnePingGoroutine(t *testing.T) {
	// Use a real pinger so the ping goroutine actually starts.
	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())
	defer hg.Stop()

	for i := 0; i < 100; i++ {
		hg.ReportError(errors.New("connection refused"))
	}

	if !hg.pinging.Load() {
		t.Fatal("at least one ping goroutine should be running")
	}

	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)
}

func TestReportError_ConcurrentFromManyGoroutines(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hg.ReportError(errors.New("connection refused"))
		}()
	}
	wg.Wait()

	if !hg.down.Load() {
		t.Fatal("gate should be tripped after concurrent reports")
	}
}

func TestReportError_CanRetripAfterRecovery(t *testing.T) {
	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())

	hg.ReportError(errors.New("connection refused"))
	if !hg.down.Load() {
		t.Fatal("gate should be down")
	}

	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)

	hg.down.Store(false)

	hg.ReportError(errors.New("connection reset"))
	if !hg.down.Load() {
		t.Fatal("gate should be tripped again after recovery")
	}
	if !hg.pinging.Load() {
		t.Fatal("a new ping goroutine should be running")
	}
	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)
}

// ---------------------------------------------------------------------------
// HealthGate.Stop
// ---------------------------------------------------------------------------

func TestStop_CancelsPingGoroutine(t *testing.T) {
	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())

	hg.ReportError(errors.New("connection refused"))
	if !hg.pinging.Load() {
		t.Fatal("ping goroutine should be running")
	}

	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)
}

func TestStop_SafeToCallTwice(_ *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	hg.Stop()
	hg.Stop()
}

// ---------------------------------------------------------------------------
// pingUntilRecovery — context cancellation
// ---------------------------------------------------------------------------

func TestPingUntilRecovery_ExitsOnStop(t *testing.T) {
	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())

	hg.ReportError(errors.New("connection refused"))
	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)

	if !hg.down.Load() {
		t.Fatal("gate should remain down when ping exits due to Stop")
	}
}

// ---------------------------------------------------------------------------
// ErrDatabaseDown sentinel
// ---------------------------------------------------------------------------

func TestErrDatabaseDown_ErrorsIs(t *testing.T) {
	if !errors.Is(ErrDatabaseDown, ErrDatabaseDown) {
		t.Fatal("ErrDatabaseDown should match itself")
	}

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
	hg := NewHealthGate(nil, alwaysConnectionError, l)
	defer hg.Stop()

	if hg.down.Load() {
		t.Fatal("new gate should be open (down=false)")
	}
	if hg.pinging.Load() {
		t.Fatal("new gate should not be pinging")
	}
	if hg.logger != l {
		t.Fatal("logger should be set")
	}
}

// ---------------------------------------------------------------------------
// Full lifecycle: trip → stop → re-trip
// ---------------------------------------------------------------------------

func TestHealthGate_FullLifecycle(t *testing.T) {
	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())

	if err := hg.Check(); err != nil {
		t.Fatalf("step 1: expected open gate, got %v", err)
	}

	hg.ReportError(errors.New("connection refused"))
	if err := hg.Check(); !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("step 2: expected ErrDatabaseDown, got %v", err)
	}
	if !hg.pinging.Load() {
		t.Fatal("step 2: should be pinging")
	}

	hg.Stop()
	waitForPingingDone(t, hg, 2*time.Second)

	if !hg.down.Load() {
		t.Fatal("step 3: gate should remain down")
	}

	hg.down.Store(false)

	if err := hg.Check(); err != nil {
		t.Fatalf("step 4: gate should be open after reset, got %v", err)
	}

	hg.ReportError(errors.New("broken pipe"))
	if err := hg.Check(); !errors.Is(err, ErrDatabaseDown) {
		t.Fatalf("step 5: expected ErrDatabaseDown, got %v", err)
	}
	hg.Stop()
}

// ---------------------------------------------------------------------------
// Edge: multiple error types in sequence
// ---------------------------------------------------------------------------

func TestReportError_MixedErrorSequence(t *testing.T) {
	isConnErr := func(err error) bool {
		return err != nil && len(err.Error()) > 5 && err.Error()[:5] == "conn:"
	}

	mp := &mockPinger{failCount: 1000}
	hg := NewHealthGate(mp, isConnErr, testLogger())
	defer hg.Stop()

	// Non-connection errors — gate stays open.
	hg.ReportError(errors.New("syntax error"))
	hg.ReportError(errors.New("query failed"))
	if hg.down.Load() {
		t.Fatal("gate should still be open after non-connection errors")
	}

	// Connection error — gate trips.
	hg.ReportError(errors.New("conn: refused"))
	if !hg.down.Load() {
		t.Fatal("gate should be tripped after connection error")
	}

	// More non-connection errors — gate stays tripped.
	hg.ReportError(errors.New("another query error"))
	if !hg.down.Load() {
		t.Fatal("gate should remain tripped")
	}
}

// ---------------------------------------------------------------------------
// pingUntilRecovery — successful recovery via mock Pinger
// ---------------------------------------------------------------------------

// mockPinger is a pinger that fails a configurable number of times then succeeds.
type mockPinger struct {
	mu        sync.Mutex
	failCount int // how many more times to fail
	pingCalls int // total calls observed
}

func (p *mockPinger) Ping(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pingCalls++
	if p.failCount > 0 {
		p.failCount--
		return errors.New("connection refused")
	}
	return nil
}

func (p *mockPinger) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pingCalls
}

func TestPingUntilRecovery_RecoversAfterFailures(t *testing.T) {
	mp := &mockPinger{failCount: 2} // fail twice, then succeed
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())
	defer hg.Stop()

	// Trip the gate.
	hg.ReportError(errors.New("connection refused"))
	if !hg.down.Load() {
		t.Fatal("gate should be down")
	}

	// Wait for the ping goroutine to recover (backoff is 1s, 2s — so ~3s total).
	deadline := time.Now().Add(10 * time.Second)
	for hg.down.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for gate to recover")
		}
		time.Sleep(50 * time.Millisecond)
	}

	waitForPingingDone(t, hg, 2*time.Second)
	if err := hg.Check(); err != nil {
		t.Fatalf("gate should be open after recovery, got %v", err)
	}
	if mp.calls() < 3 {
		t.Fatalf("expected at least 3 ping calls, got %d", mp.calls())
	}
}

func TestPingUntilRecovery_RecoversOnFirstPing(t *testing.T) {
	mp := &mockPinger{failCount: 0} // succeed immediately
	hg := NewHealthGate(mp, alwaysConnectionError, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("connection refused"))

	deadline := time.Now().Add(5 * time.Second)
	for hg.down.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for gate to recover")
		}
		time.Sleep(50 * time.Millisecond)
	}

	waitForPingingDone(t, hg, 2*time.Second)
	if err := hg.Check(); err != nil {
		t.Fatalf("gate should be open, got %v", err)
	}
	if mp.calls() != 1 {
		t.Fatalf("expected exactly 1 ping call, got %d", mp.calls())
	}
}
