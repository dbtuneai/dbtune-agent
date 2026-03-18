package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

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
}

func TestReportError_ConnectionError_TripsGate(t *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("connection refused"))

	if !hg.down.Load() {
		t.Fatal("gate should be tripped by connection error")
	}
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

// ---------------------------------------------------------------------------
// HealthGate.Stop
// ---------------------------------------------------------------------------

func TestStop_SafeToCallTwice(_ *testing.T) {
	hg := NewHealthGate(nil, alwaysConnectionError, testLogger())
	hg.Stop()
	hg.Stop()
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
// Recovery and ping goroutine tests require a real *pgxpool.Pool.
// See pkg/agent/healthgatetest/healthgate_integration_test.go.
// ---------------------------------------------------------------------------
