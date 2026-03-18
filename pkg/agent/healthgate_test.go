package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
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
// ReportError — error classification
// ---------------------------------------------------------------------------

func TestReportError_ConnectionError_TripsGate(t *testing.T) {
	hg := NewHealthGate(nil, func(_ error) bool { return true }, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("connection refused"))

	if !hg.down.Load() {
		t.Fatal("gate should be tripped by connection error")
	}
}

func TestReportError_NonConnectionError_DoesNotTrip(t *testing.T) {
	hg := NewHealthGate(nil, func(_ error) bool { return false }, testLogger())
	defer hg.Stop()

	hg.ReportError(errors.New("relation does not exist"))
	hg.ReportError(context.Canceled)

	if hg.down.Load() {
		t.Fatal("gate should not be tripped by non-connection errors")
	}
}

func TestReportError_Nil_DoesNotTrip(t *testing.T) {
	hg := NewHealthGate(nil, func(_ error) bool { return true }, testLogger())
	defer hg.Stop()

	hg.ReportError(nil)

	if hg.down.Load() {
		t.Fatal("ReportError(nil) should not trip the gate")
	}
}

func TestReportError_ConcurrentFromManyGoroutines(t *testing.T) {
	hg := NewHealthGate(nil, func(_ error) bool { return true }, testLogger())
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
