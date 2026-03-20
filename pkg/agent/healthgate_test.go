package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
	assert.False(t, hg.IsClosed())
}

func TestHealthGate_NilReceiver_ReportErrorDoesNotPanic(_ *testing.T) {
	var hg *HealthGate
	hg.ReportError(errors.New("should not panic"))
}

// ---------------------------------------------------------------------------
// ReportError — error classification
// ---------------------------------------------------------------------------

func TestReportError_ConnectionError_TripsGate(t *testing.T) {
	hg := NewHealthGate(context.Background(), nil, func(_ error) bool { return true }, testLogger())

	hg.ReportError(errors.New("connection refused"))

	if !hg.isDown.Load() {
		t.Fatal("gate should be tripped by connection error")
	}
}

func TestReportError_NonConnectionError_DoesNotTrip(t *testing.T) {
	hg := NewHealthGate(context.Background(), nil, func(_ error) bool { return false }, testLogger())

	hg.ReportError(errors.New("relation does not exist"))
	hg.ReportError(context.Canceled)

	if hg.isDown.Load() {
		t.Fatal("gate should not be tripped by non-connection errors")
	}
}

func TestReportError_Nil_DoesNotTrip(t *testing.T) {
	hg := NewHealthGate(context.Background(), nil, func(_ error) bool { return true }, testLogger())

	hg.ReportError(nil)

	if hg.isDown.Load() {
		t.Fatal("ReportError(nil) should not trip the gate")
	}
}

func TestReportError_ConcurrentFromManyGoroutines(t *testing.T) {
	hg := NewHealthGate(context.Background(), nil, func(_ error) bool { return true }, testLogger())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hg.ReportError(errors.New("connection refused"))
		}()
	}
	wg.Wait()

	if !hg.isDown.Load() {
		t.Fatal("gate should be tripped after concurrent reports")
	}
}
