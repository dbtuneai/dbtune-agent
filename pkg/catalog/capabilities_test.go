package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dbtuneai/agent/pkg/capabilities"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

// discardLogger is a logger for tests that don't check log output.
func discardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

// fakeChecker returns whatever caps points at on each tick.
func fakeChecker(name string, caps *[]capabilities.Capability) capabilities.CapabilityChecker {
	return capabilities.CapabilityChecker{
		Name:  name,
		Check: func(context.Context) ([]capabilities.Capability, error) { return *caps, nil },
	}
}

func collectCapabilities(t *testing.T, c Collector) *CapabilitiesPayload {
	t.Helper()
	r, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() error: %v", err)
	}
	if r == nil {
		return nil
	}
	var p CapabilitiesPayload
	if err := json.Unmarshal(r.JSON, &p); err != nil {
		t.Fatalf("failed to parse payload: %v", err)
	}
	return &p
}

func capabilityNames(caps []capabilities.Capability) []string {
	names := make([]string, len(caps))
	for i, c := range caps {
		names[i] = c.Name + "=" + string(c.Status)
	}
	return names
}

func TestCapabilitiesCollector_PrefixesAndOrdersCheckers(t *testing.T) {
	pg := []capabilities.Capability{{Name: "pg_read_all_stats", Status: capabilities.CapabilityOK}}
	aws := []capabilities.Capability{
		{Name: "rds:DescribeDBClusters", Status: capabilities.CapabilityMissing},
		{Name: "rds:RebootDBInstance", Status: capabilities.CapabilityUnknown, Error: "can't be checked without rebooting"},
	}
	c := CapabilitiesCollector(discardLogger(), fakeChecker("postgres", &pg), fakeChecker("aws", &aws))

	p := collectCapabilities(t, c)
	if p == nil {
		t.Fatal("first tick sent nothing")
	}
	got := strings.Join(capabilityNames(p.Capabilities), " ")
	want := "postgres.pg_read_all_stats=ok aws.rds:DescribeDBClusters=missing aws.rds:RebootDBInstance=unknown"
	if got != want {
		t.Errorf("capabilities = %s, want %s", got, want)
	}
	if p.CollectedAt.IsZero() {
		t.Error("expected collected_at")
	}
}

func TestCapabilitiesCollector_SendsOnlyOnChange(t *testing.T) {
	caps := []capabilities.Capability{{Name: "a", Status: capabilities.CapabilityMissing}}
	c := CapabilitiesCollector(discardLogger(), fakeChecker("x", &caps))

	if collectCapabilities(t, c) == nil {
		t.Fatal("first tick sent nothing")
	}
	if p := collectCapabilities(t, c); p != nil {
		t.Fatalf("unchanged tick sent %+v", p)
	}
	caps = []capabilities.Capability{{Name: "a", Status: capabilities.CapabilityOK}}
	p := collectCapabilities(t, c)
	if p == nil || p.Capabilities[0].Status != capabilities.CapabilityOK {
		t.Fatalf("changed tick = %+v, want x.a ok", p)
	}
}

func TestCapabilitiesCollector_CheckerErrorFailsTick(t *testing.T) {
	ok := []capabilities.Capability{{Name: "a", Status: capabilities.CapabilityOK}}
	failing := capabilities.CapabilityChecker{
		Name:  "broken",
		Check: func(context.Context) ([]capabilities.Capability, error) { return nil, errors.New("boom") },
	}
	c := CapabilitiesCollector(discardLogger(), fakeChecker("x", &ok), failing)
	_, err := c.Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "broken checker: boom") {
		t.Fatalf("Collect() error = %v, want the broken checker's error", err)
	}
}

func TestLogCapabilityChanges(t *testing.T) {
	logger, hook := test.NewNullLogger()

	missing := capabilities.Capability{Name: "postgres.pg_read_all_stats", Status: capabilities.CapabilityMissing, Impact: "stats hidden", Fix: "GRANT pg_monitor TO x;"}
	ok := capabilities.Capability{Name: "postgres.pg_read_all_settings", Status: capabilities.CapabilityOK}
	unknown := capabilities.Capability{Name: "aws.rds:RebootDBInstance", Status: capabilities.CapabilityUnknown, Error: "not checkable"}

	// First tick: only the missing and unknown ones are logged.
	logCapabilityChanges(logger, nil, []capabilities.Capability{missing, ok, unknown})
	entries := hook.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("first tick logged %d entries, want 2: %v", len(entries), entries)
	}
	if entries[0].Level != logrus.WarnLevel || entries[0].Message != "missing capability postgres.pg_read_all_stats: stats hidden. Fix with: GRANT pg_monitor TO x;" {
		t.Errorf("missing capability logged as %s %q", entries[0].Level, entries[0].Message)
	}
	if entries[1].Level != logrus.InfoLevel || entries[1].Message != "can't check capability aws.rds:RebootDBInstance: not checkable" {
		t.Errorf("unknown capability logged as %s %q", entries[1].Level, entries[1].Message)
	}

	// Unchanged: nothing logged.
	hook.Reset()
	prev := map[string]capabilities.Capability{missing.Name: missing, ok.Name: ok, unknown.Name: unknown}
	logCapabilityChanges(logger, prev, []capabilities.Capability{missing, ok, unknown})
	if n := len(hook.AllEntries()); n != 0 {
		t.Errorf("unchanged tick logged %d entries", n)
	}

	// Recovered: logged once at info.
	hook.Reset()
	fixed := missing
	fixed.Status = capabilities.CapabilityOK
	logCapabilityChanges(logger, prev, []capabilities.Capability{fixed, ok, unknown})
	entries = hook.AllEntries()
	if len(entries) != 1 || entries[0].Level != logrus.InfoLevel || entries[0].Message != "capability postgres.pg_read_all_stats is now available" {
		t.Errorf("recovery logged as %v, want one info entry", entries)
	}
}
