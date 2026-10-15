package queries

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// fakeChecker returns whatever caps points at on each tick.
func fakeChecker(name string, caps *[]Capability) CapabilityChecker {
	return CapabilityChecker{
		Name:  name,
		Check: func(context.Context) ([]Capability, error) { return *caps, nil },
	}
}

func collectCapabilities(t *testing.T, c CatalogCollector) *CapabilitiesPayload {
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

func capabilityNames(caps []Capability) []string {
	names := make([]string, len(caps))
	for i, c := range caps {
		names[i] = c.Name + "=" + string(c.Status)
	}
	return names
}

func TestCapabilitiesCollector_PrefixesAndOrdersCheckers(t *testing.T) {
	pg := []Capability{{Name: "pg_read_all_stats", Status: CapabilityOK}}
	aws := []Capability{
		{Name: "rds:DescribeDBClusters", Status: CapabilityMissing},
		{Name: "rds:RebootDBInstance", Status: CapabilityUnknown, Error: "can't be checked without rebooting"},
	}
	c := CapabilitiesCollector(fakeChecker("postgres", &pg), fakeChecker("aws", &aws))

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
	caps := []Capability{{Name: "a", Status: CapabilityMissing}}
	c := CapabilitiesCollector(fakeChecker("x", &caps))

	if collectCapabilities(t, c) == nil {
		t.Fatal("first tick sent nothing")
	}
	if p := collectCapabilities(t, c); p != nil {
		t.Fatalf("unchanged tick sent %+v", p)
	}
	caps = []Capability{{Name: "a", Status: CapabilityOK}}
	p := collectCapabilities(t, c)
	if p == nil || p.Capabilities[0].Status != CapabilityOK {
		t.Fatalf("changed tick = %+v, want x.a ok", p)
	}
}

func TestCapabilitiesCollector_CheckerErrorFailsTick(t *testing.T) {
	ok := []Capability{{Name: "a", Status: CapabilityOK}}
	failing := CapabilityChecker{
		Name:  "broken",
		Check: func(context.Context) ([]Capability, error) { return nil, errors.New("boom") },
	}
	c := CapabilitiesCollector(fakeChecker("x", &ok), failing)
	_, err := c.Collect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "broken checker: boom") {
		t.Fatalf("Collect() error = %v, want the broken checker's error", err)
	}
}

func TestLogCapabilityChanges(t *testing.T) {
	var buf bytes.Buffer
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	missing := Capability{Name: "postgres.pg_read_all_stats", Status: CapabilityMissing, Impact: "stats hidden", Fix: "GRANT pg_monitor TO x;"}
	ok := Capability{Name: "postgres.pg_read_all_settings", Status: CapabilityOK}
	unknown := Capability{Name: "aws.rds:RebootDBInstance", Status: CapabilityUnknown, Error: "not checkable"}

	// First tick: only the missing and unknown ones are logged.
	logCapabilityChanges(nil, []Capability{missing, ok, unknown})
	first := buf.String()
	for _, want := range []string{
		"level=WARN", "missing capability postgres.pg_read_all_stats: stats hidden. Fix with: GRANT pg_monitor TO x;",
		"can't check capability aws.rds:RebootDBInstance: not checkable",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("first tick log missing %q:\n%s", want, first)
		}
	}
	if strings.Contains(first, "pg_read_all_settings") {
		t.Errorf("first tick logged an ok capability:\n%s", first)
	}

	// Unchanged: nothing logged.
	buf.Reset()
	prev := map[string]Capability{missing.Name: missing, ok.Name: ok, unknown.Name: unknown}
	logCapabilityChanges(prev, []Capability{missing, ok, unknown})
	if buf.Len() != 0 {
		t.Errorf("unchanged tick logged:\n%s", buf.String())
	}

	// Recovered: logged once at info.
	buf.Reset()
	fixed := missing
	fixed.Status = CapabilityOK
	logCapabilityChanges(prev, []Capability{fixed, ok, unknown})
	if got := buf.String(); !strings.Contains(got, "level=INFO") || !strings.Contains(got, "capability postgres.pg_read_all_stats is now available") {
		t.Errorf("recovery not logged:\n%s", got)
	}
}

func TestPostgresCapabilitiesFor(t *testing.T) {
	caps := postgresCapabilitiesFor(PostgresRolePrivileges{Role: `odd"role`, ReadAllSettings: true})
	got := strings.Join(capabilityNames(caps), " ")
	if want := "pg_read_all_stats=missing pg_read_all_settings=ok"; got != want {
		t.Errorf("capabilities = %s, want %s", got, want)
	}
	if want := `GRANT pg_monitor TO "odd""role";`; caps[0].Fix != want {
		t.Errorf("fix = %q, want %q", caps[0].Fix, want)
	}
	if caps[0].Impact == "" {
		t.Error("expected an impact")
	}
}

// Every capability's fix must take exactly the role name.
func TestPostgresCapabilities_Fix(t *testing.T) {
	for _, pc := range postgresCapabilities {
		if strings.Count(pc.fix, "%") != 1 || strings.Count(pc.fix, "%s") != 1 {
			t.Errorf("%s: fix %q must have exactly one %%s and no other verbs", pc.name, pc.fix)
		}
	}
}
