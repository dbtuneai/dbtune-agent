package queries

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestUptimeMinutesRow_OptionalFieldsOmittedWhenUnset(t *testing.T) {
	data, err := json.Marshal(UptimeMinutesRow{UptimeMinutes: 12.5})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(data)
	if got != `{"uptime_minutes":12.5}` {
		t.Fatalf("expected optional fields omitted, got %s", got)
	}
}

func TestUptimeMinutesRow_OptionalFieldsPresentWhenSet(t *testing.T) {
	sysID := "7294956156579818536"
	timelineID := int64(3)
	started := "2026-07-27T10:15:00Z"
	data, err := json.Marshal(UptimeMinutesRow{
		UptimeMinutes:       1.0,
		SystemIdentifier:    &sysID,
		TimelineID:          &timelineID,
		PostmasterStartTime: &started,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(data)
	for _, want := range []string{
		`"system_identifier":"7294956156579818536"`,
		`"timeline_id":3`,
		`"postmaster_start_time":"2026-07-27T10:15:00Z"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %s in %s", want, got)
		}
	}
}

func TestClusterIdentityCache_ServesCachedWithinRefreshInterval(t *testing.T) {
	id := &clusterIdentity{systemIdentifier: "42", timelineID: 1}
	// pool is nil — a query attempt would panic, proving the cache is served.
	c := &clusterIdentityCache{cached: id, lastAttempt: time.Now()}
	if got := c.get(context.Background(), time.Time{}); got != id {
		t.Fatalf("expected cached identity, got %v", got)
	}
}

func TestClusterIdentityCache_ServesNilFailureResultWithinRefreshInterval(t *testing.T) {
	c := &clusterIdentityCache{cached: nil, lastAttempt: time.Now()}
	if got := c.get(context.Background(), time.Time{}); got != nil {
		t.Fatalf("expected nil identity, got %v", got)
	}
}

func TestClusterIdentityCache_ServesCachedWhenPostmasterStartTimeUnchanged(t *testing.T) {
	started := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	id := &clusterIdentity{systemIdentifier: "42", timelineID: 1}
	c := &clusterIdentityCache{cached: id, lastAttempt: time.Now(), lastStartTime: started}
	if got := c.get(context.Background(), started); got != id {
		t.Fatalf("expected cached identity, got %v", got)
	}
}

func TestClusterIdentityCache_InvalidatesWhenPostmasterStartTimeChanges(t *testing.T) {
	started := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	c := &clusterIdentityCache{
		cached:        &clusterIdentity{systemIdentifier: "42", timelineID: 1},
		lastAttempt:   time.Now(),
		lastStartTime: started,
	}
	// pool is nil — the refresh query panics, proving the changed start time
	// bypassed the lastAttempt gate instead of serving pre-failover identity.
	defer func() {
		if recover() == nil {
			t.Fatal("expected an immediate refresh attempt, got cached identity")
		}
		if c.cached != nil {
			t.Fatalf("expected cached identity dropped, got %v", c.cached)
		}
	}()
	c.get(context.Background(), started.Add(time.Hour))
}

func TestFormatSystemIdentifier(t *testing.T) {
	if got := formatSystemIdentifier(7294956156579818536); got != "7294956156579818536" {
		t.Fatalf("expected positive value unchanged, got %s", got)
	}
	if got := formatSystemIdentifier(-1); got != "18446744073709551615" {
		t.Fatalf("expected -1 reinterpreted as max uint64, got %s", got)
	}
}
