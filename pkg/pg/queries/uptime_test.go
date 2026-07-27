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
	if got := c.get(context.Background()); got != id {
		t.Fatalf("expected cached identity, got %v", got)
	}
}

func TestClusterIdentityCache_ServesNilFailureResultWithinRefreshInterval(t *testing.T) {
	c := &clusterIdentityCache{cached: nil, lastAttempt: time.Now()}
	if got := c.get(context.Background()); got != nil {
		t.Fatalf("expected nil identity, got %v", got)
	}
}
