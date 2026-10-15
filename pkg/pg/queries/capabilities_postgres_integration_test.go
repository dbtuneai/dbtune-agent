//go:build integration

package queries

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// postgresStatuses runs the Postgres checker once and returns each
// capability's status by name.
func postgresStatuses(t *testing.T, pool *pgxpool.Pool) map[string]CapabilityStatus {
	t.Helper()
	caps, err := PostgresCapabilityChecker(pool, noopPrepareCtx).Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error: %v", err)
	}
	statuses := make(map[string]CapabilityStatus, len(caps))
	for _, c := range caps {
		statuses[c.Name] = c.Status
	}
	return statuses
}

func TestPostgresCapabilities_PgMonitorAndSuperuser(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()
		var role string
		if err := inst.pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
			t.Fatalf("current_user: %v", err)
		}
		// Guards the harness: collector tests must not run as superuser.
		if role != "monitor" {
			t.Fatalf("inst.pool connects as %q, want monitor", role)
		}
		for name, pool := range map[string]*pgxpool.Pool{"pg_monitor": inst.pool, "superuser": inst.admin} {
			for capability, status := range postgresStatuses(t, pool) {
				if status != CapabilityOK {
					t.Errorf("%s: %s = %s, want ok", name, capability, status)
				}
			}
		}
	})
}

func TestPostgresCapabilities_GrantAndRevoke(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		ctx := context.Background()
		for _, sql := range []string{
			`DROP ROLE IF EXISTS cap_unprivileged`,
			`CREATE ROLE cap_unprivileged LOGIN PASSWORD 'cap_unprivileged'`,
			`GRANT CONNECT ON DATABASE testdb TO cap_unprivileged`,
		} {
			if _, err := inst.admin.Exec(ctx, sql); err != nil {
				t.Fatalf("fixture %q: %v", sql, err)
			}
		}
		cfg := inst.pool.Config().Copy()
		cfg.ConnConfig.User = "cap_unprivileged"
		cfg.ConnConfig.Password = "cap_unprivileged"
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("unprivileged pool: %v", err)
		}
		t.Cleanup(func() {
			pool.Close()
			_, _ = inst.admin.Exec(context.Background(), `DROP ROLE IF EXISTS cap_unprivileged`)
		})

		// Through the collector, so a grant is shown to reach the payload.
		collector := CapabilitiesCollector(PostgresCapabilityChecker(pool, noopPrepareCtx))
		statusOf := func(p *CapabilitiesPayload, name string) CapabilityStatus {
			t.Helper()
			for _, c := range p.Capabilities {
				if c.Name == name {
					return c.Status
				}
			}
			t.Fatalf("payload has no %s: %+v", name, p.Capabilities)
			return ""
		}

		first := collectCapabilities(t, collector)
		if first == nil {
			t.Fatal("first tick sent nothing")
		}
		for _, name := range []string{"postgres.pg_read_all_stats", "postgres.pg_read_all_settings"} {
			if s := statusOf(first, name); s != CapabilityMissing {
				t.Errorf("%s = %s, want missing", name, s)
			}
		}
		if p := collectCapabilities(t, collector); p != nil {
			t.Fatalf("unchanged tick sent %+v", p)
		}

		if _, err := inst.admin.Exec(ctx, `GRANT pg_read_all_stats TO cap_unprivileged`); err != nil {
			t.Fatalf("grant: %v", err)
		}
		granted := collectCapabilities(t, collector)
		if granted == nil {
			t.Fatal("tick after grant sent nothing")
		}
		if s := statusOf(granted, "postgres.pg_read_all_stats"); s != CapabilityOK {
			t.Errorf("after grant pg_read_all_stats = %s, want ok", s)
		}
		if s := statusOf(granted, "postgres.pg_read_all_settings"); s != CapabilityMissing {
			t.Errorf("after grant pg_read_all_settings = %s, want missing", s)
		}

		if _, err := inst.admin.Exec(ctx, `REVOKE pg_read_all_stats FROM cap_unprivileged`); err != nil {
			t.Fatalf("revoke: %v", err)
		}
		revoked := collectCapabilities(t, collector)
		if revoked == nil || statusOf(revoked, "postgres.pg_read_all_stats") != CapabilityMissing {
			t.Fatalf("payload after revoke = %+v, want pg_read_all_stats missing", revoked)
		}
	})
}
