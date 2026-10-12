//go:build integration

package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// collectUntil ticks the collector until done accepts a payload. Skipped
// (unchanged) ticks are retried.
func collectUntil[P any](t *testing.T, ctx context.Context, c CatalogCollector, done func(P) bool) P {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		if r != nil {
			var p P
			if err := json.Unmarshal(r.JSON, &p); err != nil {
				t.Fatalf("failed to parse payload: %v", err)
			}
			if done(p) {
				return p
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
	var zero P
	return zero
}

func findRunning(p VacuumActivityPayload, relid Oid) *VacuumActivityRunningRow {
	for i := range p.Rows {
		if p.Rows[i].RelID != nil && *p.Rows[i].RelID == relid {
			return &p.Rows[i]
		}
	}
	return nil
}

func findFinished(p VacuumFinishedPayload, relid Oid) *VacuumFinishedRow {
	for i := range p.Rows {
		if p.Rows[i].RelID != nil && *p.Rows[i].RelID == relid {
			return &p.Rows[i]
		}
	}
	return nil
}

func TestVacuumActivity_RunningThenFinished(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		ctx := context.Background()
		for _, sql := range []string{
			`CREATE TABLE va_pinned (id int, payload text)`,
			`INSERT INTO va_pinned SELECT i, repeat('x', 100) FROM generate_series(1, 1000) AS i`,
		} {
			if _, err := inst.admin.Exec(ctx, sql); err != nil {
				t.Fatalf("fixture %q: %v", sql, err)
			}
		}
		var relid Oid
		if err := inst.admin.QueryRow(ctx, `SELECT 'va_pinned'::regclass::oid`).Scan(&relid); err != nil {
			t.Fatalf("relid: %v", err)
		}

		running := VacuumActivityCollector(inst.pool, noopPrepareCtx, inst.version)
		finished := VacuumFinishedCollector(inst.pool, noopPrepareCtx, inst.version)
		// The first tick opens the window; nothing finished before it is reported.
		if _, err := finished.Collect(ctx); err != nil {
			t.Fatalf("Collect() error: %v", err)
		}

		// An open cursor pins the first heap page; VACUUM (FREEZE) must wait
		// for a cleanup lock on it, which holds the vacuum in progress.
		pinner, err := inst.admin.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer pinner.Release()
		for _, sql := range []string{
			`BEGIN`,
			`DECLARE pin CURSOR FOR SELECT * FROM va_pinned`,
			`FETCH 1 FROM pin`,
		} {
			if _, err := pinner.Exec(ctx, sql); err != nil {
				t.Fatalf("pin %q: %v", sql, err)
			}
		}

		vacuumDone := make(chan error, 1)
		go func() {
			_, err := inst.admin.Exec(ctx, `VACUUM (FREEZE) va_pinned`)
			vacuumDone <- err
		}()

		active := collectUntil(t, ctx, running, func(p VacuumActivityPayload) bool {
			return findRunning(p, relid) != nil
		})
		if active.ObservedAt.IsZero() {
			t.Error("expected observed_at")
		}
		row := findRunning(active, relid)
		if row.BackendType == nil || *row.BackendType != "client backend" {
			t.Errorf("backend_type = %v, want client backend", row.BackendType)
		}
		if row.XactStart == nil {
			t.Error("expected xact_start")
		}
		if row.RelName == nil || *row.RelName != "va_pinned" {
			t.Errorf("relname = %v, want va_pinned", row.RelName)
		}
		// A manual VACUUM is never anti-wraparound; whether it analyzes is unknown.
		if row.IsWraparound == nil || *row.IsWraparound {
			t.Errorf("is_wraparound = %v, want false", row.IsWraparound)
		}
		if row.WithAnalyze != nil {
			t.Errorf("with_analyze = %v, want nil", *row.WithAnalyze)
		}

		if _, err := pinner.Exec(ctx, `COMMIT`); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := <-vacuumDone; err != nil {
			t.Fatalf("vacuum: %v", err)
		}

		gone := collectUntil(t, ctx, running, func(p VacuumActivityPayload) bool {
			return findRunning(p, relid) == nil
		})

		// The vacuum left vacuum_activity by gone.ObservedAt, so its completion
		// is in the first vacuum_finished window that reaches that time.
		window := collectUntil(t, ctx, finished, func(p VacuumFinishedPayload) bool {
			return !p.Until.Before(gone.ObservedAt)
		})
		done := findFinished(window, relid)
		if done == nil {
			t.Fatalf("no completion in window [%v, %v) covering %v", window.Since, window.Until, gone.ObservedAt)
		}
		if done.LastVacuum == nil || done.VacuumCount == nil || *done.VacuumCount < 1 {
			t.Fatalf("expected last_vacuum and vacuum_count >= 1, got %v / %v", done.LastVacuum, done.VacuumCount)
		}
		lastVacuum := time.Time(*done.LastVacuum)
		if lastVacuum.Before(window.Since) || lastVacuum.After(gone.ObservedAt) {
			t.Errorf("last_vacuum %v outside [since %v, observed_at %v]", lastVacuum, window.Since, gone.ObservedAt)
		}
		if (done.TotalVacuumTime != nil) != (inst.version >= 18) {
			t.Errorf("total_vacuum_time = %v on PG%d", done.TotalVacuumTime, inst.version)
		}
	})
}

func TestVacuumFinished_ToastReportsParent(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		ctx := context.Background()
		c := VacuumFinishedCollector(inst.pool, noopPrepareCtx, inst.version)
		// The first tick opens the window; nothing finished before it is reported.
		if _, err := c.Collect(ctx); err != nil {
			t.Fatalf("Collect() error: %v", err)
		}

		for _, sql := range []string{
			`CREATE TABLE va_toasted (id int, body text)`,
			`INSERT INTO va_toasted SELECT i, repeat(md5(i::text), 1000) FROM generate_series(1, 50) AS i`,
			`VACUUM va_toasted`,
		} {
			if _, err := inst.admin.Exec(ctx, sql); err != nil {
				t.Fatalf("fixture %q: %v", sql, err)
			}
		}
		var parent, toast Oid
		if err := inst.admin.QueryRow(ctx,
			`SELECT oid, reltoastrelid FROM pg_class WHERE oid = 'va_toasted'::regclass`,
		).Scan(&parent, &toast); err != nil {
			t.Fatalf("relids: %v", err)
		}

		p := collectUntil(t, ctx, c, func(p VacuumFinishedPayload) bool {
			return findFinished(p, toast) != nil && findFinished(p, parent) != nil
		})
		row := findFinished(p, toast)
		if row.ParentRelID == nil || *row.ParentRelID != parent {
			t.Errorf("parent_relid = %v, want %d", row.ParentRelID, parent)
		}
		if row.SchemaName == nil || *row.SchemaName != "pg_toast" {
			t.Errorf("schemaname = %v, want pg_toast", row.SchemaName)
		}
	})
}

func TestVacuumActivity_Flags(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		yes, no := true, false
		cases := []struct {
			name        string
			backendType string
			query       string
			wantWrap    *bool
			wantAnalyze *bool
		}{
			{"autovacuum", "autovacuum worker", "autovacuum: VACUUM public.t", &no, &no},
			{"autovacuum analyze", "autovacuum worker", "autovacuum: VACUUM ANALYZE public.t", &no, &yes},
			{"autovacuum wraparound", "autovacuum worker", "autovacuum: VACUUM public.t (to prevent wraparound)", &yes, &no},
			{"autovacuum analyze wraparound", "autovacuum worker", "autovacuum: VACUUM ANALYZE public.t (to prevent wraparound)", &yes, &yes},
			{"autovacuum on a table named analyze", "autovacuum worker", "autovacuum: VACUUM public.analyze", &no, &no},
			{"autovacuum, query hidden", "autovacuum worker", "<insufficient privilege>", nil, nil},
			{"manual vacuum analyze", "client backend", "VACUUM (ANALYZE) t", &no, nil},
			{"manual analyze false", "client backend", "VACUUM (ANALYZE false) t", &no, nil},
			{"manual, query hidden", "client backend", "<insufficient privilege>", &no, nil},
			{"backend unknown", "", "", nil, nil},
		}
		query := `SELECT ` + vacuumActivityFlags + ` FROM (SELECT NULLIF($1, '') AS backend_type, NULLIF($2, '') AS query) a`
		for _, tc := range cases {
			var gotWrap, gotAnalyze *bool
			if err := inst.pool.QueryRow(context.Background(), query, tc.backendType, tc.query).Scan(&gotWrap, &gotAnalyze); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if fmt.Sprint(nullBool(gotWrap)) != fmt.Sprint(nullBool(tc.wantWrap)) {
				t.Errorf("%s: is_wraparound = %v, want %v", tc.name, nullBool(gotWrap), nullBool(tc.wantWrap))
			}
			if fmt.Sprint(nullBool(gotAnalyze)) != fmt.Sprint(nullBool(tc.wantAnalyze)) {
				t.Errorf("%s: with_analyze = %v, want %v", tc.name, nullBool(gotAnalyze), nullBool(tc.wantAnalyze))
			}
		}
	})
}

// nullBool renders a nullable bool for comparison and messages.
func nullBool(b *bool) any {
	if b == nil {
		return "NULL"
	}
	return *b
}
