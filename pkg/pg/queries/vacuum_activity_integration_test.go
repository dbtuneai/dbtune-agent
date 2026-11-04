//go:build integration

package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
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
	for i := range p.Running {
		if p.Running[i].RelID != nil && *p.Running[i].RelID == relid {
			return &p.Running[i]
		}
	}
	return nil
}

func findFinished(p VacuumActivityPayload, relid Oid) *VacuumFinishedRow {
	if p.Finished == nil {
		return nil
	}
	for i := range p.Finished.Rows {
		if p.Finished.Rows[i].RelID != nil && *p.Finished.Rows[i].RelID == relid {
			return &p.Finished.Rows[i]
		}
	}
	return nil
}

// startPinnedVacuum starts a VACUUM (FREEZE) of table and holds it in progress:
// an open cursor pins the first heap page, and the vacuum must wait for a
// cleanup lock on it. The returned func releases the pin and waits for the
// vacuum to finish.
func startPinnedVacuum(t *testing.T, ctx context.Context, inst pgInstance, table string) func() {
	t.Helper()
	pinner, err := inst.admin.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(pinner.Release)
	for _, sql := range []string{
		`BEGIN`,
		`DECLARE pin CURSOR FOR SELECT * FROM ` + table,
		`FETCH 1 FROM pin`,
	} {
		if _, err := pinner.Exec(ctx, sql); err != nil {
			t.Fatalf("pin %q: %v", sql, err)
		}
	}

	vacuumDone := make(chan error, 1)
	go func() {
		_, err := inst.admin.Exec(ctx, `VACUUM (FREEZE) `+table)
		vacuumDone <- err
	}()

	return func() {
		t.Helper()
		if _, err := pinner.Exec(ctx, `COMMIT`); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := <-vacuumDone; err != nil {
			t.Fatalf("vacuum: %v", err)
		}
	}
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

		// A window every tick, so the one after the vacuum ends is immediate.
		c := newVacuumActivityCollector(inst.pool, noopPrepareCtx, inst.version, logrus.New(), 0)
		// The first tick opens the window; nothing finished before it is reported.
		first, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		var opened VacuumActivityPayload
		if err := json.Unmarshal(first.JSON, &opened); err != nil {
			t.Fatalf("failed to parse payload: %v", err)
		}
		if opened.Finished == nil || !opened.Finished.WindowEnd.Equal(opened.ObservedAt) {
			t.Fatalf("first payload should open a window ending at observed_at, got %+v", opened.Finished)
		}

		finishVacuum := startPinnedVacuum(t, ctx, inst, "va_pinned")

		active := collectUntil(t, ctx, c, func(p VacuumActivityPayload) bool {
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
		if row.NDeadTup == nil {
			t.Error("expected n_dead_tup")
		}
		// track_cost_delay_timing is off by default, so nothing was measured.
		if row.DelayTime != nil {
			t.Errorf("delay_time = %v, want nil", *row.DelayTime)
		}

		finishVacuum()

		// The vacuum is stamped before it leaves the progress view, so the
		// window ending at the tick that no longer sees it has its completion.
		gone := collectUntil(t, ctx, c, func(p VacuumActivityPayload) bool {
			return findRunning(p, relid) == nil
		})
		if gone.Finished == nil {
			t.Fatal("payload where the vacuum ended has no finished window")
		}
		if !gone.Finished.WindowEnd.Equal(gone.ObservedAt) {
			t.Errorf("window_end %v != observed_at %v", gone.Finished.WindowEnd, gone.ObservedAt)
		}
		if !gone.Finished.WindowStart.Before(active.ObservedAt) {
			t.Errorf("window_start %v does not reach back before the last sighting %v", gone.Finished.WindowStart, active.ObservedAt)
		}
		done := findFinished(gone, relid)
		if done == nil {
			t.Fatalf("no completion in window [%v, %v)", gone.Finished.WindowStart, gone.Finished.WindowEnd)
		}
		if done.LastVacuum == nil {
			t.Fatal("expected last_vacuum")
		}
		lastVacuum := time.Time(*done.LastVacuum)
		if lastVacuum.Before(gone.Finished.WindowStart) || lastVacuum.After(gone.ObservedAt) {
			t.Errorf("last_vacuum %v outside [window_start %v, observed_at %v]", lastVacuum, gone.Finished.WindowStart, gone.ObservedAt)
		}
		if (done.RelAllFrozen != nil) != (inst.version >= 18) {
			t.Errorf("relallfrozen = %v on PG%d", done.RelAllFrozen, inst.version)
		}
	})
}

func TestVacuumActivity_ToastReportsParent(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		ctx := context.Background()
		// A window every tick: this vacuum is too short to be seen running.
		c := newVacuumActivityCollector(inst.pool, noopPrepareCtx, inst.version, logrus.New(), 0)
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

		p := collectUntil(t, ctx, c, func(p VacuumActivityPayload) bool {
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

func TestVacuumActivity_FinishedIntervalAndUnchangedSkip(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		ctx := context.Background()
		for _, sql := range []string{
			`CREATE TABLE va_gated (id int, payload text)`,
			`INSERT INTO va_gated SELECT i, repeat('x', 100) FROM generate_series(1, 1000) AS i`,
		} {
			if _, err := inst.admin.Exec(ctx, sql); err != nil {
				t.Fatalf("fixture %q: %v", sql, err)
			}
		}
		var relid Oid
		if err := inst.admin.QueryRow(ctx, `SELECT 'va_gated'::regclass::oid`).Scan(&relid); err != nil {
			t.Fatalf("relid: %v", err)
		}

		// Only the first tick is due a window during the test.
		c := newVacuumActivityCollector(inst.pool, noopPrepareCtx, inst.version, logrus.New(), time.Hour)
		first, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect() error: %v", err)
		}
		var opened VacuumActivityPayload
		if err := json.Unmarshal(first.JSON, &opened); err != nil {
			t.Fatalf("failed to parse payload: %v", err)
		}
		if opened.Finished == nil {
			t.Fatal("first payload should open a window")
		}

		// A change in running vacuums is sent without waiting for a window.
		finishVacuum := startPinnedVacuum(t, ctx, inst, "va_gated")
		active := collectUntil(t, ctx, c, func(p VacuumActivityPayload) bool {
			return findRunning(p, relid) != nil
		})
		if active.Finished != nil {
			t.Errorf("window sent before the interval elapsed: %+v", active.Finished)
		}

		finishVacuum()
		gone := collectUntil(t, ctx, c, func(p VacuumActivityPayload) bool {
			return findRunning(p, relid) == nil
		})
		if gone.Finished != nil {
			t.Errorf("window sent before the interval elapsed: %+v", gone.Finished)
		}

		// With no window due, an unchanged running set is not sent. Autovacuum
		// can change it between ticks, so retry until two ticks match.
		for i := 0; ; i++ {
			r, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("Collect() error: %v", err)
			}
			if r == nil {
				break
			}
			var p VacuumActivityPayload
			if err := json.Unmarshal(r.JSON, &p); err != nil {
				t.Fatalf("failed to parse payload: %v", err)
			}
			if p.Finished != nil {
				t.Errorf("window sent before the interval elapsed: %+v", p.Finished)
			}
			if i == 20 {
				t.Fatal("unchanged running vacuums were sent on every tick")
			}
			time.Sleep(250 * time.Millisecond)
		}
	})
}

func TestVacuumActivity_FinishedQueryFails(t *testing.T) {
	forEachPG(t, func(t *testing.T, inst pgInstance) {
		if inst.version < vacuumActivityMinPGVersion {
			t.Skipf("vacuum collectors require PG%d+", vacuumActivityMinPGVersion)
		}
		ctx := context.Background()
		// pg_stat_get_last_vacuum_time is only called by the finished query.
		// A function of the same name earlier in the collector's search_path
		// fails it while va_fail.flag is set, leaving the running query alone.
		for _, sql := range []string{
			`CREATE SCHEMA va_fail`,
			`CREATE TABLE va_fail.flag (fail boolean NOT NULL)`,
			`INSERT INTO va_fail.flag VALUES (true)`,
			`CREATE FUNCTION va_fail.pg_stat_get_last_vacuum_time(oid) RETURNS timestamptz
				LANGUAGE plpgsql AS $$
				BEGIN
					IF (SELECT fail FROM va_fail.flag) THEN
						RAISE EXCEPTION 'finished query failed on purpose';
					END IF;
					RETURN pg_catalog.pg_stat_get_last_vacuum_time($1);
				END $$`,
			`GRANT USAGE ON SCHEMA va_fail TO monitor`,
			`GRANT SELECT ON va_fail.flag TO monitor`,
		} {
			if _, err := inst.admin.Exec(ctx, sql); err != nil {
				t.Fatalf("fixture %q: %v", sql, err)
			}
		}
		setFail := func(fail bool) {
			t.Helper()
			if _, err := inst.admin.Exec(ctx, `UPDATE va_fail.flag SET fail = $1`, fail); err != nil {
				t.Fatalf("set flag: %v", err)
			}
		}

		cfg := inst.pool.Config()
		cfg.ConnConfig.RuntimeParams["search_path"] = "va_fail, pg_catalog, public"
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("pool: %v", err)
		}
		defer pool.Close()

		collect := func(c CatalogCollector) *VacuumActivityPayload {
			t.Helper()
			r, err := c.Collect(ctx)
			if err != nil {
				t.Fatalf("Collect() error: %v", err)
			}
			if r == nil {
				return nil
			}
			var p VacuumActivityPayload
			if err := json.Unmarshal(r.JSON, &p); err != nil {
				t.Fatalf("failed to parse payload: %v", err)
			}
			return &p
		}

		c := newVacuumActivityCollector(pool, noopPrepareCtx, inst.version, logrus.New(), 0)

		// The running half is still sent when the finished query fails.
		if p := collect(c); p == nil {
			t.Fatal("first tick sent nothing; want the running half")
		} else if p.Finished != nil {
			t.Fatalf("finished window sent although its query failed: %+v", p.Finished)
		}

		setFail(false)
		opened := collect(c)
		if opened == nil || opened.Finished == nil {
			t.Fatal("expected a window once the finished query succeeds")
		}

		// A failed window does not advance the next window's start.
		setFail(true)
		if p := collect(c); p != nil && p.Finished != nil {
			t.Fatalf("finished window sent although its query failed: %+v", p.Finished)
		}
		setFail(false)
		retried := collect(c)
		if retried == nil || retried.Finished == nil {
			t.Fatal("expected a window once the finished query succeeds again")
		}
		wantStart := opened.Finished.WindowEnd.Add(-vacuumFinishedDataOverlap)
		if !retried.Finished.WindowStart.Equal(wantStart) {
			t.Errorf("window_start = %v, want %v (previous window_end - overlap)", retried.Finished.WindowStart, wantStart)
		}
	})
}
