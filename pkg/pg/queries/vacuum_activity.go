package queries

// https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	VacuumActivityName     = "vacuum_activity"
	VacuumActivityInterval = 5 * time.Second

	// vacuumFinishedInterval is how often a finished window is sent. Each
	// window reads the vacuum stats of every relation, so it runs on a slower
	// cadence than the progress view.
	vacuumFinishedInterval = 1 * time.Minute

	// vacuumFinishedDataOverlap starts each window slightly before the
	// previous one ended. last_vacuum/last_autovacuum are stamped just before
	// being written to shared stats, so a vacuum stamped before the previous
	// WindowEnd can become visible after it was read.
	vacuumFinishedDataOverlap = 10 * time.Second

	// vacuumActivityMinPGVersion: before PG15 every stats read makes the stats collector
	// rewrite the database's stats file, and completions are published
	// asynchronously, so the finished window can't be trusted.
	vacuumActivityMinPGVersion = 15
)

// VacuumActivityRunningRow is a pg_stat_progress_vacuum row joined to the
// vacuuming backend in pg_stat_activity and the relation in pg_class.
type VacuumActivityRunningRow struct {
	PID              *Integer         `json:"pid" db:"pid"`
	RelID            *Oid             `json:"relid" db:"relid"`
	SchemaName       *Name            `json:"schemaname" db:"schemaname"`
	RelName          *Name            `json:"relname" db:"relname"`
	Phase            *Text            `json:"phase" db:"phase"`
	HeapBlksTotal    *Bigint          `json:"heap_blks_total" db:"heap_blks_total"`
	HeapBlksScanned  *Bigint          `json:"heap_blks_scanned" db:"heap_blks_scanned"`
	HeapBlksVacuumed *Bigint          `json:"heap_blks_vacuumed" db:"heap_blks_vacuumed"`
	IndexVacuumCount *Bigint          `json:"index_vacuum_count" db:"index_vacuum_count"`
	IndexesTotal     *Bigint          `json:"indexes_total" db:"indexes_total"`
	MaxDeadTuples    *Bigint          `json:"max_dead_tuples" db:"max_dead_tuples"`
	NumDeadTuples    *Bigint          `json:"num_dead_tuples" db:"num_dead_tuples"`
	NumDeadItemIDs   *Bigint          `json:"num_dead_item_ids" db:"num_dead_item_ids"`
	DelayTime        *DoublePrecision `json:"delay_time" db:"delay_time"`
	BackendType      *Text            `json:"backend_type" db:"backend_type"`
	XactStart        *TimestampTZ     `json:"xact_start" db:"xact_start"`
	WaitEventType    *Text            `json:"wait_event_type" db:"wait_event_type"`
	WaitEvent        *Text            `json:"wait_event" db:"wait_event"`
	// IsWraparound and WithAnalyze are nil when unknown; see vacuumActivityFlags.
	IsWraparound *Boolean `json:"is_wraparound" db:"is_wraparound"`
	WithAnalyze  *Boolean `json:"with_analyze" db:"with_analyze"`
}

// VacuumActivityPayload lists the vacuums running at ObservedAt and, on some
// ticks, the relations whose vacuum finished in a window ending at the same
// instant. Running is read first, so a vacuum missing from it had finished or
// been cancelled by ObservedAt, and Postgres stamps last_vacuum/last_autovacuum
// before a vacuum leaves pg_stat_progress_vacuum: its completion, if any, is
// in the first window ending at or after ObservedAt, or an earlier one.
type VacuumActivityPayload struct {
	// Agent clock, given by every collector (not currently used)
	CollectedAt time.Time `json:"collected_at"`
	// The postgres clock, used in the vacuum logic.
	ObservedAt time.Time                  `json:"observed_at"`
	Running    []VacuumActivityRunningRow `json:"running"`
	// Finished is nil on ticks without a window: one is sent on the first
	// tick and then every vacuumFinishedInterval.
	Finished *VacuumFinishedWindow `json:"finished"`
}

// VacuumFinishedWindow holds every relation with a vacuum that finished in
// [WindowStart, WindowEnd), plus possibly some that finished just after
// WindowEnd. WindowEnd equals the payload's ObservedAt. Each window starts
// vacuumFinishedDataOverlap before the previous one ended, so completions may
// repeat; the backend dedupes. A window whose payload failed to send is not
// resent: the backend sees the gap as a WindowStart later than the last
// WindowEnd it received.
type VacuumFinishedWindow struct {
	WindowStart time.Time           `json:"window_start"`
	WindowEnd   time.Time           `json:"window_end"`
	Rows        []VacuumFinishedRow `json:"rows"`
}

// vacuumActivityFlags derives is_wraparound and with_analyze from the vacuuming
// backend's query text, which only has a fixed format for autovacuum workers:
// "autovacuum: VACUUM[ ANALYZE] schema.table[ (to prevent wraparound)]".
// A manual VACUUM is never an anti-wraparound vacuum, but whether it analyzes
// can't be read reliably from free-form SQL, so with_analyze is NULL for it.
// Both are NULL when the agent's role can't see the query text (no
// pg_read_all_stats: "<insufficient privilege>").
const vacuumActivityFlags = `CASE
		WHEN a.backend_type = 'autovacuum worker' AND a.query LIKE 'autovacuum: %'
			THEN a.query LIKE '% (to prevent wraparound)'
		WHEN a.backend_type = 'client backend' THEN false
	END AS is_wraparound,
	CASE
		WHEN a.backend_type = 'autovacuum worker' AND a.query LIKE 'autovacuum: %'
			THEN a.query LIKE 'autovacuum: VACUUM ANALYZE %'
	END AS with_analyze`

// The owning table of a TOAST relation is not looked up here: reltoastrelid is
// not indexed, so it would scan all of pg_class every tick a vacuum runs. The
// backend resolves it from pg_class (reltoastrelid) or from the finished rows.
//
// %s: the version-dependent progress columns, then vacuumActivityFlags.
const vacuumActivityRunningQuery = `SELECT
	p.pid,
	p.relid,
	n.nspname AS schemaname,
	c.relname,
	p.phase,
	p.heap_blks_total,
	p.heap_blks_scanned,
	p.heap_blks_vacuumed,
	p.index_vacuum_count,
	%s,
	a.backend_type,
	a.xact_start,
	a.wait_event_type,
	a.wait_event,
	%s
FROM pg_stat_progress_vacuum p
LEFT JOIN pg_stat_activity a ON a.pid = p.pid
LEFT JOIN pg_class c ON c.oid = p.relid
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE p.datname = current_database()`

// vacuumProgressColumns covers the PG17 rename of the dead-tuple columns
// (max_dead_tuples -> max_dead_tuple_bytes, num_dead_tuples ->
// num_dead_item_ids) and PG18's delay_time.
func vacuumProgressColumns(pgMajorVersion int) string {
	switch {
	case pgMajorVersion >= 18:
		return `p.indexes_total, NULL::bigint AS max_dead_tuples, NULL::bigint AS num_dead_tuples,
	p.num_dead_item_ids, p.delay_time`
	case pgMajorVersion >= 17:
		return `p.indexes_total, NULL::bigint AS max_dead_tuples, NULL::bigint AS num_dead_tuples,
	p.num_dead_item_ids, NULL::double precision AS delay_time`
	default:
		return `NULL::bigint AS indexes_total, p.max_dead_tuples, p.num_dead_tuples,
	NULL::bigint AS num_dead_item_ids, NULL::double precision AS delay_time`
	}
}

// VacuumActivityCollector reports the vacuums running in the current database,
// including on TOAST and catalog tables, and the relations whose vacuum
// finished since the previous window. PG15+ only.
//
// Both halves are read in one tick and sent in one ordered stream, so the
// backend can settle every run from the payloads alone. Window bounds are the
// server's now(), so agent clock skew cannot drop or replay completions.
func VacuumActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	return newVacuumActivityCollector(pool, prepareCtx, pgMajorVersion, vacuumFinishedInterval)
}

func newVacuumActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int, finishedInterval time.Duration) CatalogCollector {
	runningQuery := fmt.Sprintf(vacuumActivityRunningQuery, vacuumProgressColumns(pgMajorVersion), vacuumActivityFlags)
	finishedQuery := fmt.Sprintf(vacuumFinishedQuery, vacuumFinishedColumns(pgMajorVersion), vacuumFinishedParentJoin)
	runningScanner := pgxutil.NewScanner[VacuumActivityRunningRow]()
	finishedScanner := pgxutil.NewScanner[VacuumFinishedRow]()
	var prevRunningHash uint64
	var prevWindowEnd time.Time

	return CatalogCollector{
		Name:     VacuumActivityName,
		Interval: VacuumActivityInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			if pgMajorVersion < vacuumActivityMinPGVersion {
				return nil, nil
			}
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			collectedAt := time.Now().UTC()

			// get currently running vaccums every 5s
			running, err := CollectView(func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, runningQuery)
			}, VacuumActivityName, runningScanner)
			if err != nil {
				return nil, err
			}
			var observedAt time.Time
			if err := utils.QueryRowWithPrefix(pool, ctx, `SELECT now()`).Scan(&observedAt); err != nil {
				return nil, fmt.Errorf("failed to read server time for %s: %w", VacuumActivityName, err)
			}

			// get list of finished vacuums every 60s
			var finished *VacuumFinishedWindow
			if prevWindowEnd.IsZero() || observedAt.Sub(prevWindowEnd) >= finishedInterval {
				// window start is just used to select which rows to include
				// the data sent is cumulative anyways
				windowStart := prevWindowEnd
				if windowStart.IsZero() {
					windowStart = observedAt
				}
				windowStart = windowStart.Add(-vacuumFinishedDataOverlap)
				rows, err := CollectView(func() (pgx.Rows, error) {
					return utils.QueryWithPrefix(pool, ctx, finishedQuery, windowStart)
				}, VacuumActivityName, finishedScanner)
				if err != nil {
					return nil, err
				}
				finished = &VacuumFinishedWindow{WindowStart: windowStart.UTC(), WindowEnd: observedAt.UTC(), Rows: rows}
				prevWindowEnd = observedAt
			}

			// Skip sending running if nothing has changed.
			// Hash running rows only: the timestamps change every tick.
			hashed, err := json.Marshal(running)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s rows for hash: %w", VacuumActivityName, err)
			}
			runningHash := xxhash.Sum64(hashed)
			if finished == nil && runningHash == prevRunningHash {
				return nil, nil
			}
			prevRunningHash = runningHash

			data, err := json.Marshal(&VacuumActivityPayload{
				CollectedAt: collectedAt,
				ObservedAt:  observedAt.UTC(),
				Running:     running,
				Finished:    finished,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", VacuumActivityName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
