package queries

// https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	VacuumActivityName     = "vacuum_activity"
	VacuumActivityInterval = 5 * time.Second

	// vacuumActivityMinPGVersion applies to vacuum_activity and
	// vacuum_finished. Before PG15 every stats read makes the stats collector
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

// VacuumActivityPayload lists the vacuums running at ObservedAt. Completions
// are reported separately by vacuum_finished; the backend joins the two on
// relid. A vacuum that leaves Rows either finished, and appears in the first
// vacuum_finished window that covers ObservedAt, or was cancelled.
type VacuumActivityPayload struct {
	CollectedAt time.Time `json:"collected_at"`
	// ObservedAt is the server's now(), read after the progress query, so a
	// vacuum missing from Rows had ended by then on the clock that stamps
	// last_vacuum/last_autovacuum.
	ObservedAt time.Time                  `json:"observed_at"`
	Rows       []VacuumActivityRunningRow `json:"rows"`
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
// backend resolves it from pg_class (reltoastrelid) or from vacuum_finished.
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
// including on TOAST and catalog tables. PG15+ only.
func VacuumActivityCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	query := fmt.Sprintf(vacuumActivityRunningQuery, vacuumProgressColumns(pgMajorVersion), vacuumActivityFlags)
	scanner := pgxutil.NewScanner[VacuumActivityRunningRow]()
	tracker := newSkipTracker(skipUnchangedMultiplier)

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
			rows, err := CollectView(func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, query)
			}, VacuumActivityName, scanner)
			if err != nil {
				return nil, err
			}
			var observedAt time.Time
			if err := utils.QueryRowWithPrefix(pool, ctx, `SELECT now()`).Scan(&observedAt); err != nil {
				return nil, fmt.Errorf("failed to read server time for %s: %w", VacuumActivityName, err)
			}

			// Hash rows only: the timestamps change every tick.
			hashed, err := json.Marshal(rows)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s rows for hash: %w", VacuumActivityName, err)
			}
			if tracker.shouldSkip(hashed) {
				return nil, nil
			}

			data, err := json.Marshal(&VacuumActivityPayload{
				CollectedAt: collectedAt,
				ObservedAt:  observedAt.UTC(),
				Rows:        rows,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", VacuumActivityName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
