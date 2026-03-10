package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultCatalogQueryTimeout is the maximum time allowed for a single catalog query.
// This prevents stuck queries (e.g., due to lock contention) from hanging goroutines forever.
const DefaultCatalogQueryTimeout = 30 * time.Second

// ensureTimeout returns a context with DefaultCatalogQueryTimeout if the given context
// has no deadline. If the context already has a deadline, it is returned as-is.
func ensureTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, DefaultCatalogQueryTimeout)
}

// collectView is a generic helper that queries a pg catalog view and scans
// the results into a slice of structs using pgx.RowToStructByNameLax.
// This eliminates boilerplate for the common pattern of query → scan → return.
func collectView[T any](pool *pgxpool.Pool, ctx context.Context, query string, viewName string) ([]T, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pool, ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
}

// pgStatsQuery reads from the pg_stats view which provides a human-readable
// form of pg_statistic. Array columns use format() to stringify anyarray values
// since anyarray cannot be cast to text[] directly.
const pgStatsQuery = `
SELECT
    schemaname,
    tablename,
    attname,
    inherited,
    null_frac,
    avg_width,
    n_distinct,
    CASE WHEN most_common_vals IS NULL THEN NULL
         ELSE pg_catalog.format('%s', most_common_vals) END,
    CASE WHEN most_common_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(most_common_freqs) END,
    CASE WHEN histogram_bounds IS NULL THEN NULL
         ELSE pg_catalog.format('%s', histogram_bounds) END,
    correlation,
    CASE WHEN most_common_elems IS NULL THEN NULL
         ELSE pg_catalog.format('%s', most_common_elems) END,
    CASE WHEN most_common_elem_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(most_common_elem_freqs) END,
    CASE WHEN elem_count_histogram IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(elem_count_histogram) END
FROM pg_stats
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, attname;
`

// pgStatUserTablesQuery reads columns from pg_stat_user_tables with explicit
// type casts for OID and timestamp columns. PG 18+ timing fields use the
// to_jsonb trick so the query works on older versions (returning NULL).
const pgStatUserTablesQuery = `
SELECT
    relid::bigint AS relid, schemaname, relname,
    seq_scan,
    (to_jsonb(t) ->> 'last_seq_scan')::text AS last_seq_scan,
    seq_tup_read, idx_scan,
    (to_jsonb(t) ->> 'last_idx_scan')::text AS last_idx_scan,
    idx_tup_fetch,
    n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
    (to_jsonb(t) ->> 'n_tup_newpage_upd')::bigint AS n_tup_newpage_upd,
    n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
    last_vacuum::text AS last_vacuum,
    last_autovacuum::text AS last_autovacuum,
    last_analyze::text AS last_analyze,
    last_autoanalyze::text AS last_autoanalyze,
    vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
    (to_jsonb(t) ->> 'total_vacuum_time')::float8 AS total_vacuum_time,
    (to_jsonb(t) ->> 'total_autovacuum_time')::float8 AS total_autovacuum_time,
    (to_jsonb(t) ->> 'total_analyze_time')::float8 AS total_analyze_time,
    (to_jsonb(t) ->> 'total_autoanalyze_time')::float8 AS total_autoanalyze_time
FROM pg_stat_user_tables t
ORDER BY schemaname, relname
`

// CollectPgStatistic queries the pg_stats view and returns rows for the backend.
func CollectPgStatistic(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatisticRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stats: %w", err)
	}
	defer rows.Close()

	result := make([]agent.PgStatisticRow, 0)

	for rows.Next() {
		var r agent.PgStatisticRow
		var (
			mostCommonVals      *string
			mostCommonFreqs     *string
			histogramBounds     *string
			mostCommonElems     *string
			mostCommonElemFreqs *string
			elemCountHistogram  *string
		)

		err := rows.Scan(
			&r.SchemaName,
			&r.TableName,
			&r.AttName,
			&r.Inherited,
			&r.NullFrac,
			&r.AvgWidth,
			&r.NDistinct,
			&mostCommonVals,
			&mostCommonFreqs,
			&histogramBounds,
			&r.Correlation,
			&mostCommonElems,
			&mostCommonElemFreqs,
			&elemCountHistogram,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stats row: %w", err)
		}

		r.MostCommonVals = pgArrayToJSON(mostCommonVals)
		r.MostCommonFreqs = toRawJSON(mostCommonFreqs)
		r.HistogramBounds = pgArrayToJSON(histogramBounds)
		r.MostCommonElems = pgArrayToJSON(mostCommonElems)
		r.MostCommonElemFreqs = toRawJSON(mostCommonElemFreqs)
		r.ElemCountHistogram = toRawJSON(elemCountHistogram)

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stats rows: %w", err)
	}

	return result, nil
}

// CollectPgStatUserTables queries pg_stat_user_tables and returns rows for the backend.
func CollectPgStatUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserTableRow, error) {
	return collectView[agent.PgStatUserTableRow](pgPool, ctx, pgStatUserTablesQuery, "pg_stat_user_tables")
}

// pgClassQuery reads reltuples and relpages from pg_class for user tables.
const pgClassQuery = `
SELECT
    n.nspname AS schemaname,
    c.relname,
    c.reltuples,
    c.relpages
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;
`

// CollectPgClass queries pg_class for reltuples and relpages of user tables.
func CollectPgClass(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgClassRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgClassQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_class: %w", err)
	}
	defer rows.Close()

	result := make([]agent.PgClassRow, 0)

	for rows.Next() {
		var r agent.PgClassRow
		err := rows.Scan(
			&r.SchemaName,
			&r.RelName,
			&r.RelTuples,
			&r.RelPages,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_class row: %w", err)
		}
		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_class rows: %w", err)
	}

	return result, nil
}

// --- pg_stat_activity ---
const pgStatActivityQuery = `
SELECT
    datid::bigint AS datid, datname, pid, leader_pid,
    usesysid::bigint AS usesysid, usename,
    application_name,
    client_addr::text AS client_addr,
    client_hostname, client_port,
    backend_start::text AS backend_start,
    xact_start::text AS xact_start,
    query_start::text AS query_start,
    state_change::text AS state_change,
    wait_event_type, wait_event, state,
    backend_xid::text AS backend_xid,
    backend_xmin::text AS backend_xmin,
    (to_jsonb(a) ->> 'query_id')::bigint AS query_id,
    query, backend_type
FROM pg_stat_activity a
`

func CollectPgStatActivity(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatActivityRow, error) {
	return collectView[agent.PgStatActivityRow](pgPool, ctx, pgStatActivityQuery, "pg_stat_activity")
}

// --- pg_stat_database ---
const pgStatDatabaseQuery = `
SELECT
    datid::bigint AS datid, datname, numbackends,
    xact_commit, xact_rollback, blks_read, blks_hit,
    tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
    conflicts, temp_files, temp_bytes, deadlocks,
    checksum_failures,
    checksum_last_failure::text AS checksum_last_failure,
    blk_read_time, blk_write_time,
    (to_jsonb(d) ->> 'session_time')::float8 AS session_time,
    (to_jsonb(d) ->> 'active_time')::float8 AS active_time,
    (to_jsonb(d) ->> 'idle_in_transaction_time')::float8 AS idle_in_transaction_time,
    (to_jsonb(d) ->> 'sessions')::bigint AS sessions,
    (to_jsonb(d) ->> 'sessions_abandoned')::bigint AS sessions_abandoned,
    (to_jsonb(d) ->> 'sessions_fatal')::bigint AS sessions_fatal,
    (to_jsonb(d) ->> 'sessions_killed')::bigint AS sessions_killed,
    stats_reset::text AS stats_reset,
    (to_jsonb(d) ->> 'parallel_workers')::bigint AS parallel_workers,
    (to_jsonb(d) ->> 'temp_bytes_read')::bigint AS temp_bytes_read,
    (to_jsonb(d) ->> 'temp_bytes_written')::bigint AS temp_bytes_written,
    (to_jsonb(d) ->> 'temp_files_read')::bigint AS temp_files_read,
    (to_jsonb(d) ->> 'temp_files_written')::bigint AS temp_files_written
FROM pg_stat_database d
`

func CollectPgStatDatabase(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseRow, error) {
	return collectView[agent.PgStatDatabaseRow](pgPool, ctx, pgStatDatabaseQuery, "pg_stat_database")
}

// --- pg_stat_database_conflicts ---
const pgStatDatabaseConflictsQuery = `
SELECT
    datid::bigint AS datid, datname,
    confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock,
    (to_jsonb(c) ->> 'confl_active_logicalslot')::bigint AS confl_active_logicalslot
FROM pg_stat_database_conflicts c
`

func CollectPgStatDatabaseConflicts(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseConflictsRow, error) {
	return collectView[agent.PgStatDatabaseConflictsRow](pgPool, ctx, pgStatDatabaseConflictsQuery, "pg_stat_database_conflicts")
}

// --- pg_stat_archiver ---
const pgStatArchiverQuery = `
SELECT
    archived_count, last_archived_wal,
    last_archived_time::text AS last_archived_time,
    failed_count, last_failed_wal,
    last_failed_time::text AS last_failed_time,
    stats_reset::text AS stats_reset
FROM pg_stat_archiver
`

func CollectPgStatArchiver(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatArchiverRow, error) {
	return collectView[agent.PgStatArchiverRow](pgPool, ctx, pgStatArchiverQuery, "pg_stat_archiver")
}

// --- pg_stat_bgwriter ---
// Uses to_jsonb trick for columns removed or moved in PG 17 (to pg_stat_checkpointer).
const pgStatBgwriterRawQuery = `
SELECT
    (to_jsonb(b) ->> 'checkpoints_timed')::bigint AS checkpoints_timed,
    (to_jsonb(b) ->> 'checkpoints_req')::bigint AS checkpoints_req,
    (to_jsonb(b) ->> 'checkpoint_write_time')::float8 AS checkpoint_write_time,
    (to_jsonb(b) ->> 'checkpoint_sync_time')::float8 AS checkpoint_sync_time,
    (to_jsonb(b) ->> 'buffers_checkpoint')::bigint AS buffers_checkpoint,
    buffers_clean, maxwritten_clean,
    (to_jsonb(b) ->> 'buffers_backend')::bigint AS buffers_backend,
    (to_jsonb(b) ->> 'buffers_backend_fsync')::bigint AS buffers_backend_fsync,
    buffers_alloc,
    stats_reset::text AS stats_reset
FROM pg_stat_bgwriter b
`

func CollectPgStatBgwriter(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatBgwriterRow, error) {
	return collectView[agent.PgStatBgwriterRow](pgPool, ctx, pgStatBgwriterRawQuery, "pg_stat_bgwriter")
}

// --- pg_stat_checkpointer (PG 17+) ---
const pgStatCheckpointerRawQuery = `
SELECT
    num_timed, num_requested,
    restartpoints_timed, restartpoints_req, restartpoints_done,
    write_time, sync_time, buffers_written,
    stats_reset::text AS stats_reset,
    (to_jsonb(c) ->> 'slru_written')::bigint AS slru_written
FROM pg_stat_checkpointer c
`

func CollectPgStatCheckpointer(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatCheckpointerRow, error) {
	if pgMajorVersion < 17 {
		return nil, nil
	}
	return collectView[agent.PgStatCheckpointerRow](pgPool, ctx, pgStatCheckpointerRawQuery, "pg_stat_checkpointer")
}

// --- pg_stat_wal (PG 14+) ---
// Uses to_jsonb for columns removed in PG 18 (wal_records, wal_fpi, wal_buffers_full,
// wal_write, wal_sync, wal_write_time, wal_sync_time).
const pgStatWalQuery = `
SELECT
    (to_jsonb(w) ->> 'wal_records')::bigint AS wal_records,
    (to_jsonb(w) ->> 'wal_fpi')::bigint AS wal_fpi,
    wal_bytes::bigint AS wal_bytes,
    (to_jsonb(w) ->> 'wal_buffers_full')::bigint AS wal_buffers_full,
    (to_jsonb(w) ->> 'wal_write')::bigint AS wal_write,
    (to_jsonb(w) ->> 'wal_sync')::bigint AS wal_sync,
    (to_jsonb(w) ->> 'wal_write_time')::float8 AS wal_write_time,
    (to_jsonb(w) ->> 'wal_sync_time')::float8 AS wal_sync_time,
    stats_reset::text AS stats_reset
FROM pg_stat_wal w
`

func CollectPgStatWal(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatWalRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return collectView[agent.PgStatWalRow](pgPool, ctx, pgStatWalQuery, "pg_stat_wal")
}

// --- pg_stat_io (PG 16+) ---
// Uses to_jsonb for op_bytes which was removed in PG 18.
const pgStatIOQuery = `
SELECT
    backend_type, object, context,
    reads, read_time, writes, write_time,
    writebacks, writeback_time,
    extends, extend_time,
    (to_jsonb(io) ->> 'op_bytes')::bigint AS op_bytes,
    hits, evictions, reuses,
    fsyncs, fsync_time,
    stats_reset::text AS stats_reset
FROM pg_stat_io io
`

func CollectPgStatIO(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatIORow, error) {
	if pgMajorVersion < 16 {
		return nil, nil
	}
	return collectView[agent.PgStatIORow](pgPool, ctx, pgStatIOQuery, "pg_stat_io")
}

// --- pg_stat_replication ---
const pgStatReplicationQuery = `
SELECT
    pid, usesysid, usename, application_name,
    client_addr::text AS client_addr,
    client_hostname, client_port,
    backend_start::text AS backend_start,
    backend_xmin::text AS backend_xmin,
    state,
    sent_lsn::text AS sent_lsn,
    write_lsn::text AS write_lsn,
    flush_lsn::text AS flush_lsn,
    replay_lsn::text AS replay_lsn,
    write_lag::text AS write_lag,
    flush_lag::text AS flush_lag,
    replay_lag::text AS replay_lag,
    sync_priority, sync_state,
    reply_time::text AS reply_time
FROM pg_stat_replication
`

func CollectPgStatReplication(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatReplicationRow, error) {
	return collectView[agent.PgStatReplicationRow](pgPool, ctx, pgStatReplicationQuery, "pg_stat_replication")
}

// --- pg_stat_replication_slots (PG 14+) ---
const pgStatReplicationSlotsQuery = `
SELECT
    slot_name,
    spill_txns, spill_count, spill_bytes,
    stream_txns, stream_count, stream_bytes,
    total_txns, total_bytes,
    stats_reset::text AS stats_reset
FROM pg_stat_replication_slots
`

func CollectPgStatReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatReplicationSlotsRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	return collectView[agent.PgStatReplicationSlotsRow](pgPool, ctx, pgStatReplicationSlotsQuery, "pg_stat_replication_slots")
}

// --- pg_stat_slru ---
const pgStatSlruQuery = `
SELECT
    name, blks_zeroed, blks_hit, blks_read, blks_written, blks_exists,
    flushes, truncates,
    stats_reset::text AS stats_reset
FROM pg_stat_slru
`

func CollectPgStatSlru(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatSlruRow, error) {
	return collectView[agent.PgStatSlruRow](pgPool, ctx, pgStatSlruQuery, "pg_stat_slru")
}

// --- pg_stat_user_indexes ---
const pgStatUserIndexesQuery = `
SELECT
    relid::bigint AS relid,
    indexrelid::bigint AS indexrelid,
    schemaname, relname, indexrelname,
    idx_scan,
    (to_jsonb(i) ->> 'last_idx_scan')::text AS last_idx_scan,
    idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes i
`

func CollectPgStatUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserIndexesRow, error) {
	return collectView[agent.PgStatUserIndexesRow](pgPool, ctx, pgStatUserIndexesQuery, "pg_stat_user_indexes")
}

// --- pg_statio_user_tables ---
const pgStatioUserTablesQuery = `
SELECT relid::bigint AS relid, schemaname, relname,
    heap_blks_read, heap_blks_hit,
    idx_blks_read, idx_blks_hit,
    toast_blks_read, toast_blks_hit,
    tidx_blks_read, tidx_blks_hit
FROM pg_statio_user_tables
WHERE COALESCE(heap_blks_read,0) + COALESCE(heap_blks_hit,0) +
      COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
ORDER BY COALESCE(heap_blks_read,0) + COALESCE(idx_blks_read,0) DESC
LIMIT 500
`

func CollectPgStatioUserTables(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatioUserTablesRow, error) {
	return collectView[agent.PgStatioUserTablesRow](pgPool, ctx, pgStatioUserTablesQuery, "pg_statio_user_tables")
}

// --- pg_statio_user_indexes ---
const pgStatioUserIndexesQuery = `
SELECT relid::bigint AS relid, indexrelid::bigint AS indexrelid,
    schemaname, relname, indexrelname,
    idx_blks_read, idx_blks_hit
FROM pg_statio_user_indexes
WHERE COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
`

func CollectPgStatioUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatioUserIndexesRow, error) {
	return collectView[agent.PgStatioUserIndexesRow](pgPool, ctx, pgStatioUserIndexesQuery, "pg_statio_user_indexes")
}

// --- pg_stat_user_functions ---
const pgStatUserFunctionsQuery = `
SELECT funcid::bigint AS funcid, schemaname, funcname, calls, total_time, self_time
FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY calls DESC
LIMIT 500
`

func CollectPgStatUserFunctions(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserFunctionsRow, error) {
	return collectView[agent.PgStatUserFunctionsRow](pgPool, ctx, pgStatUserFunctionsQuery, "pg_stat_user_functions")
}

// ParsePgMajorVersion extracts the integer major version from a PG version string like "16.2".
func ParsePgMajorVersion(pgVersion string) int {
	parts := strings.Split(pgVersion, ".")
	if len(parts) == 0 {
		return 0
	}
	v, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0
	}
	return v
}

// toRawJSON converts a nullable JSON string from PG into json.RawMessage.
func toRawJSON(s *string) json.RawMessage {
	if s == nil {
		return nil
	}
	return json.RawMessage(*s)
}

// pgArrayToJSON converts a PG text array literal like {foo,"bar baz",qux}
// into a JSON array like ["foo","bar baz","qux"]. If the value is NULL or
// parsing fails, returns nil.
func pgArrayToJSON(s *string) json.RawMessage {
	if s == nil {
		return nil
	}
	raw := *s

	// Try pgtype text array parser first
	var values []string
	if err := pgtype.NewMap().Scan(pgtype.TextArrayOID, pgtype.TextFormatCode, []byte(raw), &values); err == nil {
		data, err := json.Marshal(values)
		if err != nil {
			return nil
		}
		return json.RawMessage(data)
	}

	// If the PG literal is already valid JSON (shouldn't happen, but be safe)
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw)
	}

	return nil
}

