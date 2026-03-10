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

// pgStatUserTablesQuery reads all columns from pg_stat_user_tables.
// Uses SELECT * so that version-specific columns (e.g. PG 18 timing fields)
// are automatically picked up via RowToStructByNameLax.
const pgStatUserTablesQuery = `SELECT * FROM pg_stat_user_tables ORDER BY schemaname, relname`

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
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatUserTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_user_tables: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatUserTableRow])
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
    datid, datname, pid, leader_pid, usesysid, usename,
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
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatActivityQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_activity: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatActivityRow])
}

// --- pg_stat_database ---
const pgStatDatabaseQuery = `SELECT * FROM pg_stat_database`

func CollectPgStatDatabase(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatDatabaseQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatDatabaseRow])
}

// --- pg_stat_database_conflicts ---
const pgStatDatabaseConflictsQuery = `SELECT * FROM pg_stat_database_conflicts`

func CollectPgStatDatabaseConflicts(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatDatabaseConflictsRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatDatabaseConflictsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database_conflicts: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatDatabaseConflictsRow])
}

// --- pg_stat_archiver ---
const pgStatArchiverQuery = `SELECT * FROM pg_stat_archiver`

func CollectPgStatArchiver(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatArchiverRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatArchiverQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_archiver: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatArchiverRow])
}

// --- pg_stat_bgwriter ---
const pgStatBgwriterRawQuery = `SELECT * FROM pg_stat_bgwriter`

func CollectPgStatBgwriter(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatBgwriterRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatBgwriterRawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_bgwriter: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatBgwriterRow])
}

// --- pg_stat_checkpointer (PG 17+) ---
const pgStatCheckpointerRawQuery = `SELECT * FROM pg_stat_checkpointer`

func CollectPgStatCheckpointer(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatCheckpointerRow, error) {
	if pgMajorVersion < 17 {
		return nil, nil
	}
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatCheckpointerRawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_checkpointer: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatCheckpointerRow])
}

// --- pg_stat_wal (PG 14+) ---
const pgStatWalQuery = `
SELECT
    (to_jsonb(w) ->> 'wal_records')::bigint AS wal_records,
    (to_jsonb(w) ->> 'wal_fpi')::bigint AS wal_fpi,
    wal_bytes::bigint AS wal_bytes,
    (to_jsonb(w) ->> 'wal_buffers_full')::bigint AS wal_buffers_full,
    (to_jsonb(w) ->> 'wal_write')::bigint AS wal_write,
    (to_jsonb(w) ->> 'wal_sync')::bigint AS wal_sync,
    wal_write_time,
    wal_sync_time,
    stats_reset::text AS stats_reset
FROM pg_stat_wal w
`

func CollectPgStatWal(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatWalRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatWalQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_wal: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatWalRow])
}

// --- pg_stat_io (PG 16+) ---
const pgStatIOQuery = `SELECT * FROM pg_stat_io`

func CollectPgStatIO(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatIORow, error) {
	if pgMajorVersion < 16 {
		return nil, nil
	}
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatIOQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_io: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatIORow])
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
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatReplicationQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_replication: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatReplicationRow])
}

// --- pg_stat_replication_slots (PG 14+) ---
const pgStatReplicationSlotsQuery = `SELECT * FROM pg_stat_replication_slots`

func CollectPgStatReplicationSlots(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]agent.PgStatReplicationSlotsRow, error) {
	if pgMajorVersion < 14 {
		return nil, nil
	}
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatReplicationSlotsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_replication_slots: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatReplicationSlotsRow])
}

// --- pg_stat_slru ---
const pgStatSlruQuery = `SELECT * FROM pg_stat_slru`

func CollectPgStatSlru(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatSlruRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatSlruQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_slru: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatSlruRow])
}

// --- pg_stat_user_indexes ---
const pgStatUserIndexesQuery = `SELECT * FROM pg_stat_user_indexes`

func CollectPgStatUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserIndexesRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatUserIndexesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_user_indexes: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatUserIndexesRow])
}

// --- pg_statio_user_tables ---
const pgStatioUserTablesQuery = `
SELECT relid, schemaname, relname,
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
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatioUserTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_statio_user_tables: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatioUserTablesRow])
}

// --- pg_statio_user_indexes ---
const pgStatioUserIndexesQuery = `
SELECT relid, indexrelid, schemaname, relname, indexrelname,
    idx_blks_read, idx_blks_hit
FROM pg_statio_user_indexes
WHERE COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0
`

func CollectPgStatioUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatioUserIndexesRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatioUserIndexesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_statio_user_indexes: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatioUserIndexesRow])
}

// --- pg_stat_user_functions ---
const pgStatUserFunctionsQuery = `
SELECT funcid, schemaname, funcname, calls, total_time, self_time
FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY calls DESC
LIMIT 500
`

func CollectPgStatUserFunctions(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatUserFunctionsRow, error) {
	ctx, cancel := ensureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatUserFunctionsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_user_functions: %w", err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[agent.PgStatUserFunctionsRow])
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

