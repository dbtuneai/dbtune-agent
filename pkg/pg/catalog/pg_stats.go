package catalog

// https://www.postgresql.org/docs/current/view-pg-stats.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatsRow represents a single row from the pg_stats view,
// matching the backend's PgStats Django model.
type PgStatsRow struct {
	SchemaName          Name        `json:"schemaname"`
	TableName           Name        `json:"tablename"`
	AttName             Name        `json:"attname"`
	Inherited           Boolean     `json:"inherited"`
	NullFrac            *Real       `json:"null_frac"`
	AvgWidth            *Integer    `json:"avg_width"`
	NDistinct           *Real       `json:"n_distinct"`
	MostCommonVals      Anyarray    `json:"most_common_vals"`
	MostCommonFreqs     Float4Array `json:"most_common_freqs"`
	HistogramBounds     Anyarray    `json:"histogram_bounds"`
	Correlation         *Real       `json:"correlation"`
	MostCommonElems     Anyarray    `json:"most_common_elems"`
	MostCommonElemFreqs Float4Array `json:"most_common_elem_freqs"`
	ElemCountHistogram  Float4Array `json:"elem_count_histogram"`
}

// PgStatsPayload is the JSON body POSTed to /api/v1/agent/pg_stats.
type PgStatsPayload struct {
	Rows []PgStatsRow `json:"rows"`
}

const (
	PgStatsName     = "pg_stats"
	PgStatsInterval = 1 * time.Minute

	// PgStatsBackfillBatchSize controls how many tables' statistics are sent per
	// tick during the initial backfill phase. This bounds memory usage on large
	// databases (e.g. 10k+ tables) by spreading the first full snapshot across
	// multiple ticks instead of sending everything at once. After all batches
	// are sent, the collector switches to delta mode using last_analyze timestamps.
	PgStatsBackfillBatchSize = 200
)

// pgStatsColumns is the SELECT list shared by the full and delta queries.
// Array columns use format() to stringify anyarray values since anyarray
// cannot be cast to text[] directly.
const pgStatsColumns = `
    ps.schemaname,
    ps.tablename,
    ps.attname,
    ps.inherited,
    ps.null_frac,
    ps.avg_width,
    ps.n_distinct,
    CASE WHEN ps.most_common_vals IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.most_common_vals) END,
    CASE WHEN ps.most_common_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.most_common_freqs) END,
    CASE WHEN ps.histogram_bounds IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.histogram_bounds) END,
    ps.correlation,
    CASE WHEN ps.most_common_elems IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.most_common_elems) END,
    CASE WHEN ps.most_common_elem_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.most_common_elem_freqs) END,
    CASE WHEN ps.elem_count_histogram IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.elem_count_histogram) END`

// pgStatsQueryBatch fetches stats for a batch of tables during the initial
// backfill phase. The subquery paginates through pg_stat_user_tables using
// LIMIT/OFFSET ($1=batch size, $2=offset) to bound memory per tick.
const pgStatsQueryBatch = `SELECT` + pgStatsColumns + `
FROM pg_stats ps
JOIN (
    SELECT schemaname, relname
    FROM pg_stat_user_tables
    ORDER BY schemaname, relname
    LIMIT $1 OFFSET $2
) batch ON ps.schemaname = batch.schemaname
       AND ps.tablename  = batch.relname
ORDER BY ps.schemaname, ps.tablename, ps.attname
`

// pgStatsQueryDelta returns stats only for tables that have been (auto-)analyzed
// since $1. The JOIN against pg_stat_user_tables implicitly excludes system schemas.
const pgStatsQueryDelta = `SELECT` + pgStatsColumns + `
FROM pg_stats ps
JOIN pg_stat_user_tables pst
  ON ps.schemaname = pst.schemaname
 AND ps.tablename  = pst.relname
WHERE GREATEST(pst.last_analyze, pst.last_autoanalyze) > $1
ORDER BY ps.schemaname, ps.tablename, ps.attname
`

// PgStatsQueryMode specifies which pg_stats query mode to use.
type PgStatsQueryMode struct {
	// Backfill mode: paginate through all tables.
	batchSize int
	offset    int
	// Delta mode: only tables analyzed after this time.
	since *time.Time
}

// PgStatsBackfillQuery creates a query mode for batch backfill.
func PgStatsBackfillQuery(batchSize, offset int) PgStatsQueryMode {
	return PgStatsQueryMode{batchSize: batchSize, offset: offset}
}

// PgStatsDeltaQuery creates a query mode for delta updates since a given time.
func PgStatsDeltaQuery(since time.Time) PgStatsQueryMode {
	return PgStatsQueryMode{since: &since}
}

// CollectPgStats queries the pg_stats view and returns rows for the backend.
// This uses custom scanning because pg_stats has anyarray columns that cannot
// be directly scanned by pgx.RowToStructByNameLax.
func CollectPgStats(pgPool *pgxpool.Pool, ctx context.Context, q PgStatsQueryMode) ([]PgStatsRow, error) {
	ctx, cancel := EnsureTimeout(ctx)
	defer cancel()

	var (
		rows pgx.Rows
		err  error
	)
	if q.since != nil {
		rows, err = utils.QueryWithPrefix(pgPool, ctx, pgStatsQueryDelta, *q.since)
	} else {
		rows, err = utils.QueryWithPrefix(pgPool, ctx, pgStatsQueryBatch, q.batchSize, q.offset)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stats: %w", err)
	}
	defer rows.Close()

	result := make([]PgStatsRow, 0)

	for rows.Next() {
		var r PgStatsRow
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

		if r.MostCommonVals, err = pgArrayToJSON(mostCommonVals); err != nil {
			return nil, fmt.Errorf("pg_stats %s.%s.%s most_common_vals: %w", r.SchemaName, r.TableName, r.AttName, err)
		}
		r.MostCommonFreqs = toRawJSON(mostCommonFreqs)
		if r.HistogramBounds, err = pgArrayToJSON(histogramBounds); err != nil {
			return nil, fmt.Errorf("pg_stats %s.%s.%s histogram_bounds: %w", r.SchemaName, r.TableName, r.AttName, err)
		}
		if r.MostCommonElems, err = pgArrayToJSON(mostCommonElems); err != nil {
			return nil, fmt.Errorf("pg_stats %s.%s.%s most_common_elems: %w", r.SchemaName, r.TableName, r.AttName, err)
		}
		r.MostCommonElemFreqs = toRawJSON(mostCommonElemFreqs)
		r.ElemCountHistogram = toRawJSON(elemCountHistogram)

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stats rows: %w", err)
	}

	return result, nil
}

// toRawJSON converts a nullable JSON string from PG into json.RawMessage.
func toRawJSON(s *string) json.RawMessage {
	if s == nil {
		return nil
	}
	return json.RawMessage(*s)
}

// pgtypeMap is reused across calls to pgArrayToJSON to avoid per-row allocation.
var pgtypeMap = pgtype.NewMap()

// pgArrayToJSON converts a PG text array literal like {foo,"bar baz",qux}
// into a JSON array like ["foo","bar baz","qux"]. If the value is NULL,
// returns (nil, nil). If parsing fails, returns an error.
func pgArrayToJSON(s *string) (json.RawMessage, error) {
	if s == nil {
		return nil, nil
	}
	raw := *s

	// Try pgtype text array parser first
	var values []string
	if err := pgtypeMap.Scan(pgtype.TextArrayOID, pgtype.TextFormatCode, []byte(raw), &values); err == nil {
		data, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal pg array to JSON: %w", err)
		}
		return json.RawMessage(data), nil
	}

	// If the PG literal is already valid JSON (shouldn't happen, but be safe)
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw), nil
	}

	return nil, fmt.Errorf("failed to parse pg array: %q", raw)
}

// PgStatsCollectFunc is the function signature for querying pg_stats.
// Injected into the collector to allow testing without a real database.
type PgStatsCollectFunc func(ctx context.Context, q PgStatsQueryMode) ([]PgStatsRow, error)

// newPgStatsCollectFunc builds the collect closure that implements the
// backfill → delta state machine. Extracted for testability.
func newPgStatsCollectFunc(collectFn PgStatsCollectFunc, batchSize int) func(ctx context.Context) (any, error) {
	var (
		backfillOffset = 0        // advances by batchSize each tick
		backfillDone   = false    // true once a batch returns 0 rows
		lastPoll       *time.Time // set after backfill completes, used for delta queries
	)
	return func(ctx context.Context) (any, error) {
		var (
			statsRows []PgStatsRow
			err       error
		)

		if !backfillDone {
			// Backfill phase: paginate through all tables in batches.
			statsRows, err = collectFn(ctx, PgStatsBackfillQuery(batchSize, backfillOffset))
			if err != nil {
				return nil, err
			}
			if len(statsRows) == 0 {
				// OFFSET past all tables — switch to delta mode.
				backfillDone = true
				now := time.Now().UTC()
				lastPoll = &now
			} else {
				backfillOffset += batchSize
			}
		} else {
			// Delta phase: only tables analyzed since last poll.
			now := time.Now().UTC()
			statsRows, err = collectFn(ctx, PgStatsDeltaQuery(*lastPoll))
			if err != nil {
				return nil, err
			}
			lastPoll = &now
		}

		if len(statsRows) == 0 {
			return nil, nil
		}
		return &PgStatsPayload{Rows: statsRows}, nil
	}
}

func NewPgStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	collectFn := func(ctx context.Context, q PgStatsQueryMode) ([]PgStatsRow, error) {
		ctx, err := prepareCtx(ctx)
		if err != nil {
			return nil, err
		}
		return CollectPgStats(pool, ctx, q)
	}
	return agent.CatalogCollector{
		Name:     PgStatsName,
		Interval: PgStatsInterval,
		Collect:  newPgStatsCollectFunc(collectFn, PgStatsBackfillBatchSize),
	}
}
