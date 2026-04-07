package queries

// https://www.postgresql.org/docs/current/view-pg-stats.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatsRow represents a single row from the pg_stats view.
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

const (
	PgStatsName     = "pg_stats"
	PgStatsInterval = 1 * time.Minute

	// PgStatsBackfillBatchSize controls how many tables' statistics are sent per
	// tick during the initial backfill phase.
	PgStatsBackfillBatchSize = 200
)

// buildPgStatsColumns returns the SELECT list for pg_stats queries.
// When includeTableData is false, columns containing actual data values
// are NULLed out to avoid sending potentially sensitive information.
func buildPgStatsColumns(includeTableData bool) string {
	var mcv, hb, mce string
	if includeTableData {
		mcv = `CASE WHEN ps.most_common_vals IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.most_common_vals) END`
		hb = `CASE WHEN ps.histogram_bounds IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.histogram_bounds) END`
		mce = `CASE WHEN ps.most_common_elems IS NULL THEN NULL
         ELSE pg_catalog.format('%s', ps.most_common_elems) END`
	} else {
		const nullText = `NULL::text`
		mcv = nullText
		hb = nullText
		mce = nullText
	}

	return fmt.Sprintf(`
    ps.schemaname,
    ps.tablename,
    ps.attname,
    ps.inherited,
    ps.null_frac,
    ps.avg_width,
    ps.n_distinct,
    %s,
    CASE WHEN ps.most_common_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.most_common_freqs) END,
    %s,
    ps.correlation,
    %s,
    CASE WHEN ps.most_common_elem_freqs IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.most_common_elem_freqs) END,
    CASE WHEN ps.elem_count_histogram IS NULL THEN NULL
         ELSE pg_catalog.array_to_json(ps.elem_count_histogram) END`, mcv, hb, mce)
}

func buildPgStatsQueryBatch(includeTableData bool) string {
	return `SELECT` + buildPgStatsColumns(includeTableData) + `
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
}

func buildPgStatsQueryDelta(includeTableData bool) string {
	return `SELECT` + buildPgStatsColumns(includeTableData) + `
FROM pg_stats ps
JOIN pg_stat_user_tables pst
  ON ps.schemaname = pst.schemaname
 AND ps.tablename  = pst.relname
WHERE GREATEST(pst.last_analyze, pst.last_autoanalyze) > $1
ORDER BY ps.schemaname, ps.tablename, ps.attname
`
}

// pgtypeMap is reused across calls to pgArrayToJSON to avoid per-row allocation.
// Concurrent use is safe: Scan only performs read-only lookups on the internal
// codec registry; no mutation occurs after NewMap() returns.
var pgtypeMap = pgtype.NewMap()

// pgArrayToJSON converts a PG text array literal like {foo,"bar baz",qux}
// into a JSON array like ["foo","bar baz","qux"].
func pgArrayToJSON(s *string) (json.RawMessage, error) {
	if s == nil {
		return nil, nil
	}
	raw := *s

	var values []string
	if err := pgtypeMap.Scan(pgtype.TextArrayOID, pgtype.TextFormatCode, []byte(raw), &values); err == nil {
		data, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal pg array to JSON: %w", err)
		}
		return json.RawMessage(data), nil
	}

	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw), nil
	}

	return nil, fmt.Errorf("failed to parse pg array: %q", raw)
}

// toRawJSON converts a nullable JSON string from PG into json.RawMessage.
func toRawJSON(s *string) json.RawMessage {
	if s == nil {
		return nil
	}
	return json.RawMessage(*s)
}

func collectPgStatsRows(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) ([]PgStatsRow, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, query, args...)
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

func PgStatsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, backfillBatchSize int, includeTableData bool) CatalogCollector {
	batchQuery := buildPgStatsQueryBatch(includeTableData)
	deltaQuery := buildPgStatsQueryDelta(includeTableData)

	var (
		backfillOffset = 0
		backfillDone   = false
		lastPoll       time.Time
	)

	return CatalogCollector{
		Name:     PgStatsName,
		Interval: PgStatsInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			collectedAt := time.Now().UTC()
			var statsRows []PgStatsRow

			if !backfillDone {
				statsRows, err = collectPgStatsRows(pool, ctx, batchQuery, backfillBatchSize, backfillOffset)
				if err != nil {
					return nil, err
				}
				if len(statsRows) == 0 {
					backfillDone = true
					lastPoll = collectedAt
				} else {
					backfillOffset += backfillBatchSize
				}
			} else {
				statsRows, err = collectPgStatsRows(pool, ctx, deltaQuery, lastPoll)
				if err != nil {
					return nil, err
				}
				lastPoll = collectedAt
			}

			if len(statsRows) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgStatsRow]{CollectedAt: collectedAt, Rows: statsRows})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgStatsName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
