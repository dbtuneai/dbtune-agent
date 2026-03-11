package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgStatsQuery reads from the pg_stats view which provides a human-readable
// form of the pg_statistic catalog. Array columns use format() to stringify anyarray values
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

// CollectPgStats queries the pg_stats view and returns rows for the backend.
// This uses custom scanning because pg_stats has anyarray columns that cannot
// be directly scanned by pgx.RowToStructByNameLax.
func CollectPgStats(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatsRow, error) {
	ctx, cancel := EnsureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pgPool, ctx, pgStatsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stats: %w", err)
	}
	defer rows.Close()

	result := make([]agent.PgStatsRow, 0)

	for rows.Next() {
		var r agent.PgStatsRow
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
