package catalog

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
// This uses custom scanning because reltuples is float32 in pg_class.
func CollectPgClass(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgClassRow, error) {
	ctx, cancel := EnsureTimeout(ctx)
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
