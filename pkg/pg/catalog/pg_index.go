package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Joined with pg_class/pg_namespace for human-readable names.
// reltuples cast to float8 to avoid float32 scanning issues.
// Filtered to user schemas only.
const pgIndexQuery = `
SELECT
    n.nspname AS schemaname,
    t.relname AS tablename,
    c.relname AS indexname,
    i.indexrelid,
    i.indrelid,
    i.indnatts::bigint AS indnatts,
    i.indnkeyatts::bigint AS indnkeyatts,
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    i.indimmediate,
    i.indisclustered,
    i.indisvalid,
    i.indcheckxmin,
    i.indisready,
    i.indislive,
    i.indisreplident,
    c.reltuples::float8 AS reltuples,
    pg_get_indexdef(i.indexrelid) AS indexdef
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, t.relname, c.relname
`

func CollectPgIndex(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgIndexRow, error) {
	return CollectView[agent.PgIndexRow](pgPool, ctx, pgIndexQuery, "pg_index")
}
