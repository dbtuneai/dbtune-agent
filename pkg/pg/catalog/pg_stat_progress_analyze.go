package catalog

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgStatProgressAnalyzeQuery = `
SELECT
    pid, datid, datname, relid,
    phase,
    sample_blks_total, sample_blks_scanned,
    ext_stats_total, ext_stats_computed,
    child_tables_total, child_tables_done,
    current_child_table_relid
FROM pg_stat_progress_analyze
`

func CollectPgStatProgressAnalyze(pgPool *pgxpool.Pool, ctx context.Context) ([]agent.PgStatProgressAnalyzeRow, error) {
	return CollectView[agent.PgStatProgressAnalyzeRow](pgPool, ctx, pgStatProgressAnalyzeQuery, "pg_stat_progress_analyze")
}
