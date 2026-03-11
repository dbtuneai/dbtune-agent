package catalog

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatProgressAnalyzeName     = "pg_stat_progress_analyze"
	PgStatProgressAnalyzeInterval = 30 * time.Second
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

func NewPgStatProgressAnalyzeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatProgressAnalyzeName,
		Interval: PgStatProgressAnalyzeInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatProgressAnalyze(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressAnalyzePayload{Rows: rows}, nil
		},
	}
}
