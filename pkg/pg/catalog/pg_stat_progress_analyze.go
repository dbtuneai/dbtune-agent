package catalog

// https://www.postgresql.org/docs/current/progress-reporting.html#ANALYZE-PROGRESS-REPORTING

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

const pgStatProgressAnalyzeQuery = `SELECT * FROM pg_stat_progress_analyze WHERE datname = current_database()`

// PgStatProgressAnalyzeRow represents a row from pg_stat_progress_analyze (PG 13+).
type PgStatProgressAnalyzeRow struct {
	PID                    *Integer `json:"pid" db:"pid"`
	DatID                  *Oid     `json:"datid" db:"datid"`
	DatName                *Name    `json:"datname" db:"datname"`
	RelID                  *Oid     `json:"relid" db:"relid"`
	Phase                  *Text    `json:"phase" db:"phase"`
	SampleBlksTotal        *Bigint  `json:"sample_blks_total" db:"sample_blks_total"`
	SampleBlksScanned      *Bigint  `json:"sample_blks_scanned" db:"sample_blks_scanned"`
	ExtStatsTotal          *Bigint  `json:"ext_stats_total" db:"ext_stats_total"`
	ExtStatsComputed       *Bigint  `json:"ext_stats_computed" db:"ext_stats_computed"`
	ChildTablesTotal       *Bigint  `json:"child_tables_total" db:"child_tables_total"`
	ChildTablesDone        *Bigint  `json:"child_tables_done" db:"child_tables_done"`
	CurrentChildTableRelID *Oid     `json:"current_child_table_relid" db:"current_child_table_relid"`
}

type PgStatProgressAnalyzePayload struct {
	Rows []PgStatProgressAnalyzeRow `json:"rows"`
}

func CollectPgStatProgressAnalyze(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatProgressAnalyzeRow, error) {
	return CollectView[PgStatProgressAnalyzeRow](pgPool, ctx, pgStatProgressAnalyzeQuery, "pg_stat_progress_analyze")
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
			return &PgStatProgressAnalyzePayload{Rows: rows}, nil
		},
	}
}
