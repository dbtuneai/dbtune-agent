package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SLRU

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatSlruName     = "pg_stat_slru"
	PgStatSlruInterval = 1 * time.Minute
)

const pgStatSlruQuery = `SELECT * FROM pg_stat_slru`

func CollectPgStatSlru(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatSlruRow, error) {
	return CollectView[PgStatSlruRow](pgPool, ctx, pgStatSlruQuery, "pg_stat_slru")
}

func NewPgStatSlruCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatSlruName,
		Interval: PgStatSlruInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatSlru(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatSlruPayload{Rows: rows}, nil
		},
	}
}

// PgStatSlruRow represents a row from pg_stat_slru.
type PgStatSlruRow struct {
	Name        *string `json:"name" db:"name"`               // pg: text
	BlksZeroed  *int64  `json:"blks_zeroed" db:"blks_zeroed"` // pg: bigint
	BlksHit     *int64  `json:"blks_hit" db:"blks_hit"`       // pg: bigint
	BlksRead    *int64  `json:"blks_read" db:"blks_read"`     // pg: bigint
	BlksWritten *int64  `json:"blks_written" db:"blks_written"` // pg: bigint
	BlksExists  *int64  `json:"blks_exists" db:"blks_exists"` // pg: bigint
	Flushes     *int64  `json:"flushes" db:"flushes"`         // pg: bigint
	Truncates   *int64  `json:"truncates" db:"truncates"`     // pg: bigint
	StatsReset  *string `json:"stats_reset" db:"stats_reset"` // pg: timestamp with time zone
}

type PgStatSlruPayload struct {
	Rows []PgStatSlruRow `json:"rows"`
}
