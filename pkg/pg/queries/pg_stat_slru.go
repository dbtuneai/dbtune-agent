package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SLRU

import (
	"context"
	"time"

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

func PgStatSlruCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
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
	Name        *Text        `json:"name" db:"name"`
	BlksZeroed  *Bigint      `json:"blks_zeroed" db:"blks_zeroed"`
	BlksHit     *Bigint      `json:"blks_hit" db:"blks_hit"`
	BlksRead    *Bigint      `json:"blks_read" db:"blks_read"`
	BlksWritten *Bigint      `json:"blks_written" db:"blks_written"`
	BlksExists  *Bigint      `json:"blks_exists" db:"blks_exists"`
	Flushes     *Bigint      `json:"flushes" db:"flushes"`
	Truncates   *Bigint      `json:"truncates" db:"truncates"`
	StatsReset  *TimestampTZ `json:"stats_reset" db:"stats_reset"`
}

type PgStatSlruPayload struct {
	Rows []PgStatSlruRow `json:"rows"`
}
