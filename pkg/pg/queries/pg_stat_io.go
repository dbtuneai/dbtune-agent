package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-IO

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatIOName     = "pg_stat_io"
	PgStatIOInterval = 1 * time.Minute
)

// PG 16+ only.
const pgStatIOQuery = `SELECT * FROM pg_stat_io`

func CollectPgStatIO(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatIORow, error) {
	if pgMajorVersion < 16 {
		return nil, nil
	}
	return CollectView[PgStatIORow](pgPool, ctx, pgStatIOQuery, "pg_stat_io")
}

func PgStatIOCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	return CatalogCollector{
		Name:     PgStatIOName,
		Interval: PgStatIOInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatIO(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &PgStatIOPayload{Rows: rows}, nil
		},
	}
}

// PgStatIORow represents a row from pg_stat_io (PG 16+).
type PgStatIORow struct {
	BackendType   *Text            `json:"backend_type" db:"backend_type"`
	Object        *Text            `json:"object" db:"object"`
	Context       *Text            `json:"context" db:"context"`
	Reads         *Bigint          `json:"reads" db:"reads"`
	ReadTime      *DoublePrecision `json:"read_time" db:"read_time"`
	Writes        *Bigint          `json:"writes" db:"writes"`
	WriteTime     *DoublePrecision `json:"write_time" db:"write_time"`
	Writebacks    *Bigint          `json:"writebacks" db:"writebacks"`
	WritebackTime *DoublePrecision `json:"writeback_time" db:"writeback_time"`
	Extends       *Bigint          `json:"extends" db:"extends"`
	ExtendTime    *DoublePrecision `json:"extend_time" db:"extend_time"`
	OpBytes       *Bigint          `json:"op_bytes" db:"op_bytes"`
	Hits          *Bigint          `json:"hits" db:"hits"`
	Evictions     *Bigint          `json:"evictions" db:"evictions"`
	Reuses        *Bigint          `json:"reuses" db:"reuses"`
	Fsyncs        *Bigint          `json:"fsyncs" db:"fsyncs"`
	FsyncTime     *DoublePrecision `json:"fsync_time" db:"fsync_time"`
	StatsReset    *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
}

type PgStatIOPayload struct {
	Rows []PgStatIORow `json:"rows"`
}
