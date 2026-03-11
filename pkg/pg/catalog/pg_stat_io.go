package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-IO

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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

func NewPgStatIOCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
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
	BackendType   *string  `json:"backend_type" db:"backend_type"`     // pg: text
	Object        *string  `json:"object" db:"object"`                 // pg: text
	Context       *string  `json:"context" db:"context"`               // pg: text
	Reads         *int64   `json:"reads" db:"reads"`                   // pg: bigint
	ReadTime      *float64 `json:"read_time" db:"read_time"`           // pg: double precision
	Writes        *int64   `json:"writes" db:"writes"`                 // pg: bigint
	WriteTime     *float64 `json:"write_time" db:"write_time"`         // pg: double precision
	Writebacks    *int64   `json:"writebacks" db:"writebacks"`         // pg: bigint
	WritebackTime *float64 `json:"writeback_time" db:"writeback_time"` // pg: double precision
	Extends       *int64   `json:"extends" db:"extends"`               // pg: bigint
	ExtendTime    *float64 `json:"extend_time" db:"extend_time"`       // pg: double precision
	OpBytes       *int64   `json:"op_bytes" db:"op_bytes"`             // pg: bigint
	Hits          *int64   `json:"hits" db:"hits"`                     // pg: bigint
	Evictions     *int64   `json:"evictions" db:"evictions"`           // pg: bigint
	Reuses        *int64   `json:"reuses" db:"reuses"`                 // pg: bigint
	Fsyncs        *int64   `json:"fsyncs" db:"fsyncs"`                 // pg: bigint
	FsyncTime     *float64 `json:"fsync_time" db:"fsync_time"`         // pg: double precision
	StatsReset    *string  `json:"stats_reset" db:"stats_reset"`       // pg: timestamp with time zone
}

type PgStatIOPayload struct {
	Rows []PgStatIORow `json:"rows"`
}
