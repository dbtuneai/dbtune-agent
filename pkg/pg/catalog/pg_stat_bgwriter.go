package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-BGWRITER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatBgwriterName     = "pg_stat_bgwriter"
	PgStatBgwriterInterval = 1 * time.Minute
)

const pgStatBgwriterQuery = `SELECT * FROM pg_stat_bgwriter`

func CollectPgStatBgwriter(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatBgwriterRow, error) {
	return CollectView[PgStatBgwriterRow](pgPool, ctx, pgStatBgwriterQuery, "pg_stat_bgwriter")
}

func NewPgStatBgwriterCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatBgwriterName,
		Interval: PgStatBgwriterInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatBgwriter(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatBgwriterPayload{Rows: rows}, nil
		},
	}
}

// PgStatBgwriterRow represents a row from pg_stat_bgwriter.
type PgStatBgwriterRow struct {
	CheckpointsTimed    *int64   `json:"checkpoints_timed" db:"checkpoints_timed"`       // pg: bigint
	CheckpointsReq      *int64   `json:"checkpoints_req" db:"checkpoints_req"`           // pg: bigint
	CheckpointWriteTime *float64 `json:"checkpoint_write_time" db:"checkpoint_write_time"` // pg: double precision
	CheckpointSyncTime  *float64 `json:"checkpoint_sync_time" db:"checkpoint_sync_time"` // pg: double precision
	BuffersCheckpoint   *int64   `json:"buffers_checkpoint" db:"buffers_checkpoint"`     // pg: bigint
	BuffersClean        *int64   `json:"buffers_clean" db:"buffers_clean"`               // pg: bigint
	MaxwrittenClean     *int64   `json:"maxwritten_clean" db:"maxwritten_clean"`         // pg: bigint
	BuffersBackend      *int64   `json:"buffers_backend" db:"buffers_backend"`           // pg: bigint
	BuffersBackendFsync *int64   `json:"buffers_backend_fsync" db:"buffers_backend_fsync"` // pg: bigint
	BuffersAlloc        *int64   `json:"buffers_alloc" db:"buffers_alloc"`               // pg: bigint
	StatsReset          *string  `json:"stats_reset" db:"stats_reset"`                   // pg: timestamp with time zone
}

type PgStatBgwriterPayload struct {
	Rows []PgStatBgwriterRow `json:"rows"`
}
