package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-CHECKPOINTER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatCheckpointerName     = "pg_stat_checkpointer"
	PgStatCheckpointerInterval = 1 * time.Minute
)

// PG 17+ only.
const pgStatCheckpointerQuery = `SELECT * FROM pg_stat_checkpointer`

func CollectPgStatCheckpointer(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatCheckpointerRow, error) {
	if pgMajorVersion < 17 {
		return nil, nil
	}
	return CollectView[PgStatCheckpointerRow](pgPool, ctx, pgStatCheckpointerQuery, "pg_stat_checkpointer")
}

func NewPgStatCheckpointerCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatCheckpointerName,
		Interval: PgStatCheckpointerInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatCheckpointer(pool, ctx, pgMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &PgStatCheckpointerPayload{Rows: rows}, nil
		},
	}
}

// PgStatCheckpointerRow represents a row from pg_stat_checkpointer (PG 17+).
type PgStatCheckpointerRow struct {
	NumTimed           *int64   `json:"num_timed" db:"num_timed"`
	NumRequested       *int64   `json:"num_requested" db:"num_requested"`
	RestartpointsTimed *int64   `json:"restartpoints_timed" db:"restartpoints_timed"`
	RestartpointsReq   *int64   `json:"restartpoints_req" db:"restartpoints_req"`
	RestartpointsDone  *int64   `json:"restartpoints_done" db:"restartpoints_done"`
	WriteTime          *float64 `json:"write_time" db:"write_time"`
	SyncTime           *float64 `json:"sync_time" db:"sync_time"`
	BuffersWritten     *int64   `json:"buffers_written" db:"buffers_written"`
	StatsReset         *string  `json:"stats_reset" db:"stats_reset"`
	SlruWritten        *int64   `json:"slru_written" db:"slru_written"`
}

type PgStatCheckpointerPayload struct {
	Rows []PgStatCheckpointerRow `json:"rows"`
}
