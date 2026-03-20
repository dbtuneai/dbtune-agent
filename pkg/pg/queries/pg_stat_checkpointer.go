package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-CHECKPOINTER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatCheckpointerRow represents a row from pg_stat_checkpointer (PG 17+).
type PgStatCheckpointerRow struct {
	NumTimed           *Bigint          `json:"num_timed" db:"num_timed"`
	NumRequested       *Bigint          `json:"num_requested" db:"num_requested"`
	RestartpointsTimed *Bigint          `json:"restartpoints_timed" db:"restartpoints_timed"`
	RestartpointsReq   *Bigint          `json:"restartpoints_req" db:"restartpoints_req"`
	RestartpointsDone  *Bigint          `json:"restartpoints_done" db:"restartpoints_done"`
	WriteTime          *DoublePrecision `json:"write_time" db:"write_time"`
	SyncTime           *DoublePrecision `json:"sync_time" db:"sync_time"`
	BuffersWritten     *Bigint          `json:"buffers_written" db:"buffers_written"`
	StatsReset         *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
	SlruWritten        *Bigint          `json:"slru_written" db:"slru_written"`
}

type PgStatCheckpointerPayload struct {
	Rows []PgStatCheckpointerRow `json:"rows"`
}

const (
	PgStatCheckpointerName     = "pg_stat_checkpointer"
	PgStatCheckpointerInterval = 1 * time.Minute
)

// PG 17+ only.
const pgStatCheckpointerQuery = `SELECT * FROM pg_stat_checkpointer`

func CollectPgStatCheckpointer(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int, scanner *pgxutil.Scanner[PgStatCheckpointerRow]) ([]PgStatCheckpointerRow, error) {
	if pgMajorVersion < 17 {
		return nil, nil
	}
	return CollectView(pgPool, ctx, pgStatCheckpointerQuery, "pg_stat_checkpointer", scanner)
}

func PgStatCheckpointerCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatCheckpointerRow]()
	return CatalogCollector{
		Name:     PgStatCheckpointerName,
		Interval: PgStatCheckpointerInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatCheckpointer(pool, ctx, pgMajorVersion, scanner)
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
