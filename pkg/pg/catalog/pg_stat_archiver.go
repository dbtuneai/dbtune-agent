package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

const pgStatArchiverQuery = `SELECT * FROM pg_stat_archiver`

func CollectPgStatArchiver(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatArchiverRow, error) {
	return CollectView[PgStatArchiverRow](pgPool, ctx, pgStatArchiverQuery, "pg_stat_archiver")
}

func NewPgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatArchiverName,
		Interval: PgStatArchiverInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatArchiver(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatArchiverPayload{Rows: rows}, nil
		},
	}
}

// PgStatArchiverRow represents a row from pg_stat_archiver.
type PgStatArchiverRow struct {
	ArchivedCount    *int64  `json:"archived_count" db:"archived_count"`         // pg: bigint
	LastArchivedWal  *string `json:"last_archived_wal" db:"last_archived_wal"`   // pg: text
	LastArchivedTime *string `json:"last_archived_time" db:"last_archived_time"` // pg: timestamp with time zone
	FailedCount      *int64  `json:"failed_count" db:"failed_count"`             // pg: bigint
	LastFailedWal    *string `json:"last_failed_wal" db:"last_failed_wal"`       // pg: text
	LastFailedTime   *string `json:"last_failed_time" db:"last_failed_time"`     // pg: timestamp with time zone
	StatsReset       *string `json:"stats_reset" db:"stats_reset"`               // pg: timestamp with time zone
}

type PgStatArchiverPayload struct {
	Rows []PgStatArchiverRow `json:"rows"`
}
