package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatArchiverRow represents a row from pg_stat_archiver.
type PgStatArchiverRow struct {
	ArchivedCount    *Bigint      `json:"archived_count" db:"archived_count"`
	LastArchivedWal  *Text        `json:"last_archived_wal" db:"last_archived_wal"`
	LastArchivedTime *TimestampTZ `json:"last_archived_time" db:"last_archived_time"`
	FailedCount      *Bigint      `json:"failed_count" db:"failed_count"`
	LastFailedWal    *Text        `json:"last_failed_wal" db:"last_failed_wal"`
	LastFailedTime   *TimestampTZ `json:"last_failed_time" db:"last_failed_time"`
	StatsReset       *TimestampTZ `json:"stats_reset" db:"stats_reset"`
}

type PgStatArchiverPayload struct {
	Rows []PgStatArchiverRow `json:"rows"`
}

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

const pgStatArchiverQuery = `SELECT * FROM pg_stat_archiver`

func CollectPgStatArchiver(pgPool *pgxpool.Pool, ctx context.Context, scanner *pgxutil.Scanner[PgStatArchiverRow]) ([]PgStatArchiverRow, error) {
	return CollectView(pgPool, ctx, pgStatArchiverQuery, "pg_stat_archiver", scanner)
}

func PgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatArchiverRow]()
	return CatalogCollector{
		Name:     PgStatArchiverName,
		Interval: PgStatArchiverInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatArchiver(pool, ctx, scanner)
			if err != nil {
				return nil, err
			}
			return &PgStatArchiverPayload{Rows: rows}, nil
		},
	}
}
