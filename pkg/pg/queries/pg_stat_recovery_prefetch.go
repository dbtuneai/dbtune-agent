package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-RECOVERY-PREFETCH

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatRecoveryPrefetchRow represents a row from pg_stat_recovery_prefetch (PG 15+).
type PgStatRecoveryPrefetchRow struct {
	StatsReset    *TimestampTZ `json:"stats_reset" db:"stats_reset"`
	Prefetch      *Bigint      `json:"prefetch" db:"prefetch"`
	Hit           *Bigint      `json:"hit" db:"hit"`
	SkipInit      *Bigint      `json:"skip_init" db:"skip_init"`
	SkipNew       *Bigint      `json:"skip_new" db:"skip_new"`
	SkipFpw       *Bigint      `json:"skip_fpw" db:"skip_fpw"`
	SkipRep       *Bigint      `json:"skip_rep" db:"skip_rep"`
	WalDistance   *Integer     `json:"wal_distance" db:"wal_distance"`
	BlockDistance *Integer     `json:"block_distance" db:"block_distance"`
	IoDepth       *Integer     `json:"io_depth" db:"io_depth"`
}

type PgStatRecoveryPrefetchPayload struct {
	Rows []PgStatRecoveryPrefetchRow `json:"rows"`
}

const (
	PgStatRecoveryPrefetchName     = "pg_stat_recovery_prefetch"
	PgStatRecoveryPrefetchInterval = 1 * time.Minute
)

// PG 15+ only.
const pgStatRecoveryPrefetchQuery = `SELECT * FROM pg_stat_recovery_prefetch`

func CollectPgStatRecoveryPrefetch(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int, scanner *pgxutil.Scanner[PgStatRecoveryPrefetchRow]) ([]PgStatRecoveryPrefetchRow, error) {
	if pgMajorVersion < 15 {
		return nil, nil
	}
	return CollectView(pgPool, ctx, pgStatRecoveryPrefetchQuery, "pg_stat_recovery_prefetch", scanner)
}

func PgStatRecoveryPrefetchCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatRecoveryPrefetchRow]()
	return CatalogCollector{
		Name:     PgStatRecoveryPrefetchName,
		Interval: PgStatRecoveryPrefetchInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatRecoveryPrefetch(pool, ctx, pgMajorVersion, scanner)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &PgStatRecoveryPrefetchPayload{Rows: rows}, nil
		},
	}
}
