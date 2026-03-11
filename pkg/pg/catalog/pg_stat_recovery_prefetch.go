package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-RECOVERY-PREFETCH

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatRecoveryPrefetchRow represents a row from pg_stat_recovery_prefetch (PG 15+).
type PgStatRecoveryPrefetchRow struct {
	StatsReset    *string `json:"stats_reset" db:"stats_reset"`       // pg: timestamp with time zone
	Prefetch      *int64  `json:"prefetch" db:"prefetch"`             // pg: bigint
	Hit           *int64  `json:"hit" db:"hit"`                       // pg: bigint
	SkipInit      *int64  `json:"skip_init" db:"skip_init"`           // pg: bigint
	SkipNew       *int64  `json:"skip_new" db:"skip_new"`             // pg: bigint
	SkipFpw       *int64  `json:"skip_fpw" db:"skip_fpw"`             // pg: bigint
	SkipRep       *int64  `json:"skip_rep" db:"skip_rep"`             // pg: bigint
	WalDistance   *int64  `json:"wal_distance" db:"wal_distance"`     // pg: integer
	BlockDistance *int64  `json:"block_distance" db:"block_distance"` // pg: integer
	IoDepth       *int64  `json:"io_depth" db:"io_depth"`             // pg: integer
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

func CollectPgStatRecoveryPrefetch(pgPool *pgxpool.Pool, ctx context.Context, pgMajorVersion int) ([]PgStatRecoveryPrefetchRow, error) {
	if pgMajorVersion < 15 {
		return nil, nil
	}
	return CollectView[PgStatRecoveryPrefetchRow](pgPool, ctx, pgStatRecoveryPrefetchQuery, "pg_stat_recovery_prefetch")
}

func NewPgStatRecoveryPrefetchCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatRecoveryPrefetchName,
		Interval: PgStatRecoveryPrefetchInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatRecoveryPrefetch(pool, ctx, pgMajorVersion)
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
