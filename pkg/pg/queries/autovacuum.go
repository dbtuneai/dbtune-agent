package queries

// Autovacuum counts the number of currently running autovacuum worker processes
// by filtering pg_stat_activity for queries that start with 'autovacuum:'.
//
// https://www.postgresql.org/docs/current/routine-vacuuming.html#AUTOVACUUM

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	AutovacuumCountName     = "autovacuum_count"
	AutovacuumCountInterval = 5 * time.Second
)

const AutovacuumQuery = `SELECT COUNT(*) FROM pg_stat_activity WHERE starts_with(query, 'autovacuum:')`

// CollectAutovacuumCount returns the number of currently running autovacuum worker processes.
func CollectAutovacuumCount(pool *pgxpool.Pool, ctx context.Context) (int, error) {
	var result int
	err := utils.QueryRowWithPrefix(pool, ctx, AutovacuumQuery).Scan(&result)
	return result, err
}

type AutovacuumCountPayload struct {
	Count int `json:"count"`
}

func AutovacuumCountCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     AutovacuumCountName,
		Interval: AutovacuumCountInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			count, err := CollectAutovacuumCount(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &AutovacuumCountPayload{Count: count}, nil
		},
	}
}
