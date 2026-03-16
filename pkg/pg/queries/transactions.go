package queries

// TransactionCommits queries the cumulative committed transaction count from
// pg_stat_database and computes TPS from consecutive samples.
//
// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-VIEW

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TransactionCommitsName     = "database_transactions"
	TransactionCommitsInterval = 5 * time.Second
)

const TransactionCommitsQuery = `
SELECT SUM(xact_commit)::bigint AS server_xact_commits
FROM pg_stat_database`

// CollectTransactionCommits returns the total committed transaction count across all databases.
func CollectTransactionCommits(pool *pgxpool.Pool, ctx context.Context) (int64, error) {
	var serverXactCommits int64
	err := utils.QueryRowWithPrefix(pool, ctx, TransactionCommitsQuery).Scan(&serverXactCommits)
	return serverXactCommits, err
}

type TransactionCommitsPayload struct {
	XactCommit int64   `json:"xact_commit"`
	TPS        float64 `json:"tps,omitempty"`
}

func TransactionCommitsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	var prev struct {
		count     int64
		timestamp time.Time
	}

	return CatalogCollector{
		Name:     TransactionCommitsName,
		Interval: TransactionCommitsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			xactCommit, err := CollectTransactionCommits(pool, ctx)
			if err != nil {
				return nil, err
			}

			payload := TransactionCommitsPayload{
				XactCommit: xactCommit,
			}

			now := time.Now()
			if prev.count > 0 && xactCommit >= prev.count {
				duration := now.Sub(prev.timestamp).Seconds()
				if duration > 0 {
					payload.TPS = float64(xactCommit-prev.count) / duration
				}
			}

			prev.count = xactCommit
			prev.timestamp = now

			return &payload, nil
		},
	}
}
