package queries

// TransactionCommits queries the cumulative committed transaction count from
// pg_stat_database and computes TPS from consecutive samples.
//
// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-VIEW

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TransactionCommitsName     = "database_transactions"
	TransactionCommitsInterval = 5 * time.Second
)

const transactionCommitsQuery = `
SELECT SUM(xact_commit)::bigint AS server_xact_commits
FROM pg_stat_database`

type TransactionCommitsPayload struct {
	CollectedAt time.Time `json:"collected_at"`
	XactCommit  int64     `json:"xact_commit"`
	TPS         float64   `json:"tps,omitempty"`
}

func TransactionCommitsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	var prev struct {
		count     int64
		timestamp time.Time
	}

	return CatalogCollector{
		Name:     TransactionCommitsName,
		Interval: TransactionCommitsInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			collectedAt := time.Now().UTC()
			var xactCommit int64
			err = utils.QueryRowWithPrefix(pool, ctx, transactionCommitsQuery).Scan(&xactCommit)
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %w", TransactionCommitsName, err)
			}

			payload := TransactionCommitsPayload{
				CollectedAt: collectedAt,
				XactCommit:  xactCommit,
			}

			now := collectedAt
			if !prev.timestamp.IsZero() && xactCommit >= prev.count {
				duration := now.Sub(prev.timestamp).Seconds()
				if duration > 0 {
					payload.TPS = float64(xactCommit-prev.count) / duration
				}
			}

			prev.count = xactCommit
			prev.timestamp = now

			data, err := json.Marshal(&payload)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", TransactionCommitsName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
