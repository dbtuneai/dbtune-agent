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

// parallelWorkerCommitsPerWorker is how much every launched parallel worker
// inflates xact_commit.
const parallelWorkerCommitsPerWorker = 2

const transactionCommitsQueryTemplate = `
SELECT SUM(xact_commit)::bigint AS server_xact_commits,
       %s AS server_parallel_workers_launched
FROM pg_stat_database`

func transactionCommitsQuery(pgMajorVersion int) string {
	parallelWorkersLaunched := "NULL::bigint"
	if pgMajorVersion >= 18 {
		parallelWorkersLaunched = "SUM(parallel_workers_launched)::bigint"
	}
	return fmt.Sprintf(transactionCommitsQueryTemplate, parallelWorkersLaunched)
}

type TransactionCommitsRow struct {
	XactCommit              int64   `json:"xact_commit"`
	ParallelWorkersLaunched *int64  `json:"parallel_workers_launched"`
	NumTransactions         int64   `json:"num_transactions"`
	TPS                     float64 `json:"tps,omitempty"`
}

func TransactionCommitsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	query := transactionCommitsQuery(pgMajorVersion)

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
			var parallelWorkersLaunched *int64
			err = utils.QueryRowWithPrefix(pool, ctx, query).Scan(&xactCommit, &parallelWorkersLaunched)
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %w", TransactionCommitsName, err)
			}

			numTransactions := xactCommit
			if parallelWorkersLaunched != nil {
				numTransactions -= parallelWorkerCommitsPerWorker * *parallelWorkersLaunched
			}

			row := TransactionCommitsRow{
				XactCommit:              xactCommit,
				ParallelWorkersLaunched: parallelWorkersLaunched,
				NumTransactions:         numTransactions,
			}

			// numTransactions can dip briefly: workers flush their commits on exit,
			// but the leader only flushes parallel_workers_launched once it goes
			// idle. We bound TPS from below by 0 but pay back the negatives as
			// soon as possible to keep it honest over time.
			//
			// if we don't have previous data, we need to update prev.count
			if prev.timestamp.IsZero() {
				prev.count = numTransactions
			} else if numTransactions > prev.count {
				duration := collectedAt.Sub(prev.timestamp).Seconds()
				if duration > 0 {
					row.TPS = float64(numTransactions-prev.count) / duration
				}
				// only update prev.count if it would increase it
				prev.count = numTransactions
			}
			prev.timestamp = collectedAt

			data, err := json.Marshal(&Payload[TransactionCommitsRow]{
				CollectedAt: collectedAt,
				Rows:        []TransactionCommitsRow{row},
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", TransactionCommitsName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
