package pg

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PGStatWalQuery = `
SELECT
    wal_records,
	wal_fpi,
	wal_bytes,
	wal_buffers_full
FROM pg_stat_wal;
`

// PGStatWAL collects statistics from pg_stat_wal and computes and emits deltas for them.
func PGStatWAL(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var WalRecords, WalFpi, WalBytes, WalBuffersFull int64
		err := utils.QueryRowWithPrefix(pgPool, ctx, PGStatWalQuery).Scan(
			&WalRecords,
			&WalFpi,
			&WalBytes,
			&WalBuffersFull,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappingsInt := []MetricMapping[int64]{
			{Current: &WalRecords, Previous: &state.Cache.PGWAL.WALRecords, Metric: metrics.PGWALRecords},
			{Current: &WalFpi, Previous: &state.Cache.PGWAL.WALFpi, Metric: metrics.PGWALFpi},
			{Current: &WalBytes, Previous: &state.Cache.PGWAL.WALBytes, Metric: metrics.PGWALBytes},
			{Current: &WalBuffersFull, Previous: &state.Cache.PGWAL.WALBuffersFull, Metric: metrics.PGWALBuffersFull},
		}

		if err := EmitCumulativeMetricsMap(state, mappingsInt); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGWAL = agent.PGWAL{
			WALRecords:     WalRecords,
			WALFpi:         WalFpi,
			WALBytes:       WalBytes,
			WALBuffersFull: WalBuffersFull,
			Timestamp:      time.Now(),
		}

		return nil
	}
}
