package pg

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PGStatCheckpointerQuery = `
SELECT
    num_timed,
	num_requested,
	write_time,
	sync_time,
	buffers_written
FROM pg_stat_checkpointer
`

// PGStatCheckpointer collects statistics from pg_stat_checkpointer and computes and emits deltas.
func PGStatCheckpointer(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var numTimed, numRequested, BuffersWritten int64
		var writeTime, syncTime float64
		err := utils.QueryRowWithPrefix(pgPool, ctx, PGStatCheckpointerQuery).Scan(
			&numTimed,
			&numRequested,
			&writeTime,
			&syncTime,
			&BuffersWritten,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappingsInt := []MetricMapping[int64]{
			{Current: &numTimed, Previous: &state.Cache.PGCheckPointer.NumTimed, Metric: metrics.PGCPNumTimed},
			{Current: &numRequested, Previous: &state.Cache.PGCheckPointer.NumRequested, Metric: metrics.PGCPNumRequested},
			{Current: &BuffersWritten, Previous: &state.Cache.PGCheckPointer.BuffersWritten, Metric: metrics.PGCPBuffersWritten},
		}
		mappingsFloat := []MetricMapping[float64]{
			{Current: &writeTime, Previous: &state.Cache.PGCheckPointer.WriteTime, Metric: metrics.PGCPWriteTime},
			{Current: &syncTime, Previous: &state.Cache.PGCheckPointer.SyncTime, Metric: metrics.PGCPSyncTime},
		}

		if err := EmitCumulativeMetricsMap(state, mappingsInt); err != nil {
			return err
		}
		if err := EmitCumulativeMetricsMap(state, mappingsFloat); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGCheckPointer = agent.PGCheckPointer{
			NumTimed:       numTimed,
			NumRequested:   numRequested,
			WriteTime:      writeTime,
			SyncTime:       syncTime,
			BuffersWritten: BuffersWritten,
			Timestamp:      time.Now(),
		}

		return nil
	}
}
