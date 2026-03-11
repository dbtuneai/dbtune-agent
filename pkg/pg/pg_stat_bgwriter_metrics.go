package pg

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PGStatBGWriterQuery = `
SELECT
    buffers_clean,
	maxwritten_clean,
	buffers_alloc
FROM pg_stat_bgwriter;
`

// PGStatBGwriter collects statistics from pg_stat_bgwriter and computes and emits deltas for them.
func PGStatBGwriter(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var buffersClean, maxWrittenClean, buffersAlloc int64
		err := utils.QueryRowWithPrefix(pgPool, ctx, PGStatBGWriterQuery).Scan(
			&buffersClean,
			&maxWrittenClean,
			&buffersAlloc,
		)

		if err != nil {
			return err
		}

		// Check if we have cached values from the previous collection
		mappings := []MetricMapping[int64]{
			{Current: &buffersClean, Previous: &state.Cache.PGBGWriter.BuffersClean, Metric: metrics.PGBGWBuffersClean},
			{Current: &maxWrittenClean, Previous: &state.Cache.PGBGWriter.MaxWrittenClean, Metric: metrics.PGMBGWaxWrittenClean},
			{Current: &buffersAlloc, Previous: &state.Cache.PGBGWriter.BuffersAlloc, Metric: metrics.PGBGWBuffersAlloc},
		}

		if err := EmitCumulativeMetricsMap(state, mappings); err != nil {
			return err
		}

		// Update cache for next iteration
		state.Cache.PGBGWriter = agent.PGBGWriter{
			BuffersClean:    buffersClean,
			MaxWrittenClean: maxWrittenClean,
			BuffersAlloc:    buffersAlloc,
			Timestamp:       time.Now(),
		}

		return nil
	}
}
