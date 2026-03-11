package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const WaitEventsQuery = `
WITH RECURSIVE
current_waits AS (
	SELECT
		wait_event_type,
		count(*) as count
	FROM pg_stat_activity
	WHERE wait_event_type IS NOT NULL
	GROUP BY wait_event_type
),
all_wait_types AS (
	VALUES
		('Activity'),
		('BufferPin'),
		('Client'),
		('Extension'),
		('IO'),
		('IPC'),
		('Lock'),
		('LWLock'),
		('Timeout')
),
wait_counts AS (
	SELECT
		awt.column1 as wait_event_type,
		COALESCE(cw.count, 0) as current_count
	FROM all_wait_types awt
	LEFT JOIN current_waits cw ON awt.column1 = cw.wait_event_type
)
SELECT
	wait_event_type,
	current_count
FROM wait_counts
UNION ALL
SELECT
	'TOTAL' as wait_event_type,
	sum(current_count) as current_count
FROM wait_counts;
`

func WaitEvents(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		rows, err := utils.QueryWithPrefix(pgPool, ctx, WaitEventsQuery)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var event string
			var count int
			err = rows.Scan(&event, &count)
			if err != nil {
				return err
			}

			metricEntry, _ := metrics.PGWaitEvent{Name: event}.AsFlatValue(count)
			state.AddMetric(metricEntry)
		}

		return nil
	}
}
