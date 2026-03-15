package queries

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	WaitEventsName     = "wait_events"
	WaitEventsInterval = 5 * time.Second
)

const waitEventsQuery = `
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
FROM wait_counts`

func CollectWaitEvents(pgPool *pgxpool.Pool, ctx context.Context) ([]WaitEventRow, error) {
	return CollectView[WaitEventRow](pgPool, ctx, waitEventsQuery, "wait_events")
}

func WaitEventsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     WaitEventsName,
		Interval: WaitEventsInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectWaitEvents(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &WaitEventsPayload{Rows: rows}, nil
		},
	}
}

// WaitEventRow represents a row from the wait events aggregation query.
type WaitEventRow struct {
	WaitEventType Text   `json:"wait_event_type" db:"wait_event_type"`
	CurrentCount  Bigint `json:"current_count" db:"current_count"`
}

type WaitEventsPayload struct {
	Rows []WaitEventRow `json:"rows"`
}
