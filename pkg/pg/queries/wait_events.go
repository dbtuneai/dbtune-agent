package queries

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WaitEventRow represents a row from the wait events aggregation query.
// Events maps each wait_event currently observed under WaitEventType to its
// count; it is empty for types with no waits and nil on the TOTAL row.
type WaitEventRow struct {
	WaitEventType Text            `json:"wait_event_type" db:"wait_event_type"`
	CurrentCount  Bigint          `json:"current_count" db:"current_count"`
	Events        map[Text]Bigint `json:"events" db:"events"`
}

const (
	WaitEventsName     = "wait_events"
	WaitEventsInterval = 5 * time.Second
)

// waitEventsQuery returns one row per known wait_event_type (zero-filled), each
// carrying an events object with the per-wait_event breakdown, plus a TOTAL row.
// Only non-zero wait_events appear in events since there are hundreds of them.
const waitEventsQuery = `
WITH
current_waits AS (
	SELECT
		wait_event_type,
		wait_event,
		count(*) as count
	FROM pg_stat_activity
	WHERE wait_event_type IS NOT NULL
	GROUP BY wait_event_type, wait_event
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
		COALESCE(sum(cw.count), 0)::bigint as current_count,
		COALESCE(
			jsonb_object_agg(cw.wait_event, cw.count) FILTER (WHERE cw.wait_event IS NOT NULL),
			'{}'::jsonb
		) as events
	FROM all_wait_types awt
	LEFT JOIN current_waits cw ON awt.column1 = cw.wait_event_type
	GROUP BY awt.column1
)
SELECT
	wait_event_type,
	current_count,
	events
FROM wait_counts
UNION ALL
SELECT
	'TOTAL' as wait_event_type,
	sum(current_count)::bigint as current_count,
	NULL::jsonb as events
FROM wait_counts`

func WaitEventsCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[WaitEventRow](pool, prepareCtx, WaitEventsName, WaitEventsInterval, waitEventsQuery)
}
