package catalog

// https://www.postgresql.org/docs/current/catalog-pg-class.html

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgClassName     = "pg_class"
	PgClassInterval = 1 * time.Minute

	// PgClassBackfillBatchSize controls how many tables are fetched per tick
	// during the initial backfill. After all batches are sent, the collector
	// switches to delta mode using VACUUM/ANALYZE timestamps.
	PgClassBackfillBatchSize = 500
)

// pgClassColumns is the SELECT list shared by batch and delta queries.
const pgClassColumns = `
    n.nspname AS schemaname,
    c.relname,
    c.reltuples,
    c.relpages`

// pgClassQueryBatch paginates through user tables during backfill.
// The subquery uses LIMIT/OFFSET ($1=batch size, $2=offset) on pg_stat_user_tables.
const pgClassQueryBatch = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN (
    SELECT schemaname, relname
    FROM pg_stat_user_tables
    ORDER BY schemaname, relname
    LIMIT $1 OFFSET $2
) batch ON n.nspname = batch.schemaname
       AND c.relname = batch.relname
WHERE c.relkind = 'r'
ORDER BY n.nspname, c.relname
`

// pgClassQueryDelta returns rows only for tables that have been vacuumed or
// analyzed since $1. Both VACUUM and ANALYZE update reltuples/relpages.
const pgClassQueryDelta = `SELECT` + pgClassColumns + `
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_stat_user_tables pst
  ON n.nspname = pst.schemaname
 AND c.relname = pst.relname
WHERE c.relkind = 'r'
  AND GREATEST(pst.last_analyze, pst.last_autoanalyze,
               pst.last_vacuum, pst.last_autovacuum) > $1
ORDER BY n.nspname, c.relname
`

// PgClassRow represents a single row from pg_class for user tables,
// providing reltuples and relpages needed for index recommendation.
type PgClassRow struct {
	SchemaName string  `json:"schemaname"`
	RelName    string  `json:"relname"`
	RelTuples  float64 `json:"reltuples"`
	RelPages   int64   `json:"relpages"`
}

// PgClassPayload is the JSON body POSTed to /api/v1/agent/pg_class.
type PgClassPayload struct {
	Rows []PgClassRow `json:"rows"`
}

// PgClassQueryMode specifies which pg_class query mode to use.
type PgClassQueryMode struct {
	batchSize int
	offset    int
	since     *time.Time
}

// PgClassBackfillQuery creates a query mode for batch backfill.
func PgClassBackfillQuery(batchSize, offset int) PgClassQueryMode {
	return PgClassQueryMode{batchSize: batchSize, offset: offset}
}

// PgClassDeltaQuery creates a query mode for delta updates since a given time.
func PgClassDeltaQuery(since time.Time) PgClassQueryMode {
	return PgClassQueryMode{since: &since}
}

// CollectPgClass queries pg_class for reltuples and relpages of user tables.
// This uses custom scanning because reltuples is float32 in pg_class.
func CollectPgClass(pgPool *pgxpool.Pool, ctx context.Context, q PgClassQueryMode) ([]PgClassRow, error) {
	ctx, cancel := EnsureTimeout(ctx)
	defer cancel()

	var (
		rows pgx.Rows
		err  error
	)
	if q.since != nil {
		rows, err = utils.QueryWithPrefix(pgPool, ctx, pgClassQueryDelta, *q.since)
	} else {
		rows, err = utils.QueryWithPrefix(pgPool, ctx, pgClassQueryBatch, q.batchSize, q.offset)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_class: %w", err)
	}
	defer rows.Close()

	result := make([]PgClassRow, 0)

	for rows.Next() {
		var r PgClassRow
		err := rows.Scan(
			&r.SchemaName,
			&r.RelName,
			&r.RelTuples,
			&r.RelPages,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_class row: %w", err)
		}
		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_class rows: %w", err)
	}

	return result, nil
}

// PgClassCollectFunc is the function signature for querying pg_class.
type PgClassCollectFunc func(ctx context.Context, q PgClassQueryMode) ([]PgClassRow, error)

// newPgClassCollectFunc builds the collect closure that implements the
// backfill → delta state machine.
func newPgClassCollectFunc(collectFn PgClassCollectFunc, batchSize int) func(ctx context.Context) (any, error) {
	var (
		backfillOffset = 0
		backfillDone   = false
		lastPoll       *time.Time
	)
	return func(ctx context.Context) (any, error) {
		var (
			classRows []PgClassRow
			err       error
		)

		if !backfillDone {
			classRows, err = collectFn(ctx, PgClassBackfillQuery(batchSize, backfillOffset))
			if err != nil {
				return nil, err
			}
			if len(classRows) == 0 {
				backfillDone = true
				now := time.Now().UTC()
				lastPoll = &now
			} else {
				backfillOffset += batchSize
			}
		} else {
			now := time.Now().UTC()
			classRows, err = collectFn(ctx, PgClassDeltaQuery(*lastPoll))
			if err != nil {
				return nil, err
			}
			lastPoll = &now
		}

		if len(classRows) == 0 {
			return nil, nil
		}
		return &PgClassPayload{Rows: classRows}, nil
	}
}

func NewPgClassCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	collectFn := func(ctx context.Context, q PgClassQueryMode) ([]PgClassRow, error) {
		ctx, err := prepareCtx(ctx)
		if err != nil {
			return nil, err
		}
		return CollectPgClass(pool, ctx, q)
	}
	return agent.CatalogCollector{
		Name:     PgClassName,
		Interval: PgClassInterval,
		Collect:  newPgClassCollectFunc(collectFn, PgClassBackfillBatchSize),
	}
}
