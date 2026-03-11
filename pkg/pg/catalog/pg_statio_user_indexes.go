package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-INDEXES

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatioUserIndexesName     = "pg_statio_user_indexes"
	PgStatioUserIndexesInterval = 1 * time.Minute

	// PgStatioUserIndexesCategoryLimit caps each activity category in the UNION
	// query. See PgStatUserTablesCategoryLimit for rationale.
	PgStatioUserIndexesCategoryLimit = 200
)

// pgStatioUserIndexesQuery samples index I/O stats from three complementary
// perspectives:
//
//  1. Disk reads (idx_blks_read) — indexes causing the most physical I/O
//     (cache misses). These are the primary candidates for buffer pool or
//     index tuning since disk reads are orders of magnitude slower than
//     cache hits.
//
//  2. Total I/O volume (idx_blks_read + idx_blks_hit) — indexes with the
//     most overall I/O activity regardless of cache ratio. A fully-cached
//     hot index would be missed by disk-reads-only ordering but may still
//     be worth monitoring.
//
//  3. Cache-miss ratio (idx_blks_read filtered to > 0, ordered by read
//     volume) — ensures we capture indexes that are hitting disk at all,
//     even if their absolute volume is low. A small index with 100% cache
//     misses indicates a potential configuration issue.
//
// UNION deduplicates across categories automatically.
var pgStatioUserIndexesQuery = fmt.Sprintf(`
SELECT * FROM pg_statio_user_indexes ORDER BY COALESCE(idx_blks_read,0) DESC LIMIT %d
UNION
SELECT * FROM pg_statio_user_indexes ORDER BY COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) DESC LIMIT %d
UNION
SELECT * FROM pg_statio_user_indexes WHERE COALESCE(idx_blks_read,0) > 0 ORDER BY COALESCE(idx_blks_read,0) DESC LIMIT %d
`, PgStatioUserIndexesCategoryLimit, PgStatioUserIndexesCategoryLimit, PgStatioUserIndexesCategoryLimit)

func CollectPgStatioUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatioUserIndexesRow, error) {
	return CollectView[PgStatioUserIndexesRow](pgPool, ctx, pgStatioUserIndexesQuery, "pg_statio_user_indexes")
}

func NewPgStatioUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatioUserIndexesName,
		Interval: PgStatioUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatioUserIndexesPayload{Rows: rows}, nil
		},
	}
}

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *int64  `json:"relid" db:"relid"`                 // pg: oid
	IndexRelID   *int64  `json:"indexrelid" db:"indexrelid"`       // pg: oid
	SchemaName   *string `json:"schemaname" db:"schemaname"`       // pg: name
	RelName      *string `json:"relname" db:"relname"`             // pg: name
	IndexRelName *string `json:"indexrelname" db:"indexrelname"`   // pg: name
	IdxBlksRead  *int64  `json:"idx_blks_read" db:"idx_blks_read"` // pg: bigint
	IdxBlksHit   *int64  `json:"idx_blks_hit" db:"idx_blks_hit"`   // pg: bigint
}

type PgStatioUserIndexesPayload struct {
	Rows []PgStatioUserIndexesRow `json:"rows"`
}
