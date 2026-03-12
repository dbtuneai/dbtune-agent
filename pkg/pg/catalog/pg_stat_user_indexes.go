package catalog

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-INDEXES

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgStatUserIndexesName     = "pg_stat_user_indexes"
	PgStatUserIndexesInterval = 1 * time.Minute

	// PgStatUserIndexesCategoryLimit caps each activity category in the UNION
	// query. See PgStatUserTablesCategoryLimit for rationale.
	PgStatUserIndexesCategoryLimit = 200
)

// pgStatUserIndexesQuery samples the most interesting indexes from three
// complementary perspectives:
//
//  1. Scan count (idx_scan) — the most-used indexes, directly relevant for
//     performance analysis and index recommendation.
//
//  2. Rows returned (idx_tup_read) — indexes doing the most work per scan.
//     A low-scan but high-tup-read index may indicate expensive range scans
//     worth investigating.
//
//  3. Unused indexes (idx_scan = 0, then by idx_tup_read) — indexes that
//     have never been scanned are candidates for dropping to reduce write
//     overhead and bloat. These would be missed entirely by scan-based
//     ordering alone.
//
// UNION deduplicates across categories automatically.
var pgStatUserIndexesQuery = fmt.Sprintf(`
(SELECT * FROM pg_stat_user_indexes ORDER BY COALESCE(idx_scan,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_indexes ORDER BY COALESCE(idx_tup_read,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_indexes WHERE COALESCE(idx_scan,0) = 0 ORDER BY COALESCE(idx_tup_read,0) DESC LIMIT %d)
`, PgStatUserIndexesCategoryLimit, PgStatUserIndexesCategoryLimit, PgStatUserIndexesCategoryLimit)

func CollectPgStatUserIndexes(pgPool *pgxpool.Pool, ctx context.Context) ([]PgStatUserIndexesRow, error) {
	return CollectView[PgStatUserIndexesRow](pgPool, ctx, pgStatUserIndexesQuery, "pg_stat_user_indexes")
}

func NewPgStatUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
		Name:     PgStatUserIndexesName,
		Interval: PgStatUserIndexesInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserIndexes(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgStatUserIndexesPayload{Rows: rows}, nil
		},
	}
}

// PgStatUserIndexesRow represents a row from pg_stat_user_indexes.
type PgStatUserIndexesRow struct {
	RelID        *Oid         `json:"relid" db:"relid"`
	IndexRelID   *Oid         `json:"indexrelid" db:"indexrelid"`
	SchemaName   *Name        `json:"schemaname" db:"schemaname"`
	RelName      *Name        `json:"relname" db:"relname"`
	IndexRelName *Name        `json:"indexrelname" db:"indexrelname"`
	IdxScan      *Bigint      `json:"idx_scan" db:"idx_scan"`
	LastIdxScan  *TimestampTZ `json:"last_idx_scan" db:"last_idx_scan"`
	IdxTupRead   *Bigint      `json:"idx_tup_read" db:"idx_tup_read"`
	IdxTupFetch  *Bigint      `json:"idx_tup_fetch" db:"idx_tup_fetch"`
}

type PgStatUserIndexesPayload struct {
	Rows []PgStatUserIndexesRow `json:"rows"`
}
