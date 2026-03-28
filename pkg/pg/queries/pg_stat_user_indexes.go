package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-INDEXES

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

const (
	PgStatUserIndexesName     = "pg_stat_user_indexes"
	PgStatUserIndexesInterval = 1 * time.Minute

	// PgStatUserIndexesCategoryLimit caps each activity category in the UNION query.
	PgStatUserIndexesCategoryLimit = 200
)

// BuildPgStatUserIndexesQuery samples indexes from three perspectives:
// scan count, rows returned, and unused indexes.
// UNION deduplicates across categories automatically.
func BuildPgStatUserIndexesQuery(categoryLimit int) string {
	return fmt.Sprintf(`
(SELECT * FROM pg_stat_user_indexes ORDER BY COALESCE(idx_scan,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_indexes ORDER BY COALESCE(idx_tup_read,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_stat_user_indexes WHERE COALESCE(idx_scan,0) = 0 ORDER BY COALESCE(idx_tup_read,0) DESC LIMIT %d)
`, categoryLimit, categoryLimit, categoryLimit)
}

func PgStatUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, categoryLimit int) CatalogCollector {
	query := BuildPgStatUserIndexesQuery(categoryLimit)
	return NewCollector[PgStatUserIndexesRow](pool, prepareCtx, PgStatUserIndexesName, PgStatUserIndexesInterval, query)
}
