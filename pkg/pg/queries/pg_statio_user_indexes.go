package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-INDEXES

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *Oid    `json:"relid" db:"relid"`
	IndexRelID   *Oid    `json:"indexrelid" db:"indexrelid"`
	SchemaName   *Name   `json:"schemaname" db:"schemaname"`
	RelName      *Name   `json:"relname" db:"relname"`
	IndexRelName *Name   `json:"indexrelname" db:"indexrelname"`
	IdxBlksRead  *Bigint `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit   *Bigint `json:"idx_blks_hit" db:"idx_blks_hit"`
}

const (
	PgStatioUserIndexesName     = "pg_statio_user_indexes"
	PgStatioUserIndexesInterval = 1 * time.Minute

	// PgStatioUserIndexesCategoryLimit caps each activity category in the UNION query.
	PgStatioUserIndexesCategoryLimit = 200
)

// BuildPgStatioUserIndexesQuery samples index I/O stats from three perspectives:
// disk reads, total I/O volume, and low cache-hit ratio.
// UNION deduplicates across categories automatically.
func BuildPgStatioUserIndexesQuery(categoryLimit int) string {
	return fmt.Sprintf(`
(SELECT * FROM pg_statio_user_indexes ORDER BY COALESCE(idx_blks_read,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_statio_user_indexes ORDER BY COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) DESC LIMIT %d)
UNION
(SELECT * FROM pg_statio_user_indexes WHERE COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0) > 0 ORDER BY COALESCE(idx_blks_hit,0)::float / (COALESCE(idx_blks_read,0) + COALESCE(idx_blks_hit,0)) ASC LIMIT %d)
`, categoryLimit, categoryLimit, categoryLimit)
}

func PgStatioUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, categoryLimit int) CatalogCollector {
	query := BuildPgStatioUserIndexesQuery(categoryLimit)
	return NewCollector[PgStatioUserIndexesRow](pool, prepareCtx, PgStatioUserIndexesName, PgStatioUserIndexesInterval, query)
}
