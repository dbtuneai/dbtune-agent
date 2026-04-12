package queries

// https://www.postgresql.org/docs/current/catalog-pg-attribute.html

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgAttributeRow represents a row from pg_attribute for user-table and index columns.
type PgAttributeRow struct {
	AttRelID *Oid      `json:"attrelid" db:"attrelid"`
	AttName  *Name     `json:"attname" db:"attname"`
	AttTypID *Bigint   `json:"atttypid" db:"atttypid"`
	AttLen   *Smallint `json:"attlen" db:"attlen"`
	AttNum   *Smallint `json:"attnum" db:"attnum"`
}

const (
	PgAttributeName     = "pg_attribute"
	PgAttributeInterval = 5 * time.Minute
)

// pgAttributeQuery returns pg_attribute rows for all user-table and index columns,
// filtered to user schemas and non-dropped, non-system columns.
const pgAttributeQuery = `
SELECT
    a.attrelid,
    a.attname,
    a.atttypid::bigint AS atttypid,
    a.attlen::bigint AS attlen,
    a.attnum::bigint AS attnum
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.relkind IN ('r', 'i')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum
`

// PgAttributeRegistration describes the pgattribute collector's configuration schema.
var PgAttributeRegistration = collectorconfig.CollectorRegistration{
	Name: PgAttributeName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgAttributeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgAttributeRow](pool, prepareCtx, PgAttributeName, PgAttributeInterval, pgAttributeQuery, WithSkipUnchanged())
}
