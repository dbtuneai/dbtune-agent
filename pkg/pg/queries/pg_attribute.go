package queries

// https://www.postgresql.org/docs/current/catalog-pg-attribute.html

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgAttributeRow represents a row from pg_attribute for user-table and index columns.
type PgAttributeRow struct {
	AttRelID       *Oid      `json:"attrelid" db:"attrelid"`
	AttName        *Name     `json:"attname" db:"attname"`
	AttTypID       *Bigint   `json:"atttypid" db:"atttypid"`
	AttLen         *Smallint `json:"attlen" db:"attlen"`
	AttNum         *Smallint `json:"attnum" db:"attnum"`
	AttStorage     *Text     `json:"attstorage" db:"attstorage"`
	AttCompression *Text     `json:"attcompression" db:"attcompression"` // PG14+; nil on older versions (column not in query)
	AttNotNull     *Boolean  `json:"attnotnull" db:"attnotnull"`
	AttStatTarget  *Integer  `json:"attstattarget" db:"attstattarget"`
	AttAlign       *Text     `json:"attalign" db:"attalign"`
	AttTypMod      *Integer  `json:"atttypmod" db:"atttypmod"`
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
    a.attnum::bigint AS attnum,
    a.attstorage::text AS attstorage,
    a.attnotnull,
    a.attstattarget::bigint AS attstattarget,
    a.attalign::text AS attalign,
    a.atttypmod::bigint AS atttypmod
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.relkind IN ('r', 'i')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum
`

func PgAttributeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgAttributeRow](pool, prepareCtx, PgAttributeName, PgAttributeInterval, pgAttributeQuery, WithSkipUnchanged())
}
