package queries

// https://www.postgresql.org/docs/current/catalog-pg-attribute.html

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PgAttributeName     = "pg_attribute"
	PgAttributeInterval = 5 * time.Minute
)

// pgAttributeQuery returns pg_attribute rows for all user-table columns,
// filtered to user schemas and non-dropped, non-system columns.
// Provides atttypid and attlen needed by the platform for index bloat
// estimation (lib_indigo) and future column-level analysis.
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

// PgAttributeRow represents a row from pg_attribute for a user-table column.
type PgAttributeRow struct {
	AttRelID *Oid      `json:"attrelid" db:"attrelid"`
	AttName  *Name     `json:"attname" db:"attname"`
	AttTypID *Bigint   `json:"atttypid" db:"atttypid"`
	AttLen   *Smallint `json:"attlen" db:"attlen"`
	AttNum   *Smallint `json:"attnum" db:"attnum"`
}

type PgAttributePayload struct {
	Rows []PgAttributeRow `json:"rows"`
}

func CollectPgAttribute(pgPool *pgxpool.Pool, ctx context.Context) ([]PgAttributeRow, error) {
	return CollectView[PgAttributeRow](pgPool, ctx, pgAttributeQuery, "pg_attribute")
}

func PgAttributeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:          PgAttributeName,
		Interval:      PgAttributeInterval,
		SkipUnchanged: true,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgAttribute(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgAttributePayload{Rows: rows}, nil
		},
	}
}
