package queries

// https://www.postgresql.org/docs/current/catalog-pg-constraint.html

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgConstraintRow represents a row from pg_constraint for user-schema constraints.
type PgConstraintRow struct {
	Oid           *Oid       `json:"oid" db:"oid"`
	ConName       *Name      `json:"conname" db:"conname"`
	ConType       *Text      `json:"contype" db:"contype"`
	ConRelID      *Oid       `json:"conrelid" db:"conrelid"`
	ConIndID      *Oid       `json:"conindid" db:"conindid"`
	ConfRelID     *Oid       `json:"confrelid" db:"confrelid"`
	ConKey        []Smallint `json:"conkey" db:"conkey"`
	ConfKey       []Smallint `json:"confkey" db:"confkey"`
	ConDeferrable *Boolean   `json:"condeferrable" db:"condeferrable"`
	ConDeferred   *Boolean   `json:"condeferred" db:"condeferred"`
	ConValidated  *Boolean   `json:"convalidated" db:"convalidated"`
}

const (
	PgConstraintName     = "pg_constraint"
	PgConstraintInterval = 5 * time.Minute
)

const pgConstraintQuery = `
SELECT
    c.oid,
    c.conname,
    c.contype::text AS contype,
    c.conrelid,
    c.conindid,
    c.confrelid,
    c.conkey,
    c.confkey,
    c.condeferrable,
    c.condeferred,
    c.convalidated
FROM pg_constraint c
JOIN pg_class rel ON rel.oid = c.conrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND c.contype IN ('p', 'u', 'f', 'x')
ORDER BY n.nspname, rel.relname, c.conname
`

func PgConstraintCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgConstraintRow](pool, prepareCtx, PgConstraintName, PgConstraintInterval, pgConstraintQuery, WithSkipUnchanged())
}
