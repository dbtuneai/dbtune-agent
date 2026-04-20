package queries

// https://www.postgresql.org/docs/current/catalog-pg-type.html

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgTypeRow represents a row from pg_type.
type PgTypeRow struct {
	Oid          *Oid      `json:"oid" db:"oid"`
	TypName      *Name     `json:"typname" db:"typname"`
	TypNamespace *Oid      `json:"typnamespace" db:"typnamespace"`
	TypLen       *Smallint `json:"typlen" db:"typlen"`
	TypByVal     *Boolean  `json:"typbyval" db:"typbyval"`
	TypAlign     *Text     `json:"typalign" db:"typalign"`
	TypStorage   *Text     `json:"typstorage" db:"typstorage"`
	TypCategory  *Text     `json:"typcategory" db:"typcategory"`
	TypType      *Text     `json:"typtype" db:"typtype"`
	TypBaseType  *Oid      `json:"typbasetype" db:"typbasetype"`
	TypElem      *Oid      `json:"typelem" db:"typelem"`
	TypRelID     *Oid      `json:"typrelid" db:"typrelid"`
}

const (
	PgTypeName     = "pg_type"
	PgTypeInterval = 5 * time.Minute
)

// pgTypeQuery returns built-in types (stable OIDs < 10000) and user-defined
// types. pg_toast types are excluded as they have no diagnostic value.
const pgTypeQuery = `
SELECT
    t.oid,
    t.typname,
    t.typnamespace,
    t.typlen::bigint AS typlen,
    t.typbyval,
    t.typalign::text AS typalign,
    t.typstorage::text AS typstorage,
    t.typcategory::text AS typcategory,
    t.typtype::text AS typtype,
    t.typbasetype,
    t.typelem,
    t.typrelid
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.typisdefined
  AND (n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       OR t.oid < 10000)
ORDER BY n.nspname, t.typname
`

func PgTypeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgTypeRow](pool, prepareCtx, PgTypeName, PgTypeInterval, pgTypeQuery, WithSkipUnchanged())
}
