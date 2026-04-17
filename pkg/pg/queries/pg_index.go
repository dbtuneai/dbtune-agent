package queries

// https://www.postgresql.org/docs/current/catalog-pg-index.html

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgIndexRow represents a row from pg_index joined with pg_class/pg_namespace.
type PgIndexRow struct {
	SchemaName     *Name      `json:"schemaname" db:"schemaname"`
	TableName      *Name      `json:"tablename" db:"tablename"`
	IndexName      *Name      `json:"indexname" db:"indexname"`
	IndexRelID     *Oid       `json:"indexrelid" db:"indexrelid"`
	IndRelID       *Oid       `json:"indrelid" db:"indrelid"`
	IndNatts       *Smallint  `json:"indnatts" db:"indnatts"`
	IndNKeyAtts    *Smallint  `json:"indnkeyatts" db:"indnkeyatts"`
	IndIsUnique    *Boolean   `json:"indisunique" db:"indisunique"`
	IndIsPrimary   *Boolean   `json:"indisprimary" db:"indisprimary"`
	IndIsExclusion *Boolean   `json:"indisexclusion" db:"indisexclusion"`
	IndImmediate   *Boolean   `json:"indimmediate" db:"indimmediate"`
	IndIsClustered *Boolean   `json:"indisclustered" db:"indisclustered"`
	IndIsValid     *Boolean   `json:"indisvalid" db:"indisvalid"`
	IndCheckXmin   *Boolean   `json:"indcheckxmin" db:"indcheckxmin"`
	IndIsReady     *Boolean   `json:"indisready" db:"indisready"`
	IndIsLive      *Boolean   `json:"indislive" db:"indislive"`
	IndIsReplIdent *Boolean   `json:"indisreplident" db:"indisreplident"`
	RelTuples      *Real      `json:"reltuples" db:"reltuples"`
	RelPages       *Integer   `json:"relpages" db:"relpages"`
	IndexDef       *Text      `json:"indexdef" db:"indexdef"`
	IsPartial      *Boolean   `json:"is_partial" db:"is_partial"`
	IndPredSQL     *Text      `json:"indpred_sql" db:"indpred_sql"`
	IndOption      []Smallint `json:"indoption" db:"indoption"`
	IndClass       []Oid      `json:"indclass" db:"indclass"`
	IndCollation   []Oid      `json:"indcollation" db:"indcollation"`
	IndExprsSQL    *Text      `json:"indexprs_sql" db:"indexprs_sql"`
}

const (
	PgIndexName     = "pg_index"
	PgIndexInterval = 5 * time.Minute
)

// Joined with pg_class/pg_namespace for human-readable names.
// reltuples cast to float8 to avoid float32 scanning issues.
// Filtered to user schemas only.
const pgIndexQuery = `
SELECT
    n.nspname AS schemaname,
    t.relname AS tablename,
    c.relname AS indexname,
    i.indexrelid,
    i.indrelid,
    i.indnatts::bigint AS indnatts,
    i.indnkeyatts::bigint AS indnkeyatts,
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    i.indimmediate,
    i.indisclustered,
    i.indisvalid,
    i.indcheckxmin,
    i.indisready,
    i.indislive,
    i.indisreplident,
    c.reltuples::float8 AS reltuples,
    c.relpages,
    pg_get_indexdef(i.indexrelid) AS indexdef,
    (i.indpred IS NOT NULL) AS is_partial,
    pg_get_expr(i.indpred, i.indrelid) AS indpred_sql,
    i.indoption::int2[] AS indoption,
    i.indclass::oid[] AS indclass,
    i.indcollation::oid[] AS indcollation,
    pg_get_expr(i.indexprs, i.indrelid) AS indexprs_sql
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, t.relname, c.relname
`

func PgIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgIndexRow](pool, prepareCtx, PgIndexName, PgIndexInterval, pgIndexQuery, WithSkipUnchanged())
}
