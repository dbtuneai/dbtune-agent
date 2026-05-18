package queries

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgIndexInventoryRow carries the slow-moving definitional fields for an
// index — everything that only changes on DDL. The fast-moving stats and
// lifecycle bools (reltuples, relpages, indisvalid, indisready, indislive,
// indimmediate, indisclustered, indisreplident, indcheckxmin) travel via
// the pg_index hot collector.
type PgIndexInventoryRow struct {
	SchemaName          *Name      `json:"schemaname" db:"schemaname"`
	TableName           *Name      `json:"tablename" db:"tablename"`
	IndexName           *Name      `json:"indexname" db:"indexname"`
	IndexRelID          *Oid       `json:"indexrelid" db:"indexrelid"`
	IndRelID            *Oid       `json:"indrelid" db:"indrelid"`
	IndNatts            *Smallint  `json:"indnatts" db:"indnatts"`
	IndNKeyAtts         *Smallint  `json:"indnkeyatts" db:"indnkeyatts"`
	IndIsUnique         *Boolean   `json:"indisunique" db:"indisunique"`
	IndNullsNotDistinct *Boolean   `json:"indnullsnotdistinct" db:"indnullsnotdistinct"` // PG15+
	IndIsPrimary        *Boolean   `json:"indisprimary" db:"indisprimary"`
	IndIsExclusion      *Boolean   `json:"indisexclusion" db:"indisexclusion"`
	IndexDef            *Text      `json:"indexdef" db:"indexdef"`
	IsPartial           *Boolean   `json:"is_partial" db:"is_partial"`
	IndPredSQL          *Text      `json:"indpred_sql" db:"indpred_sql"`
	IndKey              []Smallint `json:"indkey" db:"indkey"`
	IndOption           []Smallint `json:"indoption" db:"indoption"`
	IndClass            []Oid      `json:"indclass" db:"indclass"`
	IndCollation        []Oid      `json:"indcollation" db:"indcollation"`
	IndExprsSQL         *Text      `json:"indexprs_sql" db:"indexprs_sql"`
}

const (
	PgIndexInventoryName     = "pg_index_inventory"
	PgIndexInventoryInterval = 1 * time.Hour
)

// %s is the indnullsnotdistinct column (real column on PG15+, NULL literal below).
const pgIndexInventoryQueryFmt = `
SELECT
    n.nspname AS schemaname,
    t.relname AS tablename,
    c.relname AS indexname,
    i.indexrelid,
    i.indrelid,
    i.indnatts::bigint AS indnatts,
    i.indnkeyatts::bigint AS indnkeyatts,
    i.indisunique,
    %s AS indnullsnotdistinct,
    i.indisprimary,
    i.indisexclusion,
    pg_get_indexdef(i.indexrelid) AS indexdef,
    (i.indpred IS NOT NULL) AS is_partial,
    pg_get_expr(i.indpred, i.indrelid) AS indpred_sql,
    i.indkey::int2[] AS indkey,
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

func PgIndexInventoryCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, pgMajorVersion int) CatalogCollector {
	nullsNotDistinct := "NULL::bool"
	if pgMajorVersion >= 15 {
		nullsNotDistinct = "i.indnullsnotdistinct"
	}
	query := fmt.Sprintf(pgIndexInventoryQueryFmt, nullsNotDistinct)
	return NewCollector[PgIndexInventoryRow](
		pool, prepareCtx,
		PgIndexInventoryName, PgIndexInventoryInterval,
		query,
		WithSkipUnchanged(),
	)
}
