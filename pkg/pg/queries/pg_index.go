package queries

// https://www.postgresql.org/docs/current/catalog-pg-index.html

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
    pg_get_indexdef(i.indexrelid) AS indexdef
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, t.relname, c.relname
`

// PgIndexRow represents a row from pg_index joined with pg_class/pg_namespace.
type PgIndexRow struct {
	SchemaName     *Name     `json:"schemaname" db:"schemaname"`
	TableName      *Name     `json:"tablename" db:"tablename"`
	IndexName      *Name     `json:"indexname" db:"indexname"`
	IndexRelID     *Oid      `json:"indexrelid" db:"indexrelid"`
	IndRelID       *Oid      `json:"indrelid" db:"indrelid"`
	IndNatts       *Smallint `json:"indnatts" db:"indnatts"`
	IndNKeyAtts    *Smallint `json:"indnkeyatts" db:"indnkeyatts"`
	IndIsUnique    *Boolean  `json:"indisunique" db:"indisunique"`
	IndIsPrimary   *Boolean  `json:"indisprimary" db:"indisprimary"`
	IndIsExclusion *Boolean  `json:"indisexclusion" db:"indisexclusion"`
	IndImmediate   *Boolean  `json:"indimmediate" db:"indimmediate"`
	IndIsClustered *Boolean  `json:"indisclustered" db:"indisclustered"`
	IndIsValid     *Boolean  `json:"indisvalid" db:"indisvalid"`
	IndCheckXmin   *Boolean  `json:"indcheckxmin" db:"indcheckxmin"`
	IndIsReady     *Boolean  `json:"indisready" db:"indisready"`
	IndIsLive      *Boolean  `json:"indislive" db:"indislive"`
	IndIsReplIdent *Boolean  `json:"indisreplident" db:"indisreplident"`
	RelTuples      *Real     `json:"reltuples" db:"reltuples"`
	IndexDef       *Text     `json:"indexdef" db:"indexdef"`
}

type PgIndexPayload struct {
	Rows []PgIndexRow `json:"rows"`
}

func CollectPgIndex(pgPool *pgxpool.Pool, ctx context.Context) ([]PgIndexRow, error) {
	return CollectView[PgIndexRow](pgPool, ctx, pgIndexQuery, "pg_index")
}

func PgIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:          PgIndexName,
		Interval:      PgIndexInterval,
		SkipUnchanged: true,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgIndex(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &PgIndexPayload{Rows: rows}, nil
		},
	}
}
