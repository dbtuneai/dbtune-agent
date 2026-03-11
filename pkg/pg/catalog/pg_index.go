package catalog

// https://www.postgresql.org/docs/current/catalog-pg-index.html

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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
	SchemaName     *string  `json:"schemaname" db:"schemaname"`         // pg: name
	TableName      *string  `json:"tablename" db:"tablename"`           // pg: name
	IndexName      *string  `json:"indexname" db:"indexname"`            // pg: name
	IndexRelID     *int64   `json:"indexrelid" db:"indexrelid"`          // pg: oid
	IndRelID       *int64   `json:"indrelid" db:"indrelid"`             // pg: oid
	IndNatts       *int64   `json:"indnatts" db:"indnatts"`             // pg: int2
	IndNKeyAtts    *int64   `json:"indnkeyatts" db:"indnkeyatts"`      // pg: int2
	IndIsUnique    *bool    `json:"indisunique" db:"indisunique"`       // pg: bool
	IndIsPrimary   *bool    `json:"indisprimary" db:"indisprimary"`     // pg: bool
	IndIsExclusion *bool    `json:"indisexclusion" db:"indisexclusion"` // pg: bool
	IndImmediate   *bool    `json:"indimmediate" db:"indimmediate"`     // pg: bool
	IndIsClustered *bool    `json:"indisclustered" db:"indisclustered"` // pg: bool
	IndIsValid     *bool    `json:"indisvalid" db:"indisvalid"`         // pg: bool
	IndCheckXmin   *bool    `json:"indcheckxmin" db:"indcheckxmin"`     // pg: bool
	IndIsReady     *bool    `json:"indisready" db:"indisready"`         // pg: bool
	IndIsLive      *bool    `json:"indislive" db:"indislive"`           // pg: bool
	IndIsReplIdent *bool    `json:"indisreplident" db:"indisreplident"` // pg: bool
	RelTuples      *float64 `json:"reltuples" db:"reltuples"`           // pg: float4
	IndexDef       *string  `json:"indexdef" db:"indexdef"`              // pg: text
}

type PgIndexPayload struct {
	Rows []PgIndexRow `json:"rows"`
}

func CollectPgIndex(pgPool *pgxpool.Pool, ctx context.Context) ([]PgIndexRow, error) {
	return CollectView[PgIndexRow](pgPool, ctx, pgIndexQuery, "pg_index")
}

func NewPgIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) agent.CatalogCollector {
	return agent.CatalogCollector{
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
