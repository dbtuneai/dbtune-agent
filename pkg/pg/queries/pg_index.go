package queries

// https://www.postgresql.org/docs/current/catalog-pg-index.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgIndexRow carries the fast-moving fields per index: stats (reltuples,
// relpages) and the lifecycle bools that flip during REINDEX, CREATE INDEX
// CONCURRENTLY, validation, and clustering. Definitional fields (indexdef,
// indkey, indisunique, etc.) are emitted by the separate pg_index_inventory
// collector on a slower cadence.
type PgIndexRow struct {
	SchemaName     *Name    `json:"schemaname" db:"schemaname"`
	IndexName      *Name    `json:"indexname" db:"indexname"`
	RelTuples      *Real    `json:"reltuples" db:"reltuples"`
	RelPages       *Integer `json:"relpages" db:"relpages"`
	IndIsValid     *Boolean `json:"indisvalid" db:"indisvalid"`
	IndIsReady     *Boolean `json:"indisready" db:"indisready"`
	IndIsLive      *Boolean `json:"indislive" db:"indislive"`
	IndImmediate   *Boolean `json:"indimmediate" db:"indimmediate"`
	IndIsClustered *Boolean `json:"indisclustered" db:"indisclustered"`
	IndIsReplIdent *Boolean `json:"indisreplident" db:"indisreplident"`
	IndCheckXmin   *Boolean `json:"indcheckxmin" db:"indcheckxmin"`
}

const (
	PgIndexName     = "pg_index"
	PgIndexInterval = 5 * time.Minute
)

const pgIndexQuery = `
SELECT
    n.nspname AS schemaname,
    c.relname AS indexname,
    c.reltuples::float8 AS reltuples,
    c.relpages,
    i.indisvalid,
    i.indisready,
    i.indislive,
    i.indimmediate,
    i.indisclustered,
    i.indisreplident,
    i.indcheckxmin
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, c.relname
`

// PgIndexCollector returns a CatalogCollector that emits the full
// pg_index volatile snapshot every tick.
func PgIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	scanner := pgxutil.NewScanner[PgIndexRow]()

	return CatalogCollector{
		Name:     PgIndexName,
		Interval: PgIndexInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			collectedAt := time.Now().UTC()

			querier := func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, pgIndexQuery)
			}
			rows, err := CollectView(querier, PgIndexName, scanner)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgIndexRow]{
				CollectedAt: collectedAt,
				Rows:        rows,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgIndexName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
