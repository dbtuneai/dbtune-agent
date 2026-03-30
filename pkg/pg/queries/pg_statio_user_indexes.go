package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-INDEXES

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

// PgStatioUserIndexesRow represents a row from pg_statio_user_indexes.
type PgStatioUserIndexesRow struct {
	RelID        *Oid    `json:"relid" db:"relid"`
	IndexRelID   *Oid    `json:"indexrelid" db:"indexrelid"`
	SchemaName   *Name   `json:"schemaname" db:"schemaname"`
	RelName      *Name   `json:"relname" db:"relname"`
	IndexRelName *Name   `json:"indexrelname" db:"indexrelname"`
	IdxBlksRead  *Bigint `json:"idx_blks_read" db:"idx_blks_read"`
	IdxBlksHit   *Bigint `json:"idx_blks_hit" db:"idx_blks_hit"`
}

const (
	PgStatioUserIndexesName     = "pg_statio_user_indexes"
	PgStatioUserIndexesInterval = 1 * time.Minute

	// PgStatioUserIndexesBatchSize controls how many indexes are fetched per
	// tick. During backfill every row is emitted; during delta only rows with
	// changed idx_blks_read or idx_blks_hit are sent.
	PgStatioUserIndexesBatchSize = 2000
)

const pgStatioUserIndexesQuery = `
SELECT *
FROM pg_statio_user_indexes
ORDER BY indexrelid
LIMIT $1 OFFSET $2
`

// statioSnapshot holds the metrics we diff against.
type statioSnapshot struct {
	idxBlksRead int64
	idxBlksHit  int64
}

func PgStatioUserIndexesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx, batchSize int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgStatioUserIndexesRow]()

	var (
		offset       = 0
		backfillDone = false
		snapshot     = make(map[uint32]statioSnapshot)
	)

	return CatalogCollector{
		Name:     PgStatioUserIndexesName,
		Interval: PgStatioUserIndexesInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}

			rows, err := utils.QueryWithPrefix(pool, ctx, pgStatioUserIndexesQuery, batchSize, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %w", PgStatioUserIndexesName, err)
			}
			defer rows.Close()

			pageRows, err := pgx.CollectRows(rows, scanner.Scan)
			if err != nil {
				return nil, fmt.Errorf("failed to scan %s: %w", PgStatioUserIndexesName, err)
			}

			if len(pageRows) == 0 {
				if !backfillDone {
					backfillDone = true
				}
				offset = 0
				return nil, nil
			}

			offset += batchSize

			var emit []PgStatioUserIndexesRow

			if !backfillDone {
				// Backfill: emit everything, build snapshot.
				emit = pageRows
				for _, r := range pageRows {
					if r.IndexRelID != nil {
						snapshot[uint32(*r.IndexRelID)] = statioSnapshot{
							idxBlksRead: derefInt64(r.IdxBlksRead),
							idxBlksHit:  derefInt64(r.IdxBlksHit),
						}
					}
				}
			} else {
				// Delta: only emit rows where metrics changed.
				for _, r := range pageRows {
					if r.IndexRelID == nil {
						continue
					}
					key := uint32(*r.IndexRelID)
					cur := statioSnapshot{
						idxBlksRead: derefInt64(r.IdxBlksRead),
						idxBlksHit:  derefInt64(r.IdxBlksHit),
					}
					prev, exists := snapshot[key]
					if !exists || cur != prev {
						emit = append(emit, r)
					}
					snapshot[key] = cur
				}
			}

			if len(emit) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgStatioUserIndexesRow]{Rows: emit})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgStatioUserIndexesName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}

func derefInt64(p *Bigint) int64 {
	if p == nil {
		return 0
	}
	return int64(*p)
}
