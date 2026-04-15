package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DDLCollectorName     = "ddl"
	DDLCollectorInterval = 5 * time.Minute
)

type ddlPayload struct {
	DDL     string `json:"ddl"`
	DDLHash string `json:"ddl_hash"`
}

// DDLCollector emits the full DDL dump plus its hash. Payload is deduplicated
// via skipTracker so the backend still receives a heartbeat every
// skipUnchangedMultiplier intervals even when DDL is unchanged.
func DDLCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	tracker := newSkipTracker(skipUnchangedMultiplier)
	return CatalogCollector{
		Name:     DDLCollectorName,
		Interval: DDLCollectorInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			ddl, err := CollectDDL(pool, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to collect DDL: %w", err)
			}
			if ddl == "" {
				return nil, nil
			}
			data, err := json.Marshal(ddlPayload{DDL: ddl, DDLHash: HashDDL(ddl)})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal DDL: %w", err)
			}
			if tracker.shouldSkip(data) {
				return nil, nil
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
