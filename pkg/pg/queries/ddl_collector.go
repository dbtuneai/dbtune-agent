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
	DDL string `json:"ddl"`
}

// DDLCollector emits the full DDL dump in the body; its hash travels as a
// query parameter (see CollectResult.hashOverride). Payload is deduplicated
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
			data, err := json.Marshal(ddlPayload{DDL: ddl})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal DDL: %w", err)
			}
			if tracker.shouldSkip(data) {
				return nil, nil
			}
			// The DDL hash travels in the query string (not the body) so the
			// backend can skip the large DDL payload when it is unchanged.
			return &CollectResult{JSON: data, hashOverride: HashDDL(ddl)}, nil
		},
	}
}
