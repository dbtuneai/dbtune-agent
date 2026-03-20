package queries

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CatalogCollector defines a periodic catalog collection task.
type CatalogCollector struct {
	Name          string
	Interval      time.Duration
	Collect       func(ctx context.Context) (any, error)
	SkipUnchanged bool // When true, skip send if JSON hash matches previous
}

// PrepareCtx is a hook called before each catalog query to allow adapters
// (e.g. CNPG, Patroni) to perform failover checks or replace the context.
type PrepareCtx func(ctx context.Context) (context.Context, error)

// CollectView queries a pg catalog view and scans the results into a slice
// of structs using the provided scanner. The caller must set a deadline on ctx.
func CollectView[T any](pool *pgxpool.Pool, ctx context.Context, query string, viewName string, scanner *pgxutil.Scanner[T]) ([]T, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, scanner.Scan)
}
