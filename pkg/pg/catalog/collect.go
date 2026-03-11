package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PrepareCtx is a hook called before each catalog query to allow adapters
// (e.g. CNPG, Patroni) to perform failover checks or replace the context.
type PrepareCtx func(ctx context.Context) (context.Context, error)

// DefaultQueryTimeout is the maximum time allowed for a single catalog query.
// This prevents stuck queries (e.g., due to lock contention) from hanging goroutines forever.
const DefaultQueryTimeout = 30 * time.Second

// EnsureTimeout returns a context with DefaultQueryTimeout if the given context
// has no deadline. If the context already has a deadline, it is returned as-is.
func EnsureTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, DefaultQueryTimeout)
}

// CollectView is a generic helper that queries a pg catalog view and scans
// the results into a slice of structs using pgx.RowToStructByNameLax.
// This eliminates boilerplate for the common pattern of query → scan → return.
func CollectView[T any](pool *pgxpool.Pool, ctx context.Context, query string, viewName string) ([]T, error) {
	ctx, cancel := EnsureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pool, ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
}
