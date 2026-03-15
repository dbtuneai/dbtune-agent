package queries

import (
	"context"
	"fmt"
	"reflect"
	"sync"
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

// scannerCache stores one *pgxutil.Scanner per struct type, created on first use.
var scannerCache sync.Map // reflect.Type → *pgxutil.Scanner[T] (stored as any)

// getScanner returns the cached Scanner for type T, creating it on first access.
func getScanner[T any]() *pgxutil.Scanner[T] {
	var zero T
	t := reflect.TypeOf(zero)

	if v, ok := scannerCache.Load(t); ok {
		return v.(*pgxutil.Scanner[T])
	}

	s := pgxutil.NewScanner[T]()
	actual, _ := scannerCache.LoadOrStore(t, s)
	return actual.(*pgxutil.Scanner[T])
}

// CollectView is a generic helper that queries a pg catalog view and scans
// the results into a slice of structs. It uses a cached Scanner that
// pre-computes struct reflection once per type and caches column-to-field
// mappings per column layout, making it safe across PG version changes.
func CollectView[T any](pool *pgxpool.Pool, ctx context.Context, query string, viewName string) ([]T, error) {
	ctx, cancel := EnsureTimeout(ctx)
	defer cancel()
	rows, err := utils.QueryWithPrefix(pool, ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()

	scanner := getScanner[T]()
	return pgx.CollectRows(rows, scanner.Scan)
}
