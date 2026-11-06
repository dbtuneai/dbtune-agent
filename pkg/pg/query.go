package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DBtuneQueryPrefix is the comment prefix added to all dbtune queries
	// to identify and filter them in pg_stat_statements
	DBtuneQueryPrefix = "/*dbtune*/"
)

// addPrefixToQuery trims leading whitespace from the query and ensures
// it starts with the dbtune prefix to prevent filtering issues.
func addPrefixToQuery(query string) string {
	trimmedQuery := strings.TrimSpace(query)
	if !strings.HasPrefix(trimmedQuery, DBtuneQueryPrefix) {
		trimmedQuery = DBtuneQueryPrefix + " " + trimmedQuery
	}
	return trimmedQuery
}

// QueryWithPrefix executes a query with the dbtune prefix.
func QueryWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return pool.Query(ctx, addPrefixToQuery(query), args...)
}

// QueryRowWithPrefix executes a query that returns a single row with the dbtune prefix.
func QueryRowWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) pgx.Row {
	return pool.QueryRow(ctx, addPrefixToQuery(query), args...)
}

// ExecWithPrefix executes a command with the dbtune prefix.
func ExecWithPrefix(pool *pgxpool.Pool, ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return pool.Exec(ctx, addPrefixToQuery(query), args...)
}

// CollectView queries a pg catalog view and scans the results into a slice
// of structs using the provided scanner. The querier function should set a deadline, eg via a context.
func CollectView[T any](querier func() (pgx.Rows, error), viewName string, scanner *pgxutil.Scanner[T]) ([]T, error) {
	rows, err := querier()
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, scanner.Scan)
}
