package queries

// MaxConnections queries the max_connections setting from pg_settings.
//
// Returns the maximum number of concurrent connections the PostgreSQL server allows.
// Used to calculate connection pool sizing and report system capacity.
//
// https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxConnectionsQuery = `SELECT setting::integer FROM pg_settings WHERE name = 'max_connections'`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %w", err)
	}

	return maxConnections, nil
}
