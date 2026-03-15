package queries

// DatabaseSize queries the total size of all databases in the PostgreSQL instance.
//
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DatabaseSizeName     = "system_db_size"
	DatabaseSizeInterval = 5 * time.Second
)

const DatabaseSizeQuery = `SELECT sum(pg_database_size(datname)) as total_size_bytes FROM pg_database`

// CollectDatabaseSize returns the total size of all databases in bytes.
func CollectDatabaseSize(pool *pgxpool.Pool, ctx context.Context) (int64, error) {
	var totalSizeBytes int64
	err := utils.QueryRowWithPrefix(pool, ctx, DatabaseSizeQuery).Scan(&totalSizeBytes)
	return totalSizeBytes, err
}

type DatabaseSizePayload struct {
	TotalSizeBytes int64 `json:"total_size_bytes"`
}

func DatabaseSizeCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return CatalogCollector{
		Name:     DatabaseSizeName,
		Interval: DatabaseSizeInterval,
		Collect: func(ctx context.Context) (any, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			size, err := CollectDatabaseSize(pool, ctx)
			if err != nil {
				return nil, err
			}
			return &DatabaseSizePayload{TotalSizeBytes: size}, nil
		},
	}
}
