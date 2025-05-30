package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func WaitPostgresReady(pgPool *pgxpool.Pool, ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(1 * time.Second):
			// Try to execute a simple query
			_, err := pgPool.Exec(ctx, Select1Query)
			if err == nil {
				return nil
			}
		}
	}
}
