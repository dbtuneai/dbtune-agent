package queries

// Select1 executes a trivial query to verify the PostgreSQL connection is alive.
//
// Used as a health check / liveness probe by WaitPostgresReady and connection
// validation logic.

import (
	"context"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Select1Query = `SELECT 1`

func Select1(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), Select1Query)
	return err
}
