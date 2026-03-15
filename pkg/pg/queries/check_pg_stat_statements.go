package queries

// CheckPGStatStatements verifies that the pg_stat_statements extension is
// installed and accessible by querying its row count.
//
// Called during startup checks to ensure the extension is loaded (via
// shared_preload_libraries) and the current user has permission to read it.
// Fails with a clear error if the extension is missing or not configured.
//
// https://www.postgresql.org/docs/current/pgstatstatements.html

import (
	"context"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const CheckPGStatStatementsQuery = `SELECT COUNT(*) FROM pg_stat_statements`

func CheckPGStatStatements(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), CheckPGStatStatementsQuery)
	return err
}
