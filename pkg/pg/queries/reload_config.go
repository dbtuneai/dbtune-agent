package queries

// ReloadConfig signals the PostgreSQL server to reload its configuration files
// (postgresql.conf, pg_hba.conf, etc.) without a restart.
//
// Equivalent to running `pg_ctl reload` or sending SIGHUP to the postmaster.
// Used after ALTER SYSTEM SET to apply changes to parameters with context 'sighup'.
//
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL

import (
	"context"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ReloadConfigQuery = `SELECT pg_reload_conf()`

func ReloadConfig(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), ReloadConfigQuery)
	if err != nil {
		return err
	}
	return nil
}
