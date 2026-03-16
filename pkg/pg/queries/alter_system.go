package queries

// AlterSystem modifies PostgreSQL server configuration parameters that persist
// across restarts by writing to postgresql.auto.conf.
//
// AlterSystemReset removes a previously set parameter, reverting it to the
// value in postgresql.conf.
//
// Both functions sanitize inputs to prevent SQL injection since ALTER SYSTEM
// does not support parameterized queries.
//
// Changes take effect after pg_reload_conf() for 'sighup' context parameters,
// or after a server restart for 'postmaster' context parameters.
//
// https://www.postgresql.org/docs/current/sql-altersystem.html

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AlterSystem(pgPool *pgxpool.Pool, name string, value string) error {
	safeName, err := SanitizeSettingName(name)
	if err != nil {
		return fmt.Errorf("alter system: %w", err)
	}
	safeValue, err := SanitizeSettingValue(value)
	if err != nil {
		return fmt.Errorf("alter system: %w", err)
	}
	query := fmt.Sprintf("ALTER SYSTEM SET %s = %s", safeName, safeValue)
	_, err = utils.ExecWithPrefix(pgPool, context.Background(), query)
	return err
}

func AlterSystemReset(pgPool *pgxpool.Pool, name string) error {
	safeName, err := SanitizeSettingName(name)
	if err != nil {
		return fmt.Errorf("alter system reset: %w", err)
	}
	query := fmt.Sprintf("ALTER SYSTEM RESET %s", safeName)
	_, err = utils.ExecWithPrefix(pgPool, context.Background(), query)
	return err
}
