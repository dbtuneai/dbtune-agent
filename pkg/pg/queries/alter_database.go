package queries

// AlterDatabase sets a configuration parameter for a specific database.
//
// Unlike ALTER SYSTEM (which is global), ALTER DATABASE ... SET applies the
// parameter only to sessions connected to the named database. The setting is
// stored in pg_db_role_setting and takes effect on new connections.
//
// https://www.postgresql.org/docs/current/sql-alterdatabase.html

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AlterDatabase(pgPool *pgxpool.Pool, dbname string, name string, value string) error {
	safeDB, err := SanitizeDatabaseName(dbname)
	if err != nil {
		return fmt.Errorf("alter database: %w", err)
	}
	safeName, err := SanitizeSettingName(name)
	if err != nil {
		return fmt.Errorf("alter database: %w", err)
	}
	safeValue, err := SanitizeSettingValue(value)
	if err != nil {
		return fmt.Errorf("alter database: %w", err)
	}
	query := fmt.Sprintf("ALTER DATABASE %s SET %s = %s", safeDB, safeName, safeValue)
	_, err = utils.ExecWithPrefix(pgPool, context.Background(), query)
	return err
}
