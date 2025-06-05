package pg

import (
	"context"
	"fmt"
	"regexp"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// PGVersion returns the version of the PostgreSQL instance
const PGVersionQuery = `
/*dbtune*/
SELECT version();
`

// Example: 16.4
func PGVersion(pgPool *pgxpool.Pool) (string, error) {
	var pgVersion string
	versionRegex := regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
	err := pgPool.QueryRow(context.Background(), PGVersionQuery).Scan(&pgVersion)
	if err != nil {
		return "", err
	}
	matches := versionRegex.FindStringSubmatch(pgVersion)

	return matches[1], nil
}

const MaxConnectionsQuery = `
/*dbtune*/
SELECT setting::integer FROM pg_settings WHERE  name = 'max_connections';
`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := pgPool.QueryRow(context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %v", err)
	}

	return maxConnections, nil
}

const SELECT_NUMERIC_SETTINGS = `
	/*dbtune*/
	SELECT name, setting::numeric as setting, unit, vartype, context 
	FROM pg_settings 
	WHERE vartype IN ('real', 'integer');
`

const SELECT_NON_NUMERIC_SETTINGS = `
	/*dbtune*/
	SELECT name, setting, unit, vartype, context 
	FROM pg_settings 
	WHERE vartype NOT IN ('real', 'integer');
`

func GetActiveConfig(
	pool *pgxpool.Pool,
	ctx context.Context,
	logger *logrus.Logger,
) (agent.ConfigArraySchema, error) {
	var configRows agent.ConfigArraySchema

	// Query for numeric types (real and integer)
	numericRows, err := pool.Query(ctx, SELECT_NUMERIC_SETTINGS)
	if err != nil {
		return nil, err
	}

	for numericRows.Next() {
		var row agent.PGConfigRow
		err := numericRows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			logger.Error("Error scanning numeric row", err)
			continue
		}
		configRows = append(configRows, row)
	}

	// Query for non-numeric types
	nonNumericRows, err := pool.Query(ctx, SELECT_NON_NUMERIC_SETTINGS)
	if err != nil {
		return nil, err
	}

	for nonNumericRows.Next() {
		var row agent.PGConfigRow
		err := nonNumericRows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			logger.Error("Error scanning non-numeric row", err)
			continue
		}
		configRows = append(configRows, row)
	}

	return configRows, nil
}

const ReloadConfigQuery = `
/*dbtune*/
SELECT pg_reload_conf();
`

func ReloadConfig(pgPool *pgxpool.Pool) error {
	_, err := pgPool.Exec(context.Background(), ReloadConfigQuery)
	if err != nil {
		return err
	}
	return nil
}

const AlterSystemQuery = `
/*dbtune*/
ALTER SYSTEM SET %s = %s;
`

func AlterSystem(pgPool *pgxpool.Pool, name string, value string) error {
	_, err := pgPool.Exec(context.Background(), fmt.Sprintf(AlterSystemQuery, name, value))
	if err != nil {
		return err
	}
	return nil
}

const AlterDatabaseQuery = `
/*dbtune*/
ALTER DATABASE %s SET %s = %s;
`

func AlterDatabase(pgPool *pgxpool.Pool, dbname string, name string, value string) error {
	_, err := pgPool.Exec(context.Background(), fmt.Sprintf(AlterDatabaseQuery, dbname, name, value))
	if err != nil {
		return err
	}
	return nil
}

const DataDirectoryQuery = `
/*dbtune*/
SHOW data_directory;
`

func DataDirectory(pgPool *pgxpool.Pool) (string, error) {
	var dataDir string
	err := pgPool.QueryRow(context.Background(), DataDirectoryQuery).Scan(&dataDir)
	if err != nil {
		return "", err
	}
	return dataDir, nil
}

const Select1Query = `
/*dbtune*/
SELECT 1;
`

func Select1(pgPool *pgxpool.Pool) error {
	_, err := pgPool.Exec(context.Background(), Select1Query)
	if err != nil {
		return err
	}
	return nil
}
