package pg

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

// inferNumericType attempts to parse a string setting into a numeric Go type.
// Returns int64 if parseable as integer, float64 if parseable as float, or the
// original string if neither. This preserves JSON serialization format (200 not "200")
// without relying on the potentially unreliable vartype metadata.
func InferNumericType(setting interface{}) interface{} {
	s, ok := setting.(string)
	if !ok {
		return setting
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// PGVersion returns the version of the PostgreSQL instance
const PGVersionQuery = `
SELECT version();
`

// Example: 16.4
func PGVersion(pgPool *pgxpool.Pool) (string, error) {
	var pgVersion string
	versionRegex := regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), PGVersionQuery).Scan(&pgVersion)
	if err != nil {
		return "", err
	}
	matches := versionRegex.FindStringSubmatch(pgVersion)

	return matches[1], nil
}

// PGMajorVersion extracts the integer major version from a version string like "16.4".
func PGMajorVersion(version string) (int, error) {
	parts := strings.Split(version, ".")
	if len(parts) == 0 || parts[0] == "" {
		return 0, fmt.Errorf("empty version string")
	}
	v, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("parse major version from %q: %w", version, err)
	}
	return v, nil
}

const MaxConnectionsQuery = `
SELECT setting::integer FROM pg_settings WHERE  name = 'max_connections';
`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %w", err)
	}

	return maxConnections, nil
}

const SELECT_NUMERIC_SETTINGS = `
	SELECT name, setting, unit, vartype, context
	FROM pg_settings
	WHERE vartype IN ('real', 'integer');
`

const SELECT_NON_NUMERIC_SETTINGS = `
	SELECT name, setting, unit, vartype, context
	FROM pg_settings
	WHERE vartype NOT IN ('real', 'integer');
`

func GetActiveConfig(
	pool *pgxpool.Pool,
	ctx context.Context,
) (agent.ConfigArraySchema, error) {
	settings, err := queries.QueryPgSettings(pool, ctx)
	if err != nil {
		return nil, err
	}

	configRows := make(agent.ConfigArraySchema, 0, len(settings))
	for _, s := range settings {
		var unit interface{}
		if s.Unit != nil {
			unit = string(*s.Unit)
		}

		var setting interface{} = string(s.Setting)
		if s.Vartype == "real" || s.Vartype == "integer" {
			setting = InferNumericType(setting)
		}

		configRows = append(configRows, agent.PGConfigRow{
			Name:    string(s.Name),
			Setting: setting,
			Unit:    unit,
			Vartype: string(s.Vartype),
			Context: string(s.Context),
		})
	}
	return configRows, nil
}

const ReloadConfigQuery = `
SELECT pg_reload_conf();
`

func ReloadConfig(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), ReloadConfigQuery)
	if err != nil {
		return err
	}
	return nil
}

const AlterSystemQuery = `
ALTER SYSTEM SET %s = %s;
`

func AlterSystem(pgPool *pgxpool.Pool, name string, value string) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), fmt.Sprintf(AlterSystemQuery, name, value))
	if err != nil {
		return err
	}
	return nil
}

const AlterSystemResetQuery = `
ALTER SYSTEM RESET %s;

`

func AlterSystemReset(pgPool *pgxpool.Pool, name string) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), fmt.Sprintf(AlterSystemResetQuery, name))
	if err != nil {
		return err
	}
	return nil
}

const AlterDatabaseQuery = `
ALTER DATABASE %s SET %s = %s;
`

func AlterDatabase(pgPool *pgxpool.Pool, dbname string, name string, value string) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), fmt.Sprintf(AlterDatabaseQuery, dbname, name, value))
	if err != nil {
		return err
	}
	return nil
}

const DataDirectoryQuery = `
SHOW data_directory;
`

func DataDirectory(pgPool *pgxpool.Pool) (string, error) {
	var dataDir string
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), DataDirectoryQuery).Scan(&dataDir)
	if err != nil {
		return "", err
	}
	return dataDir, nil
}

const Select1Query = `
SELECT 1;
`

func Select1(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), Select1Query)
	return err
}

const CheckPGStatStatementsQuery = `
SELECT COUNT(*) FROM pg_stat_statements;
`

func CheckPGStatStatements(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), CheckPGStatStatementsQuery)
	return err
}
