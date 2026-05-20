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

func GetActiveConfig(
	pool *pgxpool.Pool,
	ctx context.Context,
) (agent.ConfigArraySchema, error) {
	settings, err := queries.QueryPgSettings(pool, ctx)
	if err != nil {
		return nil, err
	}
	return settingsToConfigRows(settings), nil
}

// settingsToConfigRows converts raw PgSettingsRow values into the
// ConfigArraySchema consumed by the tuning loop. Numeric vartypes
// have InferNumericType applied; NULL units become nil.
func settingsToConfigRows(settings []queries.PgSettingsRow) agent.ConfigArraySchema {
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
	return configRows
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

const restartRequiredParamsQuery = `SELECT name FROM pg_settings WHERE name = ANY($1) AND context = 'postmaster'`

// ValidateRestartPolicy queries which of the supplied parameter names would
// require a PostgreSQL restart and enforces the agent's apply policy:
//
//   - if at least one requires a restart but agent.IsRestartAllowed is false,
//     a *agent.RestartNotAllowedError is returned;
//   - if at least one requires a restart but KnobApplication=reload, the apply
//     is refused with a plain error to prevent partial application.
//
// The boolean result reports whether any of the parameters require a restart
// (used by callers to decide whether to perform one). On a policy violation
// the boolean is still true so the caller can log accurately if it wishes.
func ValidateRestartPolicy(
	pgPool *pgxpool.Pool,
	ctx context.Context,
	parameterNames []string,
	knobApp agent.KnobApplication,
) (bool, error) {
	restartRequired, err := RestartRequiredParams(pgPool, ctx, parameterNames)
	if err != nil {
		return false, fmt.Errorf("failed to validate which parameters require restart: %w", err)
	}
	if len(restartRequired) == 0 {
		return false, nil
	}
	if !agent.IsRestartAllowed() {
		return true, &agent.RestartNotAllowedError{
			Message: fmt.Sprintf("restart is not allowed in the agent, but %d parameter(s) require restart: %v", len(restartRequired), restartRequired),
		}
	}
	if knobApp == agent.KnobApplicationReload {
		return true, fmt.Errorf("refusing to apply: KnobApplication=reload but %d parameter(s) require restart: %v", len(restartRequired), restartRequired)
	}
	return true, nil
}

// RestartRequiredParams returns the subset of the supplied parameter names
// whose value cannot be changed without restarting PostgreSQL (i.e.
// pg_settings.context = 'postmaster').
func RestartRequiredParams(pgPool *pgxpool.Pool, ctx context.Context, parameterNames []string) ([]string, error) {
	if len(parameterNames) == 0 {
		return nil, nil
	}

	rows, err := utils.QueryWithPrefix(pgPool, ctx, restartRequiredParamsQuery, parameterNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_settings for restart-required parameters: %w", err)
	}
	defer rows.Close()

	var restartParams []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan restart-required parameter name: %w", err)
		}
		restartParams = append(restartParams, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_settings results: %w", err)
	}
	return restartParams, nil
}
