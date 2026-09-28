package pg

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
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

// PGVersionQuery returns the version() string of the PostgreSQL instance
const PGVersionQuery = `
SELECT version();
`

var pgVersionRegex = regexp.MustCompile(`PostgreSQL (\d+)(?:\.(\d+))?`)

// Version is a parsed PostgreSQL server version. Pre-release servers
// ("PostgreSQL 18beta1", "18rc1", "19devel") have no minor, so HasMinor is
// false for them.
type Version struct {
	Major    int
	Minor    int
	HasMinor bool
}

// String renders the version as "major.minor", or just "major" when there is
// no minor. This is the format sent as the pg_version metric.
func (v Version) String() string {
	if !v.HasMinor {
		return strconv.Itoa(v.Major)
	}
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// PGVersion queries and parses the version of the PostgreSQL instance.
func PGVersion(pgPool *pgxpool.Pool) (Version, error) {
	var pgVersion string
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), PGVersionQuery).Scan(&pgVersion)
	if err != nil {
		return Version{}, err
	}
	return ParsePGVersion(pgVersion)
}

// ParsePGVersion extracts the major and minor version from a version() string.
func ParsePGVersion(versionString string) (Version, error) {
	matches := pgVersionRegex.FindStringSubmatch(versionString)
	if matches == nil {
		return Version{}, fmt.Errorf("unrecognized PostgreSQL version string: %q", versionString)
	}
	// The regex only matches digits, so Atoi can only fail on overflow.
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return Version{}, fmt.Errorf("parse major version from %q: %w", versionString, err)
	}
	v := Version{Major: major}
	if matches[2] != "" {
		v.Minor, err = strconv.Atoi(matches[2])
		if err != nil {
			return Version{}, fmt.Errorf("parse minor version from %q: %w", versionString, err)
		}
		v.HasMinor = true
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

// CurrentDatabaseQuery returns the name of the database the agent is connected to.
const CurrentDatabaseQuery = `
SELECT current_database();
`

// CurrentDatabase returns the name of the database the agent is connected to
// (i.e. the database being tuned).
func CurrentDatabase(pgPool *pgxpool.Pool) (string, error) {
	var currentDatabase string
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), CurrentDatabaseQuery).Scan(&currentDatabase)
	if err != nil {
		return "", fmt.Errorf("error getting current database: %w", err)
	}

	return currentDatabase, nil
}

// UserDatabaseCountQuery counts the non-template, connectable databases in the
// cluster. A count greater than 1 indicates the tuned database shares the
// cluster with other user databases.
const UserDatabaseCountQuery = `
SELECT count(*)::integer FROM pg_database WHERE NOT datistemplate AND datallowconn;
`

// UserDatabaseCount returns the number of non-template, connectable databases in
// the cluster.
func UserDatabaseCount(pgPool *pgxpool.Pool) (int, error) {
	var count int
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), UserDatabaseCountQuery).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error getting user database count: %w", err)
	}

	return count, nil
}

// DatabaseSystemInfo returns the system-info metrics describing which database
// the agent is tuning (pg_current_database) and how many user databases share
// the cluster (pg_database_count). This lets us tell whether the tuned database
// runs standalone or alongside other databases in the same cluster.
func DatabaseSystemInfo(pgPool *pgxpool.Pool) ([]metrics.FlatValue, error) {
	currentDatabase, err := CurrentDatabase(pgPool)
	if err != nil {
		return nil, err
	}

	databaseCount, err := UserDatabaseCount(pgPool)
	if err != nil {
		return nil, err
	}

	currentDatabaseMetric, err := metrics.PGCurrentDatabase.AsFlatValue(currentDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to create current database metric: %w", err)
	}

	databaseCountMetric, err := metrics.PGDatabaseCount.AsFlatValue(databaseCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create database count metric: %w", err)
	}

	return []metrics.FlatValue{currentDatabaseMetric, databaseCountMetric}, nil
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

const Select1Query = `
SELECT 1;
`

const CheckPGStatStatementsQuery = `
SELECT COUNT(*) FROM public.pg_stat_statements;
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
