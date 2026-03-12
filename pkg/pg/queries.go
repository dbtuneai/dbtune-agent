package pg

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// settingNameRegex matches valid PostgreSQL setting names: lowercase letters,
// digits, underscores, and dots (for extension settings like pg_stat_statements.max).
var settingNameRegex = regexp.MustCompile(`^[a-z][a-z0-9_.]*$`)

// SanitizeSettingName validates that a PostgreSQL setting name contains only
// safe characters. Setting names are identifiers like "shared_buffers" or
// "pg_stat_statements.max" and must not contain SQL metacharacters.
func SanitizeSettingName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("setting name must not be empty")
	}
	if !settingNameRegex.MatchString(name) {
		return "", fmt.Errorf("invalid setting name %q: must match [a-z][a-z0-9_.]*", name)
	}
	return name, nil
}

// dangerousChars are characters that must never appear in a setting value.
var dangerousChars = []string{";", "'", "--", "\\", "\n", "\r"}

// SanitizeSettingValue validates and quotes a PostgreSQL setting value.
// Values are single-quoted for use in ALTER SYSTEM SET statements.
func SanitizeSettingValue(value string) (string, error) {
	for _, ch := range dangerousChars {
		if strings.Contains(value, ch) {
			return "", fmt.Errorf("invalid setting value %q: contains forbidden character %q", value, ch)
		}
	}
	return "'" + value + "'", nil
}

// databaseNameRegex matches valid unquoted PostgreSQL identifiers.
var databaseNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

// SanitizeDatabaseName validates and double-quotes a database name for use
// in SQL statements like ALTER DATABASE.
func SanitizeDatabaseName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("database name must not be empty")
	}
	if !databaseNameRegex.MatchString(name) {
		return "", fmt.Errorf("invalid database name %q: must match [a-zA-Z0-9_-]+", name)
	}
	return `"` + name + `"`, nil
}

// InferNumericType attempts to parse a string setting into a numeric Go type.
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

const PGVersionQuery = `SELECT version()`

// PGVersion returns the version string (e.g. "16.4") of the PostgreSQL instance.
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

const MaxConnectionsQuery = `SELECT setting::integer FROM pg_settings WHERE name = 'max_connections'`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %w", err)
	}

	return maxConnections, nil
}

const SelectNumericSettings = `
SELECT name, setting, unit, vartype, context
FROM pg_settings
WHERE vartype IN ('real', 'integer')
`

const SelectNonNumericSettings = `
SELECT name, setting, unit, vartype, context
FROM pg_settings
WHERE vartype NOT IN ('real', 'integer')
`

// Deprecated: Use SelectNumericSettings instead.
const SELECT_NUMERIC_SETTINGS = SelectNumericSettings

// Deprecated: Use SelectNonNumericSettings instead.
const SELECT_NON_NUMERIC_SETTINGS = SelectNonNumericSettings

func GetActiveConfig(
	pool *pgxpool.Pool,
	ctx context.Context,
	logger *logrus.Logger,
) (agent.ConfigArraySchema, error) {
	var configRows agent.ConfigArraySchema

	// Query for numeric types (real and integer)
	numericRows, err := utils.QueryWithPrefix(pool, ctx, SelectNumericSettings)
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
		row.Setting = InferNumericType(row.Setting)
		configRows = append(configRows, row)
	}

	// Query for non-numeric types
	nonNumericRows, err := utils.QueryWithPrefix(pool, ctx, SelectNonNumericSettings)
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

const ReloadConfigQuery = `SELECT pg_reload_conf()`

func ReloadConfig(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), ReloadConfigQuery)
	if err != nil {
		return err
	}
	return nil
}

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

const DataDirectoryQuery = `SHOW data_directory`

func DataDirectory(pgPool *pgxpool.Pool) (string, error) {
	var dataDir string
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), DataDirectoryQuery).Scan(&dataDir)
	if err != nil {
		return "", err
	}
	return dataDir, nil
}

const Select1Query = `SELECT 1`

func Select1(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), Select1Query)
	return err
}

const CheckPGStatStatementsQuery = `SELECT COUNT(*) FROM pg_stat_statements`

func CheckPGStatStatements(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), CheckPGStatStatementsQuery)
	return err
}
