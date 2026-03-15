package queries

import (
	"fmt"
	"regexp"
	"strings"
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
