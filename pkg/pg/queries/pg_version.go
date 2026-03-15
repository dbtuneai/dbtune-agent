package queries

// PGVersion queries the PostgreSQL server version string.
//
// Returns a short version like "16.4" extracted from the full version() output.
// Used during adapter startup to determine feature availability and version-gated behavior.
//
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-SESSION

import (
	"context"
	"regexp"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
