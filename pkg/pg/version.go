package pg

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
