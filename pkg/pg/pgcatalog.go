package pg

import (
	"strconv"
	"strings"
)

// ParsePgMajorVersion extracts the integer major version from a PG version string like "16.2".
func ParsePgMajorVersion(pgVersion string) int {
	parts := strings.Split(pgVersion, ".")
	if len(parts) == 0 {
		return 0
	}
	v, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0
	}
	return v
}
