package pg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
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

func WaitPostgresReady(pgPool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(1 * time.Second):
			// Try to execute a simple query
			_, err := utils.ExecWithPrefix(pgPool, ctx, queries.Select1Query)
			if err == nil {
				return nil
			}
		}
	}
}
