package checks

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckStartupRequirements verifies all mandatory configuration and connectivity requirements.
// Returns nil if all checks pass, or an error describing the first failure.
func CheckStartupRequirements() error {
	// Check PostgreSQL connection URL
	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return fmt.Errorf("PostgreSQL connection URL not set or invalid: %w", err)
	}
	if pgConfig.ConnectionURL == "" {
		return fmt.Errorf("PostgreSQL connection URL is required but not set")
	}

	// Check dbtune server URL, API key, and database ID
	serverURLs, err := dbtune.CreateServerURLs()
	if err != nil {
		return fmt.Errorf("dbtune server configuration invalid: %w", err)
	}

	// Try to connect to the database
	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL database: %w", err)
	}
	defer dbpool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = dbpool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("unable to ping PostgreSQL database: %w", err)
	}

	err = pg.CheckPGStatStatements(dbpool)
	if err != nil {
		return fmt.Errorf("failed to query pg_stat_statements table, did you `CREATE EXTENSION pg_stat_statements;`: %w", err)
	}

	// Try to connect to the dbtune server using the existing heartbeat function
	commonAgent := agent.CreateCommonAgent()
	// Override the ServerURLs to use our validated ones
	commonAgent.ServerURLs = serverURLs

	err = commonAgent.SendHeartbeat()
	if err != nil {
		return fmt.Errorf("failed to connect to dbtune server: %w", err)
	}

	return nil
}
