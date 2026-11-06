package pgprem

import (
	"context"
	"fmt"
	"os"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"

	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

type DefaultPostgreSQLAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	pgDriver        *pgPool.Pool
	pgConfig        config.Postgres
	GuardrailConfig config.Guardrails
	PGVersion       pg.Version
}

func CreateDefaultPostgreSQLAdapter() (*DefaultPostgreSQLAdapter, error) {
	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, err
	}

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	if pgConfig.AllowRestart && pgConfig.ServiceName == "" && pgConfig.RestartScriptPath == "" {
		return nil, fmt.Errorf(
			"postgresql.allow_restart is true but neither postgresql.service_name nor postgresql.restart_script_path is configured. " +
				"Set postgresql.service_name (env: DBT_POSTGRESQL_SERVICE_NAME) " +
				"or set postgresql.restart_script_path (env: DBT_POSTGRESQL_RESTART_SCRIPT_PATH) to a custom restart script",
		)
	}

	if pgConfig.RestartScriptPath != "" {
		info, err := os.Stat(pgConfig.RestartScriptPath)
		if err != nil {
			return nil, fmt.Errorf(
				"postgresql.restart_script_path is set but %s is not accessible: %w",
				pgConfig.RestartScriptPath, err,
			)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("%s is a directory, expected an executable file", pgConfig.RestartScriptPath)
		}
		if info.Mode()&0o111 == 0 {
			return nil, fmt.Errorf("%s is not executable (mode %s); chmod +x it", pgConfig.RestartScriptPath, info.Mode())
		}
	}

	dbpool, err := pgPool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	commonAgent.DBPool = dbpool
	c := &DefaultPostgreSQLAdapter{
		CommonAgent:     *commonAgent,
		pgDriver:        dbpool,
		pgConfig:        pgConfig,
		GuardrailConfig: guardrailSettings,
		PGVersion:       PGVersion,
	}
	collectors, err := catalog.StandardCollectors(dbpool, PGVersion, commonAgent.Logger())
	if err != nil {
		return nil, err
	}
	c.SetCatalogCollectors(collectors)
	c.InitCollectors(DefaultCollectors())

	return c, nil
}
