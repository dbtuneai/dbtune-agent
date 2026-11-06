package cloudsql

import (
	"context"
	"fmt"
	"log"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/api/sqladmin/v1"
)

type CloudSQLAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	PGDriver              *pgPool.Pool
	State                 *State
	CloudSQLConfig        Config
	CloudMonitoringClient *CloudMonitoringClient
	CloudSQLAdminClient   *SqlAdminClient
	GuardrailSettings     *config.Guardrails
	pgConfig              config.Postgres
	PGVersion             pg.Version
}

func CreateCloudSQLAdapter() (*CloudSQLAdapter, error) {
	ctx := context.Background()
	commonAgent := agent.CreateCommonAgent()

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	pgPool, err := pgPool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, err
	}

	cfg, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return nil, err
	}

	sqladminService, err := sqladmin.NewService(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to SQL Admin service: %v", err)
	}

	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	PGVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	commonAgent.DBPool = pgPool
	c := &CloudSQLAdapter{
		CommonAgent:    *commonAgent,
		State:          &State{LastGuardrailCheck: time.Now()},
		PGDriver:       pgPool,
		CloudSQLConfig: cfg,
		CloudMonitoringClient: &CloudMonitoringClient{
			client: client,
			ctx:    ctx,
		},
		CloudSQLAdminClient: &SqlAdminClient{
			client: sqladminService,
		},
		GuardrailSettings: &guardrailSettings,
		pgConfig:          pgConfig,
		PGVersion:         PGVersion,
	}

	collectors, err := catalog.StandardCollectors(pgPool, PGVersion, commonAgent.Logger())
	if err != nil {
		return nil, err
	}
	c.SetCatalogCollectors(collectors)
	c.InitCollectors(c.Collectors())

	return c, nil
}
