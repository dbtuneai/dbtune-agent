package docker

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/moby/moby/client"
)

// DockerContainerAdapter works with the container name and by
// communicating with the docker Unix socket to get stats like memory usage,
// number of CPUs available and memory limit
type DockerContainerAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	dockerClient      *client.Client
	GuardrailSettings config.Guardrails
	pgConfig          config.Postgres
	PGDriver          *pgxpool.Pool
	PGVersion         pg.Version
}

func CreateDockerContainerAdapter() (*DockerContainerAdapter, error) {
	// Get required configuration
	dockerConfig, err := ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for docker %w", err)
	}

	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	// Create Docker client
	cli, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	commonAgent.DBPool = dbpool
	dockerAdapter := &DockerContainerAdapter{
		CommonAgent:       *commonAgent,
		Config:            dockerConfig,
		dockerClient:      cli,
		GuardrailSettings: guardrailSettings,
		pgConfig:          pgConfig,
		PGDriver:          dbpool,
		PGVersion:         PGVersion,
	}
	catalogCollectors, err := catalog.StandardCollectors(dbpool, PGVersion, commonAgent.Logger())
	if err != nil {
		return nil, err
	}
	dockerAdapter.SetCatalogCollectors(catalogCollectors)
	collectors := DockerCollectors(dockerAdapter)
	dockerAdapter.InitCollectors(collectors)

	return dockerAdapter, nil
}
