package aiven

import (
	"context"
	"fmt"
	"time"

	aivenclient "github.com/aiven/go-client-codegen"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

const (
	DEFAULT_SHARED_BUFFERS_PERCENTAGE = 20.0
	DEFAULT_PG_STAT_MONITOR_ENABLE    = false
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	Client            aivenclient.Client
	State             *State
	GuardrailSettings config.Guardrails
	pgConfig          config.Postgres
	PGDriver          *pgPool.Pool
	PGVersion         pg.Version
}

// CreateAivenPostgreSQLAdapter creates a new Aiven PostgreSQL adapter
func CreateAivenPostgreSQLAdapter() (*AivenPostgreSQLAdapter, error) {
	// Validate required configuration
	aivenConfig, err := ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	pgPool, err := pgPool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	// Create Aiven client
	aivenClient, err := aivenclient.NewClient(aivenclient.TokenOpt(aivenConfig.APIToken))
	if err != nil {
		return nil, fmt.Errorf("failed to create Aiven client: %w", err)
	}

	initialServiceLevelParameters, err := getInitialServiceLevelParameters(
		&aivenClient,
		aivenConfig.ProjectName,
		aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial values: %w", err)
	}
	state := &State{
		InitialSharedBuffersPercentage: initialServiceLevelParameters.InitialSharedBuffersPercentage,
		LastKnownPGStatMonitorEnable:   initialServiceLevelParameters.InitialPGStatMonitorEnable,
		LastAppliedConfig:              time.Time{},
		LastGuardrailCheck:             time.Time{},
		LastMemoryAvailableTime:        time.Time{},
		LastMemoryAvailablePercentage:  100.0,
		LastHardwareInfoTime:           time.Time{},
	}

	commonAgent := agent.CreateCommonAgent()

	PGVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	commonAgent.DBPool = pgPool
	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		CommonAgent:       *commonAgent,
		Config:            aivenConfig,
		Client:            aivenClient,
		State:             state,
		GuardrailSettings: guardrailSettings,
		pgConfig:          pgConfig,
		PGDriver:          pgPool,
		PGVersion:         PGVersion,
	}

	collectors, err := catalog.StandardCollectors(pgPool, PGVersion, commonAgent.Logger())
	if err != nil {
		return nil, err
	}
	adapter.SetCatalogCollectors(collectors)
	// Initialize collectors
	adapter.InitCollectors(AivenCollectors(adapter))

	return adapter, nil
}

type InitialServiceLevelParameters struct {
	InitialSharedBuffersPercentage float64
	InitialPGStatMonitorEnable     bool
}

func getInitialServiceLevelParameters(client *aivenclient.Client, projectName string, serviceName string) (InitialServiceLevelParameters, error) {
	aivenClient := *client
	service, err := aivenClient.ServiceGet(
		context.Background(),
		projectName,
		serviceName,
		[2]string{"include_secrets", "false"},
	)
	if err != nil {
		return InitialServiceLevelParameters{}, fmt.Errorf("failed to get service info: %w", err)
	}

	userConfig := service.UserConfig

	var sharedBuffers float64
	if val, ok := userConfig["shared_buffers_percentage"]; ok && val != nil {
		if f, ok := val.(float64); ok {
			sharedBuffers = f
		} else {
			sharedBuffers = DEFAULT_SHARED_BUFFERS_PERCENTAGE
		}
	} else {
		sharedBuffers = DEFAULT_SHARED_BUFFERS_PERCENTAGE
	}

	var pgStatMonitorEnable bool
	if val, ok := userConfig["pg_stat_monitor_enable"]; ok && val != nil {
		if b, ok := val.(bool); ok {
			pgStatMonitorEnable = b
		} else {
			pgStatMonitorEnable = DEFAULT_PG_STAT_MONITOR_ENABLE
		}
	} else {
		pgStatMonitorEnable = DEFAULT_PG_STAT_MONITOR_ENABLE
	}

	return InitialServiceLevelParameters{
		InitialSharedBuffersPercentage: sharedBuffers,
		InitialPGStatMonitorEnable:     pgStatMonitorEnable,
	}, nil
}
