package patroni

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PatroniAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	PatroniConfig   Config
	PGDriver        *pgxpool.Pool
	GuardrailConfig config.Guardrails
	pgConfig        config.Postgres
	PGVersion       pg.Version
	HTTPClient      *http.Client
	State           *State
}

func CreatePatroniAdapter() (*PatroniAdapter, error) {
	ctx := context.Background()

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	patroniConfig, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	pgPool, err := pgxpool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	pgVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	guardrailConfig, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails: %w", err)
	}

	common := agent.CreateCommonAgent()
	common.DBPool = pgPool
	adpt := PatroniAdapter{
		CommonAgent:     *common,
		PatroniConfig:   patroniConfig,
		PGDriver:        pgPool,
		GuardrailConfig: guardrailConfig,
		pgConfig:        pgConfig,
		PGVersion:       pgVersion,
		HTTPClient:      &http.Client{Timeout: 60 * time.Second}, // Increased for restart operations
		State:           &State{},
	}

	collectors, err := catalog.StandardCollectors(pgPool, pgVersion, common.Logger())
	if err != nil {
		return nil, err
	}
	adpt.SetCatalogCollectors(collectors)
	adpt.InitCollectors(adpt.Collectors())

	// Initialize operations context for PostgreSQL queries
	// This context will be cancelled during failover to abort in-flight operations
	adpt.State.CreateOperationsContext()
	common.Logger().Info("Initialized operations context for PostgreSQL queries")

	// Initialize primary tracking at startup to establish baseline
	// This prevents false failover detection on first ApplyConfig call
	common.Logger().Info("Initializing primary node tracking...")
	if err := adpt.initializePrimaryTracking(ctx); err != nil {
		common.Logger().Warnf("Failed to initialize primary tracking: %v", err)
		// Don't fail agent startup - tracking will be established on first check
	}
	isStandby, err := adpt.isStandbyNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if node is standby: %w", err)
	}
	if isStandby {
		common.Logger().Warn("WARNING: Agent is running on a Standby node. Tuning commands will be rejected until this node becomes Primary.")
	}

	return &adpt, nil
}
