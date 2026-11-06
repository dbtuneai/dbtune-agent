package azureflex

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AzureFlexAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	AzureFlexConfig Config
	PGDriver        *pgxpool.Pool
	GuardrailConfig config.Guardrails
	pgConfig        config.Postgres
	PGVersion       pg.Version
}

func CreateAzureFlexAdapter() (*AzureFlexAdapter, error) {
	ctx := context.Background()
	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	cfg, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	pgPool, err := NewPool(ctx, pgConfig.ConnectionURL, cfg)
	if err != nil {
		return nil, err
	}

	pgVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, err
	}

	guardrailConfig, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	common := agent.CreateCommonAgent()
	common.DBPool = pgPool
	adpt := AzureFlexAdapter{
		CommonAgent:     *common,
		AzureFlexConfig: cfg,
		PGDriver:        pgPool,
		GuardrailConfig: guardrailConfig,
		pgConfig:        pgConfig,
		PGVersion:       pgVersion,
	}

	collectors, err := catalog.StandardCollectors(pgPool, pgVersion, common.Logger())
	if err != nil {
		return nil, err
	}
	adpt.SetCatalogCollectors(collectors)
	adpt.InitCollectors(adpt.Collectors())
	return &adpt, nil
}
