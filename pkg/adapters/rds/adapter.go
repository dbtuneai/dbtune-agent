package rds

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/catalog/collectorconfig"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RDSAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	GuardrailSettings config.Guardrails
	pgConfig          config.Postgres
	State             State
	AWSClients        AWSClients
	PGDriver          *pgxpool.Pool
	PGVersion         pg.Version
}

func CreateRDSAdapterWithoutCollectors(configKey *string) (*RDSAdapter, error) {
	var keyValue string
	if configKey == nil {
		keyValue = RDS_CONFIG_KEY
	} else {
		keyValue = *configKey
	}

	// Created first so config parsing can log through the agent's logger.
	commonAgent := agent.CreateCommonAgent()

	var err error
	var rdsConfig Config
	rdsConfig, err = ConfigFromViper(keyValue, commonAgent.Logger())
	if err != nil {
		return nil, fmt.Errorf("failed to bind config from key %s: %w", keyValue, err)
	}

	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	ctx := context.Background()

	// Create AWS config
	cfg, err := FetchAWSConfig(
		rdsConfig.AWSAccessKey,
		rdsConfig.AWSSecretAccessKey,
		rdsConfig.AWSRegion,
		ctx,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}
	clients := NewAWSClients(cfg)

	// Check if RDS client can fetch the database instance correctly and the tokens work
	dbInfo, err := FetchDBInfo(rdsConfig.RDSDatabaseIdentifier, &clients, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
	}

	// Every apply reads the parameter group before it writes anything, so prove
	// that access now rather than at the first apply.
	if dbInfo.ParameterGroupName != "" {
		if err := checkDescribeDBParametersAccess(ctx, clients.RDSClient, dbInfo.ParameterGroupName); err != nil {
			return nil, err
		}
	}

	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	// PGVersion
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, err
	}
	commonAgent.DBPool = dbpool
	adapter := &RDSAdapter{
		CommonAgent: *commonAgent,
		Config:      rdsConfig,
		pgConfig:    pgConfig,
		State: State{
			DBInfo: &dbInfo,
		},
		AWSClients:        clients,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         PGVersion,
	}
	adapter.Logger().Infof("detected parameter group %q for instance %q", dbInfo.ParameterGroupName, rdsConfig.RDSDatabaseIdentifier)
	if dbInfo.ClusterParameterGroupName != "" {
		adapter.Logger().Infof("detected cluster parameter group %q", dbInfo.ClusterParameterGroupName)
	}
	return adapter, nil
}

func CreateRDSAdapter(configKey *string) (*RDSAdapter, error) {
	rdsAdapter, err := CreateRDSAdapterWithoutCollectors(configKey)
	if err != nil {
		return nil, err
	}
	catalog, err := catalog.StandardCollectors(rdsAdapter.PGDriver, rdsAdapter.PGVersion, rdsAdapter.Logger())
	if err != nil {
		return nil, err
	}
	rdsAdapter.SetCatalogCollectors(catalog)
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return rdsAdapter, nil
}

func (adapter *RDSAdapter) refreshDBInfo(ctx context.Context) error {
	dbInfo, err := FetchDBInfo(
		adapter.Config.RDSDatabaseIdentifier,
		&adapter.AWSClients,
		ctx,
	)
	if err != nil {
		return err
	}
	adapter.State.DBInfo = &dbInfo
	adapter.State.LastDBInfoCheck = time.Now()
	return nil
}

// Timing for the pg_settings read-back. RDS propagates an immediate change
// within about a minute; past that it is not coming.
const (
	pgVerifyTimeout  = 90 * time.Second
	pgVerifyInterval = 5 * time.Second
	// For the one-off group read on the failure path.
	paramGroupReadTimeout = 30 * time.Second
)

// NOTE: For now, Aurora doesn't deviate from RDS in any functional way via API or how we
// query things. If it were to change, we can expand upon this.
type AuroraRDSAdapter struct {
	RDSAdapter
}

func CreateAuroraRDSAdapter() (*AuroraRDSAdapter, error) {
	configKey := AURORA_CONFIG_KEY
	rdsAdapter, err := CreateRDSAdapterWithoutCollectors(&configKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AuroraRDS adapter: %w", err)
	}
	cc, err := catalog.CollectorsConfigFromViper(rdsAdapter.Logger())
	if err != nil {
		return nil, fmt.Errorf("failed to parse collectors config: %w", err)
	}
	// Aurora does not support pg_stat_wal: the underlying pg_stat_get_wal()
	// function errors with "not supported in Aurora". AWS does not document
	// this publicly; see https://github.com/DataDog/integrations-core/issues/15890.
	// pg_stat_wal_receiver is unsupported for the same reason: its backing
	// function pg_stat_get_wal_receiver() also errors with "not supported in Aurora".
	rdsAdapter.Logger().Infof("Aurora: disabling %s and %s (not supported)", catalog.PgStatWalName, catalog.PgStatWalReceiverName)
	disabled := false
	cc.Simple[catalog.PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}
	cc.Simple[catalog.PgStatWalReceiverName] = collectorconfig.BaseConfig{Enabled: &disabled}
	catalog, err := catalog.CollectorsForVersion(rdsAdapter.PGDriver, rdsAdapter.PGVersion, cc, rdsAdapter.Logger())
	if err != nil {
		return nil, err
	}
	rdsAdapter.SetCatalogCollectors(catalog)
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return &AuroraRDSAdapter{*rdsAdapter}, nil
}
