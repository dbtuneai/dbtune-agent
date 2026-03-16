package rds

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RDSAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	GuardrailSettings guardrails.Config
	pgConfig          pg.Config
	State             State
	AWSClients        AWSClients
	PGVersion         string
}

func CreateRDSAdapterWithoutCollectors(configKey *string) (*RDSAdapter, error) {
	var keyValue string
	if configKey == nil {
		keyValue = RDS_CONFIG_KEY
	} else {
		keyValue = *configKey
	}

	var err error
	var config Config
	config, err = ConfigFromViper(keyValue)
	if err != nil {
		return nil, fmt.Errorf("failed to bind config from key %s: %w", keyValue, err)
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	ctx := context.Background()

	// Create AWS config
	cfg, err := FetchAWSConfig(
		config.AWSAccessKey,
		config.AWSSecretAccessKey,
		config.AWSRegion,
		ctx,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}
	clients := NewAWSClients(cfg)

	// Check if RDS client can fetch the database instance correctly and the tokens work
	dbInfo, err := FetchDBInfo(config.RDSDatabaseIdentifier, &clients, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
	}

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	// PGVersion
	PGVersion, err := queries.PGVersion(dbpool)
	if err != nil {
		return nil, err
	}
	pgMajorVersion := pg.ParsePgMajorVersion(PGVersion)
	collectorsConfig, err := pg.CollectorsConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to load collectors config: %w", err)
	}
	adapter := &RDSAdapter{
		CommonAgent: *commonAgent,
		CatalogGetter: agent.CatalogGetter{
			PGPool:           dbpool,
			PGMajorVersion:   pgMajorVersion,
			PGConfig:         pgConfig,
			CollectorsConfig: collectorsConfig,
			HealthGate:       pg.NewHealthGate(dbpool, commonAgent.Logger()),
		},
		Config:   config,
		pgConfig: pgConfig,
		State: State{
			DBInfo: &dbInfo,
		},
		AWSClients:        clients,
		GuardrailSettings: guardrailSettings,
		PGVersion:         PGVersion,
	}
	return adapter, nil
}

func CreateRDSAdapter(configKey *string) (*RDSAdapter, error) {
	rdsAdapter, err := CreateRDSAdapterWithoutCollectors(configKey)
	if err != nil {
		return nil, err
	}
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return rdsAdapter, nil
}

func (adapter *RDSAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Info("Collecting system info")

	// Refreshes self
	dbInfo, err := FetchDBInfo(
		adapter.Config.RDSDatabaseIdentifier,
		&adapter.AWSClients,
		ctx,
	)
	if err != nil {
		return nil, err
	}
	adapter.State.DBInfo = &dbInfo
	adapter.State.LastDBInfoCheck = time.Now()

	// Get the RDSDB specific info
	info, err := adapter.State.DBInfo.TryIntoFlatValuesSlice()
	if err != nil {
		return nil, err
	}

	// PGVersion
	pgVersion, err := queries.PGVersion(adapter.PGPool)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion)
	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL version metric: %v", err)
		return nil, err
	}
	info = append(info, version)

	// MaxConnections
	maxConnections, err := queries.MaxConnections(adapter.PGPool)
	if err != nil {
		return nil, err
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)

	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL max connections metric: %v", err)
		return nil, err
	}
	info = append(info, maxConnectionsMetric)

	return info, nil
}

func (adapter *RDSAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	rows, err := queries.CollectPgSettings(adapter.PGPool, ctx, adapter.Logger())
	if err != nil {
		return nil, err
	}
	config := make(agent.ConfigArraySchema, len(rows))
	for i, r := range rows {
		config[i] = r
	}
	return config, nil
}

func (adapter *RDSAdapter) ApplyConfig(ctx context.Context, proposedConfig *agent.ProposedConfigResponse) error {
	// If the last applied config is less than 1 minute ago, return
	if adapter.State.LastAppliedConfig.Add(1 * time.Minute).After(time.Now()) {
		adapter.Logger().Info("Last applied config is less than 1 minute ago, skipping")
		return nil
	}

	err := ApplyConfig(
		proposedConfig,
		&adapter.AWSClients,
		adapter.Config.RDSParameterGroupName,
		adapter.Config.RDSDatabaseIdentifier,
		adapter.Logger(),
		ctx,
	)
	if err != nil {
		return fmt.Errorf("failed to apply config: %w", err)
	}

	// TODO(eddie): validate if this below comment is the case or
	// we were not waiting properly for parameter group changes

	// RDS has a race-condition/caching issue where the first fetches of config
	// after a restart are giving back the old config.
	// This results in re-applying the old recommended config.
	// To avoid this, we give a buffer of last applied config of 1 minute.

	// Instance is online, we validate that PostgreSQL is back online also
	adapter.Logger().Info("Waiting for PostgreSQL to come back online...")
	err = pg.WaitPostgresReady(adapter.PGPool)
	if err != nil {
		return fmt.Errorf("error waiting for PostgreSQL to come back online: %w", err)
	}

	adapter.State.LastAppliedConfig = time.Now()
	return nil
}

func (adapter *RDSAdapter) Collectors() []agent.MetricCollector {
	collectors := agent.DefaultMetricCollectors(adapter.PGPool, adapter.pgConfig)
	return append(collectors, agent.MetricCollector{
		Key: "hardware",
		Collector: RDSHardwareInfo(
			adapter.Config.RDSDatabaseIdentifier,
			&adapter.State,
			&adapter.AWSClients,
			adapter.Logger(),
		),
	})
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *RDSAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Info("Checking guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	totalMemoryBytes, err := adapter.State.DBInfo.TotalMemoryBytes()
	if err != nil {
		adapter.Logger().Errorf("Failed to get total memory bytes: %v", err)
		return nil
	}

	if adapter.State.DBInfo.PerformanceInsightsEnabled() {
		resourceID, err := adapter.State.DBInfo.ResourceID()
		if err != nil {
			adapter.Logger().Errorf("Failed to get resource ID: %v", err)
			return nil
		}

		memoryUsageBytes, err := GetMemoryUsageFromPI(
			&adapter.AWSClients,
			resourceID,
			adapter.Logger(),
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from PI: %v", err)
			return nil
		}

		memoryUsagePercent := (float64(memoryUsageBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Memory usage: %.2f%%", memoryUsagePercent)
		if memoryUsagePercent > adapter.GuardrailSettings.MemoryThreshold {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.Memory,
			}
		}
	} else {
		freeableMemoryBytes, err := GetFreeableMemoryFromCW(
			adapter.Config.RDSDatabaseIdentifier,
			&adapter.AWSClients,
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from CloudWatch: %v", err)
			return nil
		}
		freeableMemoryPercent := (float64(freeableMemoryBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Freeable memory: %.2f%%", freeableMemoryPercent)
		if freeableMemoryPercent < (100 - adapter.GuardrailSettings.MemoryThreshold) {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.FreeableMemory,
			}
		}
	}

	return nil
}

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
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return &AuroraRDSAdapter{*rdsAdapter}, nil
}
