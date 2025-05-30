package rds

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/keywords"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RDSAdapter struct {
	agent.CommonAgent
	Config            Config
	GuardrailSettings guardrails.Config
	State             State
	AWSClients        AWSClients
	PGDriver          *pgxpool.Pool
}

func CreateRDSAdapter(configKey *string) (*RDSAdapter, error) {
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
		return nil, fmt.Errorf("failed to bind RDS config: %w", err)
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for gaurdrails %w", err)
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
		return nil, fmt.Errorf("unable to load AWS config: %v", err)
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

	c := &RDSAdapter{
		CommonAgent: *commonAgent,
		Config:      config,
		State: State{
			DBInfo: &dbInfo,
		},
		AWSClients:        clients,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
	}
	collectors := c.Collectors()
	c.InitCollectors(collectors)
	return c, nil
}

func (adapter *RDSAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	// Refreshes self
	dbInfo, err := FetchDBInfo(
		adapter.Config.RDSDatabaseIdentifier,
		&adapter.AWSClients,
		context.Background(),
	)
	if err != nil {
		return nil, err
	}
	// NOTE: It's important we have the `*` here. We effectively want to replace
	// whatever was at the memory address of the old `DBInfo` with the new one, such
	// that anything holding onto a `*DBInfo` (and hence uses the memory address)
	// will see the new value we replaced it with. Otherwise, they would never see the
	// new value.
	adapter.State.DBInfo = &dbInfo
	adapter.State.LastDBInfoCheck = time.Now()

	// Get the RDSDB specific info
	info, err := adapter.State.DBInfo.TryIntoFlatValuesSlice()
	if err != nil {
		return nil, err
	}

	// Get the PostgreSQL specific info
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Warnf("Failed to get PostgreSQL version: %v", err)
	} else {
		version, err := utils.NewMetric(keywords.PGVersion, pgVersion, utils.String)
		if err != nil {
			adapter.Logger().Errorf("Failed to create PostgreSQL version metric: %v", err)
		} else {
			info = append(info, version)
		}
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Warnf("Failed to get PostgreSQL max connections: %v", err)
	} else {
		maxConnectionsMetric, err := utils.NewMetric(keywords.PGMaxConnections, maxConnections, utils.Int)
		if err != nil {
			adapter.Logger().Errorf("Failed to create PostgreSQL max connections metric: %v", err)
		} else {
			info = append(info, maxConnectionsMetric)
		}
	}
	return info, nil
}

func (adapter *RDSAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.PGDriver, context.Background(), adapter.Logger())
}

func (adapter *RDSAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
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
		context.Background(),
	)
	if err != nil {
		return fmt.Errorf("failed to apply config: %v", err)
	}

	// TODO(eddie): validate if this below comment is the case or
	// we were not waiting properly for parameter group changes

	// RDS has a race-condition/caching issue where the first fetches of config
	// after a restart are giving back the old config.
	// This results in re-applying the old recommended config.
	// To avoid this, we give a buffer of last applied config of 1 minute.

	// Instance is online, we validate that PostgreSQL is back online also
	adapter.Logger().Info("Waiting for PostgreSQL to come back online...")
	err = pg.WaitPostgresReady(adapter.PGDriver)
	if err != nil {
		return fmt.Errorf("error waiting for PostgreSQL to come back online: %v", err)
	}

	adapter.State.LastAppliedConfig = time.Now()
	return nil
}

func (adapter *RDSAdapter) Collectors() []agent.MetricCollector {
	pool := adapter.PGDriver

	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  pg.PGStatStatements(pool),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  pg.TransactionsPerSecond(pool),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  pg.ActiveConnections(pool),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  pg.DatabaseSize(pool),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  pg.Autovacuum(pool),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  pg.Uptime(pool),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  pg.BufferCacheHitRatio(pool),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  pg.WaitEvents(pool),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector: RDSHardwareInfo(
				adapter.Config.RDSDatabaseIdentifier,
				&adapter.State,
				&adapter.AWSClients,
				adapter.Logger(),
			),
		},
	}
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *RDSAdapter) Guardrails() *guardrails.Signal {
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

		adapter.Logger().Warnf("Memory usage: %.2f%%", memoryUsagePercent)
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

		adapter.Logger().Warnf("Freeable memory: %.2f%%", freeableMemoryPercent)
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
	rdsAdapter, err := CreateRDSAdapter(&configKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create RDS adapter: %w", err)
	}
	return &AuroraRDSAdapter{*rdsAdapter}, nil
}
