package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	adapters "github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/rdsutil"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jackc/pgx/v5/pgxpool"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

type State struct {
	LastAppliedConfig  time.Time
	LastGuardrailCheck time.Time
	LastDBInfoCheck    time.Time
}

type RDSAdapter struct {
	DefaultPostgreSQLAdapter
	Config     adapters.RDSConfig
	DBInfo     rdsutil.RDSDBInfo
	State      State
	AWSClients rdsutil.AWSClients
}

func (adapter *RDSAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

func (adapter *RDSAdapter) APIClient() *retryablehttp.Client {
	return adapter.APIClient()
}

func (adapter *RDSAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	// Refreshes self
	dbInfo, err := rdsutil.FetchDBInfo(
		adapter.Config.RDSDatabaseIdentifier,
		&adapter.AWSClients,
		context.Background(),
	)
	if err != nil {
		return nil, err
	}
	adapter.DBInfo = dbInfo
	adapter.State.LastDBInfoCheck = time.Now()

	// Get the RDSDB specific info
	info, err := dbInfo.TryIntoFlatValuesSlice()
	if err != nil {
		return nil, err
	}

	// Get the PostgreSQL specific info
	pgpool := adapter.PGDriver()
	pgVersion, err := collectors.PGVersion(pgpool)
	if err != nil {
		version, err := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
		if err == nil {
			info = append(info, version)
		}
	}

	maxConnections, err := collectors.MaxConnections(pgpool)
	if err == nil {
		maxConnectionsMetric, err := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)
		if err == nil {
			info = append(info, maxConnectionsMetric)
		}
	}
	return info, nil
}

func (adapter *RDSAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// If the last applied config is less than 1 minute ago, return
	if adapter.State.LastAppliedConfig.Add(1 * time.Minute).After(time.Now()) {
		adapter.Logger().Info("Last applied config is less than 1 minute ago, skipping")
		return nil
	}

	err := rdsutil.ApplyConfig(
		proposedConfig,
		&adapter.DBInfo,
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
	err = waitPostgresReady(adapter.PGDriver(), context.Background())
	if err != nil {
		return fmt.Errorf("error waiting for PostgreSQL to come back online: %v", err)
	}
	if err != nil {
		return fmt.Errorf("failed to apply config: %v", err)
	}

	adapter.State.LastAppliedConfig = time.Now()
	return nil
}

func RDSCollectors(adapter *RDSAdapter) []agent.MetricCollector {
	pool := adapter.PGDriver()

	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  collectors.PGStatStatements(pool),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(pool),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(pool),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  collectors.DatabaseSize(pool),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  collectors.Autovacuum(pool),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  collectors.Uptime(pool),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  collectors.BufferCacheHitRatio(pool),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  collectors.WaitEvents(pool),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector: collectors.RDSHardwareInfo(
				adapter.Config.RDSDatabaseIdentifier,
				&adapter.DBInfo,
				&adapter.AWSClients,
				adapter.Logger(),
			),
		},
	}
}

func CreateRDSAdapter() (*RDSAdapter, error) {
	// Create a new Viper instance for RDS configuration if the sub-config doesn't exist
	dbtuneConfig := viper.Sub("rds")
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}

	// Bind environment variables directly to keys (not nested paths)
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID", "DBT_AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY", "DBT_AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION", "DBT_AWS_REGION")
	dbtuneConfig.BindEnv("RDS_PARAMETER_GROUP_NAME", "DBT_RDS_PARAMETER_GROUP_NAME")
	dbtuneConfig.BindEnv("RDS_DATABASE_IDENTIFIER", "DBT_RDS_DATABASE_IDENTIFIER")

	// Bind also global environment variables as fallback for AWS credentials
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION")

	var rdsConfig adapters.RDSConfig
	err := dbtuneConfig.Unmarshal(&rdsConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %v", err)
	}

	// Validate required configuration
	err = utils.ValidateStruct(&rdsConfig)
	if err != nil {
		return nil, err
	}

	// Validate non-credential required configuration
	if rdsConfig.AWSRegion == "" {
		return nil, fmt.Errorf("AWS_REGION is required")
	}
	if rdsConfig.RDSDatabaseIdentifier == "" {
		return nil, fmt.Errorf("RDS_DATABASE_IDENTIFIER is required")
	}
	if rdsConfig.RDSParameterGroupName == "" {
		return nil, fmt.Errorf("RDS_PARAMETER_GROUP_NAME is required")
	}

	// Create AWS config
	var cfg aws.Config

	if rdsConfig.AWSAccessKey != "" && rdsConfig.AWSSecretAccessKey != "" {
		// Use static credentials if provided
		cfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(rdsConfig.AWSRegion),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				rdsConfig.AWSAccessKey,
				rdsConfig.AWSSecretAccessKey,
				"",
			)),
		)
	} else {
		// Use default credential chain
		// Includes by default WebIdentityToken:
		// https://github.com/aws/aws-sdk-go-v2/blob/main/config/resolve_credentials.go#L119
		cfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(rdsConfig.AWSRegion),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %v", err)
	}

	defaultAdapter, err := CreateDefaultPostgreSQLAdapter()
	if err != nil {
		return nil, fmt.Errorf("failed to create base PostgreSQL adapter: %w", err)
	}

	clients := rdsutil.NewAWSClients(cfg)

	// Check if RDS client can fetch the database instance correctly and the tokens work
	dbInfo, err := rdsutil.FetchDBInfo(
		rdsConfig.RDSDatabaseIdentifier,
		&clients,
		context.Background(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
	}

	c := &RDSAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		Config:                   rdsConfig,
		DBInfo:                   dbInfo,
		AWSClients:               clients,
	}

	// Initialize collectors with RDSCollectors instead of DefaultCollectors
	c.MetricsState = agent.MetricsState{
		Collectors: RDSCollectors(c),
		Cache:      agent.Caches{},
		Mutex:      &sync.Mutex{},
	}

	return c, nil
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *RDSAdapter) Guardrails() *agent.GuardrailSignal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Info("Checking guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	totalMemoryBytes, err := adapter.DBInfo.TotalMemoryBytes()
	if err != nil {
		adapter.Logger().Errorf("Failed to get total memory bytes: %v", err)
		return nil
	}

	if adapter.DBInfo.PerformanceInsightsEnabled() {
		resourceID, err := adapter.DBInfo.ResourceID()
		if err != nil {
			adapter.Logger().Errorf("Failed to get resource ID: %v", err)
			return nil
		}

		memoryUsageBytes, err := rdsutil.GetMemoryUsageFromPI(
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
		if memoryUsagePercent > 90 {
			return &agent.GuardrailSignal{
				Level: agent.Critical,
				Type:  agent.Memory,
			}
		}
	} else {
		freeableMemoryBytes, err := rdsutil.GetFreeableMemoryFromCW(
			adapter.Config.RDSDatabaseIdentifier,
			&adapter.AWSClients,
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from CloudWatch: %v", err)
			return nil
		}
		freeableMemoryPercent := (float64(freeableMemoryBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Warnf("Freeable memory: %.2f%%", freeableMemoryPercent)
		if freeableMemoryPercent < 10 {
			return &agent.GuardrailSignal{
				Level: agent.Critical,
				Type:  agent.FreeableMemory,
			}
		}
	}

	return nil
}

// TODO: Could move out into it's own file
func waitPostgresReady(pgPool *pgxpool.Pool, ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(1 * time.Second):
			// Try to execute a simple query
			_, err := pgPool.Exec(ctx, "SELECT 1")
			if err == nil {
				return nil
			}
		}
	}
}
