package cloudsql

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

type CloudSQLAdapter struct {
	agent.CommonAgent
	PGDriver       *pgPool.Pool
	CloudSQLConfig Config
}

func CreateCloudSQLAdapter() (*CloudSQLAdapter, error) {
	commonAgent := agent.CreateCommonAgent()

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	pgPool, err := pgPool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, err
	}

	config, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	c := &CloudSQLAdapter{
		CommonAgent:    *commonAgent,
		PGDriver:       pgPool,
		CloudSQLConfig: config,
	}

	c.InitCollectors(c.Collectors())

	return c, nil
}

func (adapter *CloudSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying config")
	return nil
}

func (adapter *CloudSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	adapter.Logger().Debugf("Getting Active Config")

	return pg.GetActiveConfig(adapter.PGDriver, context.Background(), adapter.Logger())
}

func (adapter *CloudSQLAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	adapter.Logger().Debugf("Getting System Info")

	// Get PostgreSQL version and max connections from database
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion)
	if err != nil {
		adapter.Logger().Errorf("Error creating PostgreSQL version metric: %v", err)
		return nil, err
	}

	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		adapter.Logger().Errorf("Error creating max connections metric: %v", err)
		return nil, err
	}

	systemInfo := []metrics.FlatValue{
		version,
		maxConnectionsMetric,
	}

	return systemInfo, nil
}

func (adapter *CloudSQLAdapter) Guardrails() *guardrails.Signal {
	adapter.Logger().Debugf("Checking Guardrails")
	return nil
}

func (adapter *CloudSQLAdapter) Collectors() []agent.MetricCollector {
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
			Collector:  pg.UptimeMinutes(pool),
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
			Collector:  CloudSQLHardwareInfo(adapter.Logger(), adapter.CloudSQLConfig),
		},
	}
}
