package cloudsql

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/api/sqladmin/v1"
)

type CloudSQLAdapter struct {
	agent.CommonAgent
	PGDriver              *pgPool.Pool
	State                 *State
	CloudSQLConfig        Config
	CloudMonitoringClient *CloudMonitoringClient
	CloudSQLAdminClient   *SqlAdminClient
	GuardrailSettings     *guardrails.Config
	pgConfig              pg.Config
	PGVersion             string
}

func CreateCloudSQLAdapter() (*CloudSQLAdapter, error) {
	ctx := context.Background()
	commonAgent := agent.CreateCommonAgent()

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	pgPool, err := pgPool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, err
	}

	config, err := ConfigFromViper()
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

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	PGVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	c := &CloudSQLAdapter{
		CommonAgent:    *commonAgent,
		State:          &State{LastGuardrailCheck: time.Now()},
		PGDriver:       pgPool,
		CloudSQLConfig: config,
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

	c.InitCollectors(c.Collectors())

	return c, nil
}

func (adapter *CloudSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying config")

	flags := []*sqladmin.DatabaseFlags{}

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}
		fmtValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return fmt.Errorf("failed to get setting value: %v", err)
		}

		param := &sqladmin.DatabaseFlags{
			Name:  knobConfig.Name,
			Value: fmtValue,
		}

		flags = append(flags, param)
	}

	err := adapter.CloudSQLAdminClient.ApplyFlags(adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName, flags)
	if err != nil {
		return err
	}

	err = pg.WaitPostgresReady(adapter.PGDriver)
	if err != nil {
		return fmt.Errorf("Error waiting for PostgreSQL to come back online: %v", err)
	}
	return nil
}

func (adapter *CloudSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	adapter.Logger().Debugf("Getting Active Config")

	config, err := pg.GetActiveConfig(adapter.PGDriver, context.Background(), adapter.Logger())
	if err != nil {
		return nil, err
	}

	// some config rows are marked as internal, we cannot control these at all and
	// they change in a way we can't predict and so can cause tuning to fail
	filteredConfig := make(agent.ConfigArraySchema, 0, len(config))

	for _, knob := range config {
		if k, ok := knob.(agent.PGConfigRow); ok {
			if k.Context != "internal" {
				filteredConfig = append(filteredConfig, k)
			} else {
				adapter.Logger().Debugf("Internal config option filtered out: %s", k.Name)
			}
		} else {
			adapter.Logger().Errorf("Unexpected Config Type: %T", knob)
		}
	}

	return filteredConfig, nil
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

	cpuCount, err := GetCPUCount(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("Error fetching CPU count: %v", err)
		return nil, err
	}

	cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(int64(cpuCount.GetDoubleValue()))
	if err != nil {
		adapter.Logger().Errorf("Error creating NodeCPUCount metric: %v", err)
		return nil, err
	}

	memoryTotal, err := GetMemoryTotal(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("failed to get memory total: %v", err)
		return nil, err
	}

	memoryTotalMetric, err := metrics.NodeMemoryTotal.AsFlatValue(memoryTotal.GetInt64Value())
	if err != nil {
		adapter.Logger().Errorf("Error creating NodeMemoryTotal metric: %v", err)
		return nil, err
	}

	systemInfo := []metrics.FlatValue{
		version,
		maxConnectionsMetric,
		cpuCountMetric,
		memoryTotalMetric,
	}

	adapter.Logger().Debugf("SystemMetrics: %v", systemInfo)

	return systemInfo, nil
}

func (adapter *CloudSQLAdapter) Guardrails() *guardrails.Signal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Debugf("Checking Guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	memoryStats, err := GetMemoryMetrics(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory metric for guardrail: %v", err)
		return nil
	}

	if memoryStats.UsedPercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			memoryStats.UsedPercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func (adapter *CloudSQLAdapter) Collectors() []agent.MetricCollector {
	pool := adapter.PGDriver
	collectors := []agent.MetricCollector{
		{
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pool, adapter.pgConfig.IncludeQueries, adapter.pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pool),
		},
		{
			Key:       "database_active_connections",
			Collector: pg.ActiveConnections(pool),
		},
		{
			Key:       "system_db_size",
			Collector: pg.DatabaseSize(pool),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: pg.Autovacuum(pool),
		},
		{
			Key:       "server_uptime",
			Collector: pg.UptimeMinutes(pool),
		},
		{
			Key:       "pg_database",
			Collector: pg.PGStatDatabase(pool),
		},
		{
			Key:       "pg_user_tables",
			Collector: pg.PGStatUserTables(pool),
		},
		{
			Key:       "pg_class",
			Collector: pg.PGClass(pool),
		},
		{
			Key:       "pg_stat_progress_vacuum",
			Collector: pg.PGStatProgressVacuum(pool),
		},
		{
			Key:       "pg_old_transactions",
			Collector: pg.PGOldTransactions(pool),
		},
		{
			Key:       "pg_bgwriter",
			Collector: pg.PGStatBGwriter(pool),
		},
		{
			Key:       "pg_wal",
			Collector: pg.PGStatWAL(pool),
		},
		{
			Key:       "database_wait_events",
			Collector: pg.WaitEvents(pool),
		},
		{
			Key:       "hardware",
			Collector: CloudSQLHardwareInfo(adapter.Logger(), adapter.CloudSQLConfig, adapter.CloudMonitoringClient),
		},
	}
	majorVersion := strings.Split(adapter.PGVersion, ".")
	intMajorVersion, err := strconv.Atoi(majorVersion[0])
	if err != nil {
		adapter.Logger().Warnf("Could not parse major version from version string %s: %v", adapter.PGVersion, err)
		return collectors
	}
	if intMajorVersion >= 17 {
		collectors = append(collectors, agent.MetricCollector{
			Key:       "pg_checkpointer",
			Collector: pg.PGStatCheckpointer(pool),
		})
	}
	return collectors
}
