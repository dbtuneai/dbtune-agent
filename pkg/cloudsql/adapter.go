package cloudsql

import (
	"context"
	"fmt"
	"log"
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
	agent.CatalogGetter
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

	commonAgent.DBPool = pgPool
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

	collectors, err := pg.StandardCatalogCollectors(pgPool, PGVersion)
	if err != nil {
		return nil, err
	}
	c.SetCatalogCollectors(collectors)
	c.InitCollectors(c.Collectors())

	return c, nil
}

func (adapter *CloudSQLAdapter) ApplyConfig(_ context.Context, proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying config")

	flags := []*sqladmin.DatabaseFlags{}

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %w", err)
		}
		fmtValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return fmt.Errorf("failed to get setting value: %w", err)
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
		return fmt.Errorf("Error waiting for PostgreSQL to come back online: %w", err)
	}
	return nil
}

func (adapter *CloudSQLAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	adapter.Logger().Debugf("Getting Active Config")

	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx)
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

func (adapter *CloudSQLAdapter) GetSystemInfo(_ context.Context) ([]metrics.FlatValue, error) {
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

func (adapter *CloudSQLAdapter) Guardrails(_ context.Context) *guardrails.Signal {
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
	return []agent.MetricCollector{
		{
			Key:       "hardware",
			Collector: CloudSQLHardwareInfo(adapter.Logger(), adapter.CloudSQLConfig, adapter.CloudMonitoringClient),
		},
	}
}
