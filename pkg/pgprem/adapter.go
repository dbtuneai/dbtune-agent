package pgprem

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"

	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

type DefaultPostgreSQLAdapter struct {
	agent.CommonAgent
	pgDriver        *pgPool.Pool
	pgConfig        pg.Config
	GuardrailConfig guardrails.Config
}

func CreateDefaultPostgreSQLAdapter() (*DefaultPostgreSQLAdapter, error) {
	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgPool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()

	c := &DefaultPostgreSQLAdapter{
		CommonAgent:     *commonAgent,
		pgDriver:        dbpool,
		pgConfig:        pgConfig,
		GuardrailConfig: guardrailSettings,
	}
	collectors := DefaultCollectors(c)
	c.InitCollectors(collectors)

	return c, nil
}

func DefaultCollectors(pgAdapter *DefaultPostgreSQLAdapter) []agent.MetricCollector {
	pgDriver := pgAdapter.pgDriver
	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  pg.PGStatStatements(pgDriver),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  pg.TransactionsPerSecond(pgDriver),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  pg.ActiveConnections(pgDriver),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  pg.DatabaseSize(pgDriver),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  pg.Autovacuum(pgDriver),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  pg.UptimeMinutes(pgDriver),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  pg.BufferCacheHitRatio(pgDriver),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  pg.WaitEvents(pgDriver),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  HardwareInfoOnPremise(),
		},
	}
}

func (adapter *DefaultPostgreSQLAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	pgDriver := adapter.pgDriver
	pgVersion, err := pg.PGVersion(pgDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(pgDriver)
	if err != nil {
		return nil, err
	}

	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	hostInfo, err := host.Info()
	if err != nil {
		return nil, err
	}

	noCPUs, err := cpu.Counts(true)
	if err != nil {
		return nil, err
	}

	diskType, err := GetDiskType(adapter.pgDriver)
	if err != nil {
		adapter.Logger().Warnf("Error getting disk type: %v", err)
	}

	// Convert into metrics
	totalMemory, err := metrics.NodeMemoryTotal.AsFlatValue(memoryInfo.Total)
	if err != nil {
		return nil, err
	}
	version, _ := metrics.PGVersion.AsFlatValue(pgVersion)
	hostOS, _ := metrics.NodeOSInfo.AsFlatValue(hostInfo.OS)
	platform, _ := metrics.NodeOSPlatform.AsFlatValue(hostInfo.Platform)
	platformVersion, _ := metrics.NodeOSPlatformVer.AsFlatValue(hostInfo.PlatformVersion)
	maxConnectionsMetric, _ := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	noCPUsMetric, _ := metrics.NodeCPUCount.AsFlatValue(noCPUs)
	diskTypeMetric, _ := metrics.NodeStorageType.AsFlatValue(diskType)

	systemInfo := []metrics.FlatValue{
		version,
		totalMemory,
		hostOS,
		platformVersion,
		platform,
		maxConnectionsMetric,
		noCPUsMetric,
		diskTypeMetric,
	}

	return systemInfo, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.pgDriver, context.Background(), adapter.Logger())
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	if proposedConfig.KnobApplication == "restart" {
		// If service name is missing, skip
		if adapter.pgConfig.ServiceName == "" {
			return fmt.Errorf("service name not configured, skipping restarting and applying configuration")
		}
	}

	// Apply the configuration with ALTER
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return err
		}

		// Get the setting value using the proper type conversion method
		settingValue, err := knobConfig.GetSettingValue()
		if err != nil {
			return fmt.Errorf("failed to get setting value for %s: %w", knobConfig.Name, err)
		}
		err = pg.AlterSystem(adapter.pgDriver, knobConfig.Name, settingValue)
		if err != nil {
			return err
		}
	}

	switch proposedConfig.KnobApplication {
	case "restart":
		// Restart the service
		adapter.Logger().Warn("Restarting service")
		// Execute systemctl restart command if it fails try executing it with sudo
		cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName)
		if err := cmd.Run(); err != nil {
			adapter.Logger().Warnf("failed to restart PostgreSQL service: %v. Trying with sudo...", err)

			sudoCmd := exec.Command("sudo", "systemctl", "restart", adapter.pgConfig.ServiceName)
			if sudoErr := sudoCmd.Run(); sudoErr != nil {
				return fmt.Errorf("failed to restart PostgreSQL service with sudo: %w", sudoErr)
			}
			adapter.Logger().Warn("Service restarted using sudo.")
		} else {
			adapter.Logger().Warn("Service restarted.")
		}

		err := pg.WaitPostgresReady(adapter.pgDriver)
		if err != nil {
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
	case "reload":
		// Reload database when everything is applied
		err := pg.ReloadConfig(adapter.pgDriver)
		if err != nil {
			return err
		}
	case "":
		// TODO(eddie): We should make this more explicit somehow.
		// This happens when nothing is sent from the backend about this.
		// We should send an explicit string instead of leaving it blank.
		err := pg.ReloadConfig(adapter.pgDriver)
		if err != nil {
			return err
		}
	}
	return nil
}

// Guardrails for default PostgreSQL adapter performs the following:
// 1. Checks if the total memory is set. If not fetches it from the system and sets it in cache.
// 2. Fetches current memory usage
// 3. If memory usage is greater than 90% of total memory, triggers a critical guardrail
func (adapter *DefaultPostgreSQLAdapter) Guardrails() *guardrails.Signal {
	// Get memory info
	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		adapter.Logger().Error("Failed to get memory info:", err)
		return nil
	}

	// Calculate memory usage percentage
	memoryUsagePercent := float64(memoryInfo.Total-memoryInfo.Available) / float64(memoryInfo.Total) * 100

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than 90% (default), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}
