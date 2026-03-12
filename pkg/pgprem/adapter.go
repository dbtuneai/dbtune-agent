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
	pg.CatalogGetter
	pgConfig        pg.Config
	GuardrailConfig guardrails.Config
	PGVersion       string
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
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	pgMajorVersion := pg.ParsePgMajorVersion(PGVersion)
	c := &DefaultPostgreSQLAdapter{
		CommonAgent: *commonAgent,
		CatalogGetter: pg.CatalogGetter{
			PGPool:         dbpool,
			PGMajorVersion: pgMajorVersion,
			HealthGate:     pg.NewHealthGate(dbpool, commonAgent.Logger()),
		},
		pgConfig:        pgConfig,
		GuardrailConfig: guardrailSettings,
		PGVersion:       PGVersion,
	}
	collectors := DefaultCollectors(c)
	c.InitCollectors(collectors)

	return c, nil
}

func DefaultCollectors(pgAdapter *DefaultPostgreSQLAdapter) []agent.MetricCollector {
	collectors := pg.DefaultMetricCollectors(pgAdapter.PGPool, pgAdapter.pgConfig)
	return append(collectors, agent.MetricCollector{
		Key:       "hardware",
		Collector: HardwareInfoOnPremise(),
	})
}

func (adapter *DefaultPostgreSQLAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	pgDriver := adapter.PGPool
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

	diskType, err := GetDiskType(adapter.PGPool)
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

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.PGPool, ctx, adapter.Logger())
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	if proposedConfig.KnobApplication == "restart" {
		// If service name is missing, skip
		if adapter.pgConfig.ServiceName == "" {
			return fmt.Errorf("service name not configured, skipping restarting and applying configuration")
		}
	}

	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return err
	}

	for _, knob := range parsedKnobs {
		err = pg.AlterSystem(adapter.PGPool, knob.Name, knob.SettingValue)
		if err != nil {
			return fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)
		}
	}

	switch proposedConfig.KnobApplication {
	case "restart":
		if !agent.IsRestartAllowed() {
			return &agent.RestartNotAllowedError{
				Message: "restart is not allowed in the agent",
			}
		}
		// Restart the service
		adapter.Logger().Warn("Restarting service")
		// Execute systemctl restart command if it fails try executing it with sudo
		cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
		if err := cmd.Run(); err != nil {
			adapter.Logger().Warnf("failed to restart PostgreSQL service: %v. Trying with sudo...", err)

			sudoCmd := exec.Command("sudo", "systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
			if sudoErr := sudoCmd.Run(); sudoErr != nil {
				return fmt.Errorf("failed to restart PostgreSQL service with sudo: %w", sudoErr)
			}
			adapter.Logger().Warn("Service restarted using sudo.")
		} else {
			adapter.Logger().Warn("Service restarted.")
		}

		err := pg.WaitPostgresReady(adapter.PGPool)
		if err != nil {
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
	case "reload":
		// Reload database when everything is applied
		err := pg.ReloadConfig(adapter.PGPool)
		if err != nil {
			return err
		}
	case "":
		// TODO(eddie): We should make this more explicit somehow.
		// This happens when nothing is sent from the backend about this.
		// We should send an explicit string instead of leaving it blank.
		err := pg.ReloadConfig(adapter.PGPool)
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
func (adapter *DefaultPostgreSQLAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
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
