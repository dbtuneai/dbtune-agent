package pgprem

import (
	"context"
	"fmt"
	"os"
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
	agent.CatalogGetter
	pgDriver        *pgPool.Pool
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

	if pgConfig.AllowRestart && pgConfig.ServiceName == "" && !pgConfig.UseRestartCommand {
		return nil, fmt.Errorf(
			"postgresql.allow_restart is true but neither postgresql.service_name nor postgresql.use_restart_command is configured. " +
				"Set postgresql.service_name (env: DBT_POSTGRESQL_SERVICE_NAME) " +
				"or set postgresql.use_restart_command=true (env: DBT_POSTGRESQL_USE_RESTART_COMMAND) and provide " +
				pg.RestartScriptPath,
		)
	}

	if pgConfig.UseRestartCommand {
		info, err := os.Stat(pg.RestartScriptPath)
		if err != nil {
			return nil, fmt.Errorf(
				"postgresql.use_restart_command is true but %s is not accessible: %w",
				pg.RestartScriptPath, err,
			)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("%s is a directory, expected an executable file", pg.RestartScriptPath)
		}
		if info.Mode()&0o111 == 0 {
			return nil, fmt.Errorf("%s is not executable (mode %s); chmod +x it", pg.RestartScriptPath, info.Mode())
		}
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

	commonAgent.DBPool = dbpool
	c := &DefaultPostgreSQLAdapter{
		CommonAgent:     *commonAgent,
		pgDriver:        dbpool,
		pgConfig:        pgConfig,
		GuardrailConfig: guardrailSettings,
		PGVersion:       PGVersion,
	}
	collectors, err := pg.StandardCatalogCollectors(dbpool, PGVersion)
	if err != nil {
		return nil, err
	}
	c.SetCatalogCollectors(collectors)
	c.InitCollectors(DefaultCollectors())

	return c, nil
}

func DefaultCollectors() []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key:       "hardware",
			Collector: HardwareInfoOnPremise(),
		},
	}
}

func (adapter *DefaultPostgreSQLAdapter) GetSystemInfo(_ context.Context) ([]metrics.FlatValue, error) {
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

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.pgDriver, ctx)
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(_ context.Context, proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return err
	}

	for _, knob := range parsedKnobs {
		err = pg.AlterSystem(adapter.pgDriver, knob.Name, knob.SettingValue)
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
		adapter.Logger().Warn("Restarting service")

		if adapter.pgConfig.UseRestartCommand {
			// Execute the operator-provided restart script directly (no shell
			// interpolation). Path is fixed so we never exec an attacker-controlled string.
			//
			// Contract: the script MUST signal success with exit code 0 and failure
			// with any non-zero exit code. Output written to stdout/stderr is treated
			// as diagnostic only and does not affect the success/failure decision.
			cmd := exec.Command(pg.RestartScriptPath) //nolint:gosec
			output, err := cmd.CombinedOutput()
			exitCode := cmd.ProcessState.ExitCode() // -1 if the process never ran
			if err != nil || exitCode != 0 {
				adapter.Logger().Warnf("restart script %s exited with code %d; output: %s",
					pg.RestartScriptPath, exitCode, string(output))
				return fmt.Errorf("restart script %s failed (exit code %d): %w",
					pg.RestartScriptPath, exitCode, err)
			}
			adapter.Logger().Warnf("Service restarted via %s (exit code 0).", pg.RestartScriptPath)
		} else {
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
func (adapter *DefaultPostgreSQLAdapter) Guardrails(_ context.Context) *guardrails.Signal {
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
