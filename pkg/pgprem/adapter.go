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

	if pgConfig.AllowRestart && pgConfig.ServiceName == "" && pgConfig.RestartScriptPath == "" {
		return nil, fmt.Errorf(
			"postgresql.allow_restart is true but neither postgresql.service_name nor postgresql.restart_script_path is configured. " +
				"Set postgresql.service_name (env: DBT_POSTGRESQL_SERVICE_NAME) " +
				"or set postgresql.restart_script_path (env: DBT_POSTGRESQL_RESTART_SCRIPT_PATH) to a custom restart script",
		)
	}

	if pgConfig.RestartScriptPath != "" {
		info, err := os.Stat(pgConfig.RestartScriptPath)
		if err != nil {
			return nil, fmt.Errorf(
				"postgresql.restart_script_path is set but %s is not accessible: %w",
				pgConfig.RestartScriptPath, err,
			)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("%s is a directory, expected an executable file", pgConfig.RestartScriptPath)
		}
		if info.Mode()&0o111 == 0 {
			return nil, fmt.Errorf("%s is not executable (mode %s); chmod +x it", pgConfig.RestartScriptPath, info.Mode())
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
	// gopsutil can return an empty PlatformVersion on some Windows builds; the
	// backend rejects a blank string for this field.
	if hostInfo.PlatformVersion == "" {
		hostInfo.PlatformVersion = "unknown"
	}

	noCPUs, err := cpu.Counts(true)
	if err != nil {
		return nil, err
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

	databaseInfo, err := pg.DatabaseSystemInfo(pgDriver)
	if err != nil {
		return nil, err
	}

	systemInfo := make([]metrics.FlatValue, 0, 7+len(databaseInfo))
	systemInfo = append(systemInfo,
		version,
		totalMemory,
		hostOS,
		platformVersion,
		platform,
		maxConnectionsMetric,
		noCPUsMetric,
	)
	systemInfo = append(systemInfo, databaseInfo...)

	return systemInfo, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.pgDriver, ctx)
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *agent.ProposedConfigResponse) agent.ApplyConfigError {
	adapter.Logger().Infof("Applying Config: %s", proposedConfig.KnobApplication)

	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return &agent.ConfigApplyError{Err: err}
	}

	// Validate against the running PostgreSQL before mutating
	// postgresql.auto.conf, so we never half-apply.
	paramNames := make([]string, 0, len(parsedKnobs))
	for _, k := range parsedKnobs {
		paramNames = append(paramNames, k.Name)
	}
	requiresRestart, err := pg.ValidateRestartPolicy(adapter.pgDriver, ctx, paramNames, proposedConfig.KnobApplication)
	if err != nil {
		return &agent.ConfigApplyError{Err: err}
	}
	if requiresRestart && adapter.pgConfig.ServiceName == "" && adapter.pgConfig.RestartScriptPath == "" {
		return &agent.ConfigApplyError{Err: fmt.Errorf("neither service name nor restart script configured, refusing to apply: a restart is required to take effect")}
	}

	for _, knob := range parsedKnobs {
		err = pg.AlterSystem(adapter.pgDriver, knob.Name, knob.SettingValue)
		if err != nil {
			return &agent.ConfigApplyError{Err: fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)}
		}
	}

	if !requiresRestart {
		// Reload database when everything is applied. KnobApplication=restart
		// with no postmaster-context params falls through here too: the intent
		// is treated as a hint, and we avoid a needless restart.
		err := pg.ReloadConfig(adapter.pgDriver)
		if err != nil {
			return &agent.ConfigApplyError{Err: err}
		}
		return nil
	}

	// Restart the service
	adapter.Logger().Warn("Restarting service")

	if adapter.pgConfig.RestartScriptPath != "" {
		// Execute the operator-provided restart script directly (no shell
		// interpolation). The path comes from trusted config.
		//
		// Contract: the script MUST signal success with exit code 0 and failure
		// with any non-zero exit code. Output written to stdout/stderr is treated
		// as diagnostic only (logged on failure) and does not affect the
		// success/failure decision.
		cmd := exec.Command(adapter.pgConfig.RestartScriptPath) //nolint:gosec // path is from trusted config
		output, err := cmd.CombinedOutput()
		exitCode := cmd.ProcessState.ExitCode() // -1 if the process never ran
		if err != nil || exitCode != 0 {
			adapter.Logger().Warnf("restart script %s exited with code %d; output: %s",
				adapter.pgConfig.RestartScriptPath, exitCode, string(output))
			return &agent.ConfigApplyError{Err: fmt.Errorf("restart script %s failed (exit code %d): %w",
				adapter.pgConfig.RestartScriptPath, exitCode, err)}
		}
		adapter.Logger().Warnf("Service restarted via %s (exit code 0).", adapter.pgConfig.RestartScriptPath)
	} else {
		// Execute systemctl restart command if it fails try executing it with sudo
		cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
		if err := cmd.Run(); err != nil {
			adapter.Logger().Warnf("failed to restart PostgreSQL service: %v. Trying with sudo...", err)

			sudoCmd := exec.Command("sudo", "systemctl", "restart", adapter.pgConfig.ServiceName) //nolint:gosec // ServiceName is from trusted config
			if sudoErr := sudoCmd.Run(); sudoErr != nil {
				return &agent.ConfigApplyError{Err: fmt.Errorf("failed to restart PostgreSQL service with sudo: %w", sudoErr)}
			}
			adapter.Logger().Warn("Service restarted using sudo.")
		} else {
			adapter.Logger().Warn("Service restarted.")
		}
	}

	if err := pg.WaitPostgresReady(adapter.pgDriver); err != nil {
		return &agent.ConfigApplyError{Err: fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)}
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
