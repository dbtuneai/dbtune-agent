package pgprem

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

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

	c := &DefaultPostgreSQLAdapter{
		CommonAgent:     *commonAgent,
		pgDriver:        dbpool,
		pgConfig:        pgConfig,
		GuardrailConfig: guardrailSettings,
		PGVersion:       PGVersion,
	}
	collectors := DefaultCollectors(c)
	c.InitCollectors(collectors)

	return c, nil
}

func DefaultCollectors(pgAdapter *DefaultPostgreSQLAdapter) []agent.MetricCollector {
	pgDriver := pgAdapter.pgDriver
	collectors := []agent.MetricCollector{
		{
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pgDriver, pgAdapter.pgConfig.IncludeQueries, pgAdapter.pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pgDriver),
		},
		{
			Key:       "database_connections",
			Collector: pg.Connections(pgDriver),
		},
		{
			Key:       "system_db_size",
			Collector: pg.DatabaseSize(pgDriver),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: pg.Autovacuum(pgDriver),
		},
		{
			Key:       "server_uptime",
			Collector: pg.UptimeMinutes(pgDriver),
		},
		{
			Key:       "pg_database",
			Collector: pg.PGStatDatabase(pgDriver),
		},
		{
			Key:       "pg_user_tables",
			Collector: pg.PGStatUserTables(pgDriver),
		},
		{
			Key:       "pg_bgwriter",
			Collector: pg.PGStatBGwriter(pgDriver),
		},
		{
			Key:       "pg_wal",
			Collector: pg.PGStatWAL(pgDriver),
		},
		{
			Key:       "database_wait_events",
			Collector: pg.WaitEvents(pgDriver),
		},
		{
			Key:       "hardware",
			Collector: HardwareInfoOnPremise(),
		},
	}
	majorVersion := strings.Split(pgAdapter.PGVersion, ".")
	intMajorVersion, err := strconv.Atoi(majorVersion[0])
	if err != nil {
		pgAdapter.Logger().Warnf("Could not parse major version from version string %s: %v", pgAdapter.PGVersion, err)
		return collectors
	}
	if intMajorVersion >= 17 {
		collectors = append(collectors, agent.MetricCollector{
			Key:       "pg_checkpointer",
			Collector: pg.PGStatCheckpointer(pgDriver),
		})
	}
	return collectors
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

func (adapter *DefaultPostgreSQLAdapter) pgMajorVersion() int {
	return pg.ParsePgMajorVersion(adapter.PGVersion)
}

func (adapter *DefaultPostgreSQLAdapter) GetPgStatActivity() (*agent.PgStatActivityPayload, error) {
	rows, err := pg.CollectPgStatActivity(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatActivityPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatDatabaseAll() (*agent.PgStatDatabasePayload, error) {
	rows, err := pg.CollectPgStatDatabase(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatDatabasePayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatDatabaseConflicts() (*agent.PgStatDatabaseConflictsPayload, error) {
	rows, err := pg.CollectPgStatDatabaseConflicts(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatDatabaseConflictsPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatArchiver() (*agent.PgStatArchiverPayload, error) {
	rows, err := pg.CollectPgStatArchiver(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatArchiverPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatBgwriterAll() (*agent.PgStatBgwriterPayload, error) {
	rows, err := pg.CollectPgStatBgwriter(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatBgwriterPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatCheckpointerAll() (*agent.PgStatCheckpointerPayload, error) {
	rows, err := pg.CollectPgStatCheckpointer(adapter.pgDriver, context.Background(), adapter.pgMajorVersion())
	if err != nil { return nil, err }
	return &agent.PgStatCheckpointerPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatWalAll() (*agent.PgStatWalPayload, error) {
	rows, err := pg.CollectPgStatWal(adapter.pgDriver, context.Background(), adapter.pgMajorVersion())
	if err != nil { return nil, err }
	return &agent.PgStatWalPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatIO() (*agent.PgStatIOPayload, error) {
	rows, err := pg.CollectPgStatIO(adapter.pgDriver, context.Background(), adapter.pgMajorVersion())
	if err != nil { return nil, err }
	return &agent.PgStatIOPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatReplication() (*agent.PgStatReplicationPayload, error) {
	rows, err := pg.CollectPgStatReplication(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatReplicationPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatReplicationSlots() (*agent.PgStatReplicationSlotsPayload, error) {
	rows, err := pg.CollectPgStatReplicationSlots(adapter.pgDriver, context.Background(), adapter.pgMajorVersion())
	if err != nil { return nil, err }
	return &agent.PgStatReplicationSlotsPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatSlru() (*agent.PgStatSlruPayload, error) {
	rows, err := pg.CollectPgStatSlru(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatSlruPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatUserIndexes() (*agent.PgStatUserIndexesPayload, error) {
	rows, err := pg.CollectPgStatUserIndexes(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatUserIndexesPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatioUserTables() (*agent.PgStatioUserTablesPayload, error) {
	rows, err := pg.CollectPgStatioUserTables(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatioUserTablesPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatioUserIndexes() (*agent.PgStatioUserIndexesPayload, error) {
	rows, err := pg.CollectPgStatioUserIndexes(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatioUserIndexesPayload{Rows: rows}, nil
}
func (adapter *DefaultPostgreSQLAdapter) GetPgStatUserFunctions() (*agent.PgStatUserFunctionsPayload, error) {
	rows, err := pg.CollectPgStatUserFunctions(adapter.pgDriver, context.Background())
	if err != nil { return nil, err }
	return &agent.PgStatUserFunctionsPayload{Rows: rows}, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetDDL() (*agent.DDLPayload, error) {
	ddl, err := pg.CollectDDL(adapter.pgDriver, context.Background())
	if err != nil {
		return nil, err
	}
	return &agent.DDLPayload{DDL: ddl, Hash: pg.HashDDL(ddl)}, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetPgStatistic() (*agent.PgStatisticPayload, error) {
	rows, err := pg.CollectPgStatistic(adapter.pgDriver, context.Background())
	if err != nil {
		return nil, err
	}
	return &agent.PgStatisticPayload{Rows: rows}, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetPgStatUserTables() (*agent.PgStatUserTablePayload, error) {
	rows, err := pg.CollectPgStatUserTables(adapter.pgDriver, context.Background())
	if err != nil {
		return nil, err
	}
	return &agent.PgStatUserTablePayload{Rows: rows}, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetPgClass() (*agent.PgClassPayload, error) {
	rows, err := pg.CollectPgClass(adapter.pgDriver, context.Background())
	if err != nil {
		return nil, err
	}
	return &agent.PgClassPayload{Rows: rows}, nil
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
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
