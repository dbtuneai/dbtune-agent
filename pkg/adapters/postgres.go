package adapters

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"

	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaypipes/ghw"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/spf13/viper"
)

type PostgreSQLConfig struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
	ServiceName   string `mapstructure:"service_name"`
}

type GuardrailSettings struct {
	MemoryThreshold float64 `mapstructure:"memory_threshold" validate:"gte=1,lte=100"`
}

type DefaultPostgreSQLAdapter struct {
	agent.CommonAgent
	pgDriver        *pgPool.Pool
	pgConfig        PostgreSQLConfig
	GuardrailConfig GuardrailSettings
}

func CreateDefaultPostgreSQLAdapter() (*DefaultPostgreSQLAdapter, error) {
	dbtuneConfig := viper.Sub("postgresql")
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}
	dbtuneConfig.BindEnv("connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	dbtuneConfig.BindEnv("service_name", "DBT_POSTGRESQL_SERVICE_NAME")

	var pgConfig PostgreSQLConfig
	err := dbtuneConfig.Unmarshal(&pgConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}

	err = utils.ValidateStruct(&pgConfig)
	if err != nil {
		return nil, err
	}

	GuardrailSetting := viper.Sub("guardrail_settings")
	if GuardrailSetting == nil {
		GuardrailSetting = viper.New()
	}

	GuardrailSetting.BindEnv("memory_threshold", "DBT_MEMORY_THRESHOLD")

	var GuardrailConfig GuardrailSettings
	GuardrailSetting.SetDefault("memory_threshold", 90)
	err = GuardrailSetting.Unmarshal(&GuardrailConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}

	err = utils.ValidateStruct(&GuardrailConfig)
	if err != nil {
		return nil, err
	}
	dbpool, err := pgPool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()

	c := &DefaultPostgreSQLAdapter{}
	c.CommonAgent = *commonAgent
	c.pgDriver = dbpool
	c.pgConfig = pgConfig
	c.GuardrailConfig = GuardrailConfig

	// Initialize collectors after the adapter is fully set up
	c.MetricsState = agent.MetricsState{
		Collectors: DefaultCollectors(c),
		Cache:      agent.Caches{},
		Mutex:      &sync.Mutex{},
	}

	return c, nil
}

func DefaultCollectors(pgAdapter *DefaultPostgreSQLAdapter) []agent.MetricCollector {
	// TODO: Is the metric type needed here? Maybe this can be dropped,
	// as collectors may collect multiple metrics
	// TODO: Find a better way to re-use collectors between adapters, current method does
	// not work nice, as the RemoveKey method is available on MetricsState,
	// which is inconvenient to use
	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  collectors.PGStatStatements(pgAdapter),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(pgAdapter),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(pgAdapter),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  collectors.DatabaseSize(pgAdapter),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  collectors.Autovacuum(pgAdapter),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  collectors.Uptime(pgAdapter),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  collectors.BufferCacheHitRatio(pgAdapter),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  collectors.WaitEvents(pgAdapter),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  collectors.HardwareInfoOnPremise(pgAdapter),
		},
		//{
		//	Key:        "failing_slow_queries",
		//	MetricType: "int",
		//	Collector:  collectors.ArtificiallyFailingQueries(pgAdapter),
		//},
	}
}

func (adapter *DefaultPostgreSQLAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

func (adapter *DefaultPostgreSQLAdapter) APIClient() *retryablehttp.Client {
	return adapter.APIClient()
}

func (adapter *DefaultPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Println("Collecting system info")

	var systemInfo []utils.FlatValue

	pgVersion, err := collectors.PGVersion(adapter)
	if err != nil {
		return nil, err
	}

	maxConnections, err := collectors.MaxConnections(adapter)
	if err != nil {
		return nil, err
	}

	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	totalMemory, err := utils.NewMetric("node_memory_total", memoryInfo.Total, utils.Int)
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

	version, _ := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
	hostOS, _ := utils.NewMetric("system_info_os", hostInfo.OS, utils.String)
	platform, _ := utils.NewMetric("system_info_platform", hostInfo.Platform, utils.String)
	platformVersion, _ := utils.NewMetric("system_info_platform_version", hostInfo.PlatformVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)
	noCPUsMetric, _ := utils.NewMetric("node_cpu_count", noCPUs, utils.Int)

	systemInfo = append(systemInfo, version, totalMemory, hostOS, platformVersion, platform, maxConnectionsMetric, noCPUsMetric)

	diskType, err := getDiskType(adapter)
	if err != nil {
		adapter.Logger().Debug("Error getting disk type", err)
	}

	diskTypeMetric, _ := utils.NewMetric("node_disk_device_type", diskType, utils.String)
	systemInfo = append(systemInfo, diskTypeMetric)

	return systemInfo, nil
}

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	var configRows agent.ConfigArraySchema

	// Query for numeric types (real and integer)
	numericRows, err := adapter.pgDriver.Query(context.Background(), `
		SELECT name, setting::numeric as setting, unit, vartype, context 
		FROM pg_settings 
		WHERE vartype IN ('real', 'integer');`)
	if err != nil {
		return nil, err
	}

	for numericRows.Next() {
		var row agent.PGConfigRow
		err := numericRows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			adapter.Logger().Error("Error scanning numeric row", err)
			continue
		}
		configRows = append(configRows, row)
	}

	// Query for non-numeric types
	nonNumericRows, err := adapter.pgDriver.Query(context.Background(), `
		SELECT name, setting, unit, vartype, context 
		FROM pg_settings 
		WHERE vartype NOT IN ('real', 'integer');`)
	if err != nil {
		return nil, err
	}

	for nonNumericRows.Next() {
		var row agent.PGConfigRow
		err := nonNumericRows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			adapter.Logger().Error("Error scanning non-numeric row", err)
			continue
		}
		configRows = append(configRows, row)
	}

	return configRows, nil
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

		// We make the assumption every setting is a number parsed as float
		query := fmt.Sprintf(`ALTER SYSTEM SET "%s" = %s;`, knobConfig.Name, strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64))
		adapter.Logger().Debugf(`Executing: %s`, query)

		_, err = adapter.pgDriver.Exec(context.Background(), query)
		if err != nil {
			return err
		}
	}

	if proposedConfig.KnobApplication == "restart" {
		// Restart the service
		adapter.Logger().Warn("Restarting service")

		// Execute systemctl restart command
		cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to restart PostgreSQL service: %w", err)
		}

		// Wait for PostgreSQL to be back online with retries
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
			case <-time.After(1 * time.Second):
				// Try to execute a simple query
				_, err := adapter.pgDriver.Exec(ctx, "SELECT 1")
				if err == nil {
					adapter.Logger().Info("PostgreSQL is back online")
					return nil
				}
				adapter.Logger().Debug("PostgreSQL not ready yet, retrying...")
			}
		}
	} else {
		// Reload database when everything is applied
		_, err := adapter.pgDriver.Exec(context.Background(), "SELECT pg_reload_conf();")
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO: This was heavily influenced by Claude, need to revisit and test properly
func getDiskType(adapter *DefaultPostgreSQLAdapter) (string, error) {
	// First we query PostgreSQL to get data directory mount point
	var dataDir string
	err := adapter.PGDriver().QueryRow(context.Background(), "SHOW data_directory;").Scan(&dataDir)
	if err != nil {
		return "UNKNOWN", err
	}

	// Resolve symlinks and get absolute path
	realPath, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return "UNKNOWN", err
	}

	// Get device name using df
	cmd := exec.Command("df", realPath)
	output, err := cmd.Output()
	if err != nil {
		return "UNKNOWN", err
	}

	// Parse df output - skip header line and get first field
	var deviceName string
	lines := strings.Split(string(output), "\n")
	if len(lines) >= 2 {
		fields := strings.Fields(lines[1])
		if len(fields) > 0 {
			deviceName = fields[0]
		}
	}

	// Get block storage information using ghw
	block, err := ghw.Block()
	if err != nil {
		return "UNKNOWN", err
	}

	// Find the disk type for the device
	for _, disk := range block.Disks {
		// Check if this disk matches our device
		possiblePaths := []string{
			deviceName,
			"/dev/" + filepath.Base(deviceName),
			disk.Name,
			"/dev/" + disk.Name,
		}

		for _, p := range possiblePaths {
			if p == deviceName {
				// Check for NVMe drives
				if disk.StorageController == ghw.STORAGE_CONTROLLER_NVME {
					return "NVME", nil
				}
				// Check for SSDs vs HDDs
				if disk.DriveType == ghw.DRIVE_TYPE_HDD {
					return "HDD", nil
				}
				return "SSD", nil
			}
		}
	}

	return "UNKNOWN", nil
}

// Guardrails for default PostgreSQL adapter performs the following:
// 1. Checks if the total memory is set. If not fetches it from the system and sets it in cache.
// 2. Fetches current memory usage
// 3. If memory usage is greater than 90% of total memory, triggers a critical guardrail
func (adapter *DefaultPostgreSQLAdapter) Guardrails() (*agent.GuardrailType, *agent.GuardrailMetric) {
	// Get memory info
	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		adapter.Logger().Error("Failed to get memory info:", err)
		return nil, nil
	}

	// Calculate memory usage percentage
	memoryUsagePercent := float64(memoryInfo.Total-memoryInfo.Available) / float64(memoryInfo.Total) * 100

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than 90% (default), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		level := agent.Critical
		metric := agent.Memory
		return &level, &metric
	}

	return nil, nil
}
