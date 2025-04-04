package adapters

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	aiven "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	aivenutil "github.com/dbtuneai/agent/pkg/aivenutil"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"

	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	DefaultPostgreSQLAdapter
	aivenConfig adeptersinterfaces.AivenConfig
	aivenClient aiven.Client
	state       *adeptersinterfaces.AivenState
}

func (adapter *AivenPostgreSQLAdapter) GetAivenConfig() *adeptersinterfaces.AivenConfig {
	return &adapter.aivenConfig
}

func (adapter *AivenPostgreSQLAdapter) GetAivenClient() *aiven.Client {
	return &adapter.aivenClient
}

func (adapter *AivenPostgreSQLAdapter) GetAivenState() *adeptersinterfaces.AivenState {
	return adapter.state
}

// aivenModifiableParams defines which PostgreSQL parameters can be modified on Aiven
// and how they should be applied (pg config or service-level config)
var aivenModifiableParams = map[string]struct {
	Modifiable   bool
	ServiceLevel bool // If true, apply at service level instead of under pg config
}{
	// Parameters confirmed modifiable through pg config
	"bgwriter_lru_maxpages":           {true, false},
	"bgwriter_delay":                  {true, false},
	"max_parallel_workers_per_gather": {true, false},
	"max_parallel_workers":            {true, false},

	// Parameters that must be applied at service level
	"work_mem":                  {true, true},
	"shared_buffers_percentage": {true, true}, // Instead of shared_buffers

	// max_worker_processes can only be increased, not decreased
	"max_worker_processes": {true, true},

	// Known to be restricted (for documentation)
	"max_wal_size":                 {false, false},
	"min_wal_size":                 {false, false},
	"random_page_cost":             {false, false},
	"seq_page_cost":                {false, false},
	"checkpoint_completion_target": {false, false},
	"effective_io_concurrency":     {false, false},
	"shared_buffers":               {false, false}, // Use shared_buffers_percentage instead
}

// CreateAivenPostgreSQLAdapter creates a new Aiven PostgreSQL adapter
func CreateAivenPostgreSQLAdapter() (*AivenPostgreSQLAdapter, error) {
	// Load configuration from viper
	dbtuneConfig := viper.Sub("aiven")
	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	// Bind environment variables
	dbtuneConfig.BindEnv("api_token", "DBT_AIVEN_API_TOKEN")
	dbtuneConfig.BindEnv("project_name", "DBT_AIVEN_PROJECT_NAME")
	dbtuneConfig.BindEnv("service_name", "DBT_AIVEN_SERVICE_NAME")

	var aivenConfig adeptersinterfaces.AivenConfig
	err := dbtuneConfig.Unmarshal(&aivenConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %v", err)
	}

	// Validate required configuration
	err = utils.ValidateStruct(&aivenConfig)
	if err != nil {
		return nil, err
	}

	// Create Aiven client
	aivenClient, err := aiven.NewClient(aiven.TokenOpt(aivenConfig.APIToken))
	if err != nil {
		return nil, fmt.Errorf("failed to create Aiven client: %v", err)
	}

	// Create default PostgreSQL adapter as base
	defaultAdapter, err := CreateDefaultPostgreSQLAdapter()
	if err != nil {
		return nil, fmt.Errorf("failed to create base PostgreSQL adapter: %v", err)
	}

	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		aivenConfig:              aivenConfig,
		aivenClient:              aivenClient,
		state: &adeptersinterfaces.AivenState{
			LastAppliedConfig: time.Time{},
		},
	}

	// Initialize collectors
	adapter.MetricsState = agent.MetricsState{
		Collectors: AivenCollectors(adapter),
		Cache:      agent.Caches{},
		Mutex:      &sync.Mutex{},
	}

	return adapter, nil
}

func (adapter *AivenPostgreSQLAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Info("Collecting Aiven system info")

	var systemInfo []utils.FlatValue

	// Get service information from Aiven API
	ctx := context.Background()
	service, err := adapter.aivenClient.ServiceGet(
		ctx,
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
		[2]string{"include_secrets", "false"},
	)

	if err != nil {
		adapter.Logger().Errorf("failed to get service info: %v", err)
		return nil, fmt.Errorf("failed to get service info: %v", err)
	}

	numCPUs := *service.NodeCpuCount
	nodeMemoryMb := *service.NodeMemoryMb
	totalMemoryBytes := int64(math.Round(nodeMemoryMb * 1024.0 * 1024.0))

	// Store hardware information in state
	adapter.state.Hardware = &adeptersinterfaces.AivenHardwareState{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          numCPUs,
		LastChecked:      time.Now(),
	}

	// Get PostgreSQL version and max connections from database
	pgVersion, err := collectors.PGVersion(adapter)
	if err != nil {
		return nil, err
	}

	maxConnections, err := collectors.MaxConnections(adapter)
	if err != nil {
		return nil, err
	}

	// Create metrics
	totalMemory, _ := utils.NewMetric("node_memory_total", totalMemoryBytes, utils.Int)
	noCPUsMetric, _ := utils.NewMetric("node_cpu_count", numCPUs, utils.Int)
	version, _ := utils.NewMetric("pg_version", pgVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)

	// Aiven uses SSD storage
	// TODO: Verify this? Can't find anything in their API or website that says this, but it's a reasonable assumption
	diskTypeMetric, _ := utils.NewMetric("node_disk_device_type", "SSD", utils.String)

	systemInfo = append(systemInfo, version, totalMemory, maxConnectionsMetric, noCPUsMetric, diskTypeMetric)

	return systemInfo, nil
}

// ApplyConfig applies configuration changes to the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying Config to Aiven PostgreSQL: %s", proposedConfig.KnobApplication)

	// If the last applied config is less than 1 minute ago, return
	if adapter.state.LastAppliedConfig.Add(1 * time.Minute).After(time.Now()) {
		adapter.Logger().Info("Last applied config is less than 1 minute ago, skipping")
		return nil
	}

	userConfig := make(map[string]any)

	// Ensure pg configuration section exists
	pgParams, ok := userConfig["pg"]
	if !ok || pgParams == nil {
		pgParams = make(map[string]interface{})
	}

	pgParamsMap, ok := pgParams.(map[string]any)
	if !ok {
		pgParamsMap = make(map[string]any)
	}

	// Log file for debugging parameter changes
	var logWriter *bufio.Writer
	logFile, err := os.OpenFile("/tmp/aiven_param_changes.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		defer logFile.Close()
		logWriter = bufio.NewWriter(logFile)
		defer logWriter.Flush()
		fmt.Fprintf(logWriter, "[%s] Applying config changes to Aiven PostgreSQL\n", time.Now().Format(time.RFC3339))
	}

	// Track if any changes were made
	changesMade := false

	// Debug: Print the proposed configuration
	adapter.Logger().Debugf("Proposed configuration: %+v", proposedConfig)
	if logWriter != nil {
		fmt.Fprintf(logWriter, "Proposed configuration: %+v\n", proposedConfig)
	}

	// Apply the proposed config changes
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}

		// Debug: Print each knob being processed
		adapter.Logger().Debugf("Processing knob: %s = %v", knobConfig.Name, knobConfig.Setting)
		if logWriter != nil {
			fmt.Fprintf(logWriter, "Processing knob: %s = %v\n", knobConfig.Name, knobConfig.Setting)
		}

		// Handle shared_buffers specially - convert to shared_buffers_percentage if needed
		// This is a special case for Aiven
		// TODO: This needs to be replaced by `shared_buffers_percentage` for the backend.
		// We won't be using `"shared_buffers"` as originally thought, as the calculations
		// we do locally don't correspond to what Aiven does with the percentage, cuasing
		// a mismatch which halts the tuning session.
		paramName := knobConfig.Name
		if paramName == "shared_buffers" {
			// Convert shared_buffers to shared_buffers_percentage
			// Get total memory in bytes
			if adapter.state.Hardware != nil && adapter.state.Hardware.TotalMemoryBytes > 0 {

				var sharedBuffers8KB float64
				switch v := knobConfig.Setting.(type) {
				case float64:
					sharedBuffers8KB = v
				case int64:
					sharedBuffers8KB = float64(v)
				case int:
					sharedBuffers8KB = float64(v)
				default:
					adapter.Logger().Errorf("Invalid shared_buffers value: %v", v)
					continue
				}
				// Shared buffers in pg is in 8KB blocks, convert to bytes
				sharedBuffersBytes := sharedBuffers8KB * 8 * 1024
				// Calculate as percentage of total memory
				percentage := sharedBuffersBytes / float64(adapter.state.Hardware.TotalMemoryBytes) * 100
				// Ensure within valid range for Aiven (20-60%)
				percentage = math.Max(20, math.Min(60, percentage))

				// Convert this to the service setting name
				paramName = "shared_buffers_percentage"
				knobConfig.Setting = percentage
				adapter.Logger().Infof("Converting shared_buffers to shared_buffers_percentage: %.2f%%", percentage)
				if logFile != nil {
					fmt.Fprintf(logWriter, "Converting shared_buffers to shared_buffers_percentage: %.2f%%\n", percentage)
				}
			} else {
				adapter.Logger().Errorf("Cannot convert shared_buffers to percentage - hardware info not available")
				continue
			}
		}

		// Unfortunately, they require `work_mem` to be set in MB and not KB. The search space
		// uses the KB units to be consistent with other database.
		var workMemMB int64
		if paramName == "work_mem" {
			workMemKB := knobConfig.Setting.(float64)
			// The searchspace should provide nicely divisible values, but just in case,
			// we will round to the nearest MB
			workMemMB = int64(math.Round(workMemKB / 1024.0))
			adapter.Logger().Infof("Converting work_mem (%.2f KB) to work_mem (%d MB)", workMemKB, workMemMB)
			if logFile != nil {
				fmt.Fprintf(logFile, "Converting work_mem (%.2f KB) to work_mem (%d MB)", workMemKB, workMemMB)
			}
			knobConfig.Setting = workMemMB
		}

		// Check if parameter is known to be modifiable
		paramInfo, exists := aivenModifiableParams[paramName]
		if !exists {
			adapter.Logger().Warnf("Parameter %s has unknown modifiability status on Aiven, attempting anyway", paramName)
			if logFile != nil {
				fmt.Fprintf(logWriter, "Parameter %s has unknown status, attempting anyway\n", paramName)
			}
			// Default to applying under pg config
			paramInfo = struct {
				Modifiable   bool
				ServiceLevel bool
			}{true, false}
		} else if !paramInfo.Modifiable {
			adapter.Logger().Warnf("Parameter %s is known to be restricted by Aiven, skipping", paramName)
			if logFile != nil {
				fmt.Fprintf(logWriter, "Parameter %s is restricted, skipping\n", paramName)
			}
			continue
		}

		// Special handling for max_worker_processes - can only be increased
		// TODO: We don't tune this for now in the searchspace. The optimizer can't
		// handle this kind of thing anywho.
		if paramName == "max_worker_processes" {
			// Get current value from service
			var currentValue int
			if pgCurrentValue, ok := pgParamsMap[paramName]; ok {
				currentValue = int(pgCurrentValue.(float64))
			} else {
				// Try at service level
				if svcCurrentValue, ok := userConfig[paramName]; ok {
					currentValue = int(svcCurrentValue.(float64))
				}
			}

			// Check if new value is lower than current
			newValue := int(knobConfig.Setting.(float64))
			if newValue < currentValue {
				adapter.Logger().Warnf("Cannot decrease max_worker_processes from %d to %d on Aiven", currentValue, newValue)
				if logWriter != nil {
					fmt.Fprintf(logWriter, "Cannot decrease max_worker_processes from %d to %d\n", currentValue, newValue)
				}
				continue
			}
		}

		// Convert setting to appropriate type
		var settingValue any
		switch v := knobConfig.Setting.(type) {
		case float64:
			// For numeric parameters that should be integer, convert to integer format
			// Try to determine if this is an integer parameter based on the value
			if v == float64(int64(v)) {
				settingValue = int64(v)
			} else {
				settingValue = v
			}
		case bool:
			settingValue = v
		case string:
			settingValue = v
		case int64:
			settingValue = v
		case int:
			settingValue = int64(v)
		default:
			// For other types, convert to string
			settingValue = fmt.Sprintf("%v", v)
		}

		adapter.Logger().Infof("Setting %s = %v (service level: %v)", paramName, settingValue, paramInfo.ServiceLevel)
		if logWriter != nil {
			fmt.Fprintf(logWriter, "Setting %s = %v (service level: %v)\n", paramName, settingValue, paramInfo.ServiceLevel)
		}

		// Apply the parameter at the correct level (service level or pg config)
		if paramInfo.ServiceLevel {
			userConfig[paramName] = settingValue
		} else {
			pgParamsMap[paramName] = settingValue
		}

		changesMade = true
	}

	// Only update if changes were made
	if !changesMade {
		adapter.Logger().Info("No applicable changes to apply")
		return nil
	}

	// Update the user_config with modified pg parameters
	userConfig["pg"] = pgParamsMap

	// Create the update request
	updateReq := &service.ServiceUpdateIn{
		UserConfig: &userConfig,
		Powered:    boolPtr(true),
	}

	// Debug: Print the final configuration being sent to Aiven
	adapter.Logger().Infof("Final update request to be sent to Aiven: %+v", updateReq)
	if logWriter != nil {
		fmt.Fprintf(logWriter, "Final update request to be sent to Aiven: %+v\n", updateReq)
	}

	// Apply the configuration update
	response, err := adapter.aivenClient.ServiceUpdate(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
		&service.ServiceUpdateIn{
			UserConfig: &userConfig,
			Powered:    boolPtr(true),
		},
	)

	adapter.Logger().Infof("Aiven response: %+v", response)

	if err != nil {
		return fmt.Errorf("failed to update PostgreSQL parameters: %v", err)
	}

	// Wait for the service to be running again
	err = adapter.waitForServiceState(service.ServiceStateTypeRunning)
	if err != nil {
		return err
	}

	// Verify PostgreSQL is responding
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(5 * time.Second):
			_, err := adapter.pgDriver.Exec(ctx, "SELECT 1")
			if err == nil {
				adapter.Logger().Info("PostgreSQL is back online")
				adapter.state.LastAppliedConfig = time.Now()
				return nil
			}
			adapter.Logger().Debug("PostgreSQL not ready yet, retrying...")
		}
	}
}

// waitForServiceState waits for the service to reach the specified state
func (adapter *AivenPostgreSQLAdapter) waitForServiceState(state service.ServiceStateType) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for service to reach state %s", state)
		case <-time.After(10 * time.Second):
			service, err := adapter.aivenClient.ServiceGet(
				context.Background(),
				adapter.aivenConfig.ProjectName,
				adapter.aivenConfig.ServiceName,
			)
			if err != nil {
				adapter.Logger().Warnf("Failed to get service status: %v", err)
				continue
			}

			if service.State == state {
				adapter.Logger().Infof("Service reached state: %s", state)
				return nil
			}
			adapter.Logger().Debugf("Service state: %s, waiting for: %s", service.State, state)
		}
	}
}

// Guardrails implements resource usage guardrails
// Aiven only provides 30 second resolution data for hardware info, which we
// need for guardrails.
func (adapter *AivenPostgreSQLAdapter) Guardrails() *agent.GuardrailType {
	if time.Since(adapter.state.LastGuardrailCheck) < 30*time.Second {
		return nil
	}

	// NOTE: We use the latest obtained during HardwareInfo() if it's recent, otherwise,
	// re-fetch the metrics.
	var lastMemoryAvailablePercentage float64
	if time.Since(adapter.state.LastMemoryAvailableTime) < 30*time.Second {
		lastMemoryAvailablePercentage = adapter.state.LastMemoryAvailablePercentage
	} else {
		metrics, err := aivenutil.GetFetchedMetrics(
			context.Background(),
			aivenutil.FetchedMetricsIn{
				Client:      &adapter.aivenClient,
				ProjectName: adapter.aivenConfig.ProjectName,
				ServiceName: adapter.aivenConfig.ServiceName,
				Logger:      adapter.Logger(),
				Period:      service.PeriodTypeHour,
			},
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get fetched metric for gaurdrail: %v", err)
			return nil
		}
		memAvailableMetric := metrics[aivenutil.MemAvailable]
		lastMemoryAvailablePercentage = memAvailableMetric.Value.(float64)
		adapter.state.LastMemoryAvailableTime = memAvailableMetric.Timestamp
	}

	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")
	adapter.state.LastGuardrailCheck = time.Now()

	memoryAvailablePercentage := lastMemoryAvailablePercentage

	var err error
	var connectionCount int
	var maxConnections int
	err = adapter.pgDriver.QueryRow(context.Background(),
		`SELECT 
			(SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle') as active_connections,
			current_setting('max_connections')::int as max_connections
		`).Scan(&connectionCount, &maxConnections)

	if err != nil {
		adapter.Logger().Errorf("Failed to get connection metrics: %v", err)
		connectionCount = 0
		maxConnections = 100 // Default
	}

	// Calculate usage percentages
	connectionUsagePercent := (float64(connectionCount) / float64(maxConnections)) * 100

	adapter.Logger().Infof("Memory available : %.2f%%, Connection usage: %.2f%%", memoryAvailablePercentage, connectionUsagePercent)

	if memoryAvailablePercentage < 10 {
		adapter.Logger().Info("Memory available is under 10%, triggering critical guardrail")
		critical := agent.Critical
		return &critical
	}

	if connectionUsagePercent > 90 {
		adapter.Logger().Info("Connection usage is over 90%, triggering critical guardrail")
		critical := agent.Critical
		return &critical
	}

	return nil
}

// AivenCollectors returns the metrics collectors for Aiven PostgreSQL
func AivenCollectors(adapter *AivenPostgreSQLAdapter) []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  collectors.AivenQueryRuntime(adapter), // Use Aiven-specific collector
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(adapter),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(adapter),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  collectors.DatabaseSize(adapter), // Use standard collector for consistency
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  collectors.Autovacuum(adapter),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  collectors.Uptime(adapter),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  collectors.BufferCacheHitRatio(adapter),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  collectors.WaitEvents(adapter),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  collectors.AivenHardwareInfo(adapter),
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}
