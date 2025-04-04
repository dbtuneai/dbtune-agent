package adapters

import (
	"context"
	"fmt"
	"math"
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

const (
	METRIC_RESOLUTION_SECONDS                       = 30
	GUARDRAIL_MEMORY_AVAILABLE_PERCENTAGE_THRESHOLD = 10.0
	GUARDRAIL_CONNECTION_USAGE_PERCENTAGE_THRESHOLD = 90.0
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

	// Set default values
	dbtuneConfig.SetDefault("guardrail_memory_available_percentage_threshold", GUARDRAIL_MEMORY_AVAILABLE_PERCENTAGE_THRESHOLD)
	dbtuneConfig.SetDefault("guardrail_connection_usage_percentage_threshold", GUARDRAIL_CONNECTION_USAGE_PERCENTAGE_THRESHOLD)
	dbtuneConfig.SetDefault("metric_resolution_seconds", METRIC_RESOLUTION_SECONDS)

	// Bind environment variables
	dbtuneConfig.BindEnv("api_token", "DBT_AIVEN_API_TOKEN")
	dbtuneConfig.BindEnv("project_name", "DBT_AIVEN_PROJECT_NAME")
	dbtuneConfig.BindEnv("service_name", "DBT_AIVEN_SERVICE_NAME")
	dbtuneConfig.BindEnv("guardrail_memory_available_percentage_threshold", "DBT_AIVEN_GUARDRAIL_MEMORY_AVAILABLE_PERCENTAGE_THRESHOLD")
	dbtuneConfig.BindEnv("guardrail_connection_usage_percentage_threshold", "DBT_AIVEN_GUARDRAIL_CONNECTION_USAGE_PERCENTAGE_THRESHOLD")
	dbtuneConfig.BindEnv("metric_resolution_seconds", "DBT_AIVEN_METRIC_RESOLUTION_SECONDS")
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

	// Since we are specifying in units of seconds, but a raw int such as 30
	// is interpreted as nanoseconds, we need to convert it
	aivenConfig.MetricResolutionSeconds = time.Duration(aivenConfig.MetricResolutionSeconds) * time.Second

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
			LastAppliedConfig:             time.Time{},
			LastGuardrailCheck:            time.Time{},
			LastMemoryAvailableTime:       time.Time{},
			LastMemoryAvailablePercentage: 100.0,
			LastHardwareInfoTime:          time.Time{},
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
	service, err := adapter.aivenClient.ServiceGet(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
		[2]string{"include_secrets", "false"},
	)

	if err != nil {
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

	userConfig := make(map[string]any)
	pgParamsMap := make(map[string]any)

	// Track if any changes were made
	changesMade := false

	// Debug: Print the proposed configuration
	adapter.Logger().Debugf("Proposed configuration: %+v", proposedConfig)

	// Apply the proposed config changes
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}

		// Debug: Print each knob being processed
		adapter.Logger().Debugf("Processing knob: %s = %v", knobConfig.Name, knobConfig.Setting)

		paramName := knobConfig.Name

		// TODO: We should just use their settings directly instead of converting or rounding
		// Unfortunately, they require `work_mem` to be set in MB and not KB. The search space
		// uses the KB units to be consistent with other database.
		var workMemMB int
		if paramName == "work_mem" {
			workMemKB := knobConfig.Setting.(float64)
			// The searchspace should provide nicely divisible values, but just in case,
			// we will round to the nearest MB
			workMemMB = int(math.Round(workMemKB / 1024.0))
			adapter.Logger().Debugf("Converting work_mem (%.2f KB) to work_mem (%d MB)", workMemKB, workMemMB)
			knobConfig.Setting = workMemMB
		}

		// Check if parameter is known to be modifiable
		paramInfo, ok := aivenModifiableParams[paramName]
		if !ok {
			adapter.Logger().Warnf(
				"Parameter %s has unknown modifiability status on Aiven. Skipping on applying the configuration.",
				paramName,
			)
			return nil
		}

		if !paramInfo.Modifiable {
			adapter.Logger().Warnf(
				"Parameter %s is known to be restricted by Aiven. Skipping on applying the configuration.",
				paramName,
			)
			return nil
		}

		// Convert setting to appropriate type
		var settingValue any
		switch v := knobConfig.Setting.(type) {
		// Base types, all good
		case string, int64, bool:
			settingValue = v
		case int:
			settingValue = int64(v)
		case float64:
			// For numeric parameters that should be integer, convert to integer format
			// Try to determine if this is an integer parameter based on the value
			if v == float64(int64(v)) {
				settingValue = int64(v)
			} else {
				settingValue = v
			}
		default:
			// For other types, convert to string
			settingValue = fmt.Sprintf("%v", v)
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

	updateReq := &service.ServiceUpdateIn{UserConfig: &userConfig, Powered: boolPtr(true)}

	// Debug: Print the final configuration being sent to Aiven
	adapter.Logger().Debugf("Final update request to be sent to Aiven: %+v", updateReq)

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

	adapter.Logger().Debugf("Aiven response: %+v", response)

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
			serviceResponse, err := adapter.aivenClient.ServiceGet(
				context.Background(),
				adapter.aivenConfig.ProjectName,
				adapter.aivenConfig.ServiceName,
			)
			if err != nil {
				adapter.Logger().Warnf("Failed to get service status: %v", err)
				continue
			}

			if serviceResponse.State == state {
				adapter.Logger().Infof("Service reached state: %s", state)
				return nil
			}
			adapter.Logger().Debugf("Service state: %s, waiting for: %s", serviceResponse.State, state)
		}
	}
}

// Guardrails implements resource usage guardrails
// Aiven only provides 30 second resolution data for hardware info, which we
// need for guardrails.
func (adapter *AivenPostgreSQLAdapter) Guardrails() *agent.GuardrailType {
	aivenState := adapter.GetAivenState()
	aivenConfig := adapter.GetAivenConfig()

	timeSinceLastGuardrailCheck := time.Since(aivenState.LastGuardrailCheck)
	if timeSinceLastGuardrailCheck < aivenConfig.MetricResolutionSeconds {
		adapter.Logger().Debugf(
			"Guardrails checked %s ago, lower than the resolution of %s, skipping",
			timeSinceLastGuardrailCheck,
			aivenConfig.MetricResolutionSeconds,
		)
		return nil
	}

	// NOTE: We use the latest obtained during HardwareInfo() if it's recent, otherwise,
	// re-fetch the metrics.
	var lastMemoryAvailablePercentage float64

	timeSinceLastMemoryAvailable := time.Since(aivenState.LastMemoryAvailableTime)
	if timeSinceLastMemoryAvailable < aivenConfig.MetricResolutionSeconds {
		adapter.Logger().Debugf(
			"Memory check %s ago, lower than the resolution of %s, using cached value",
			timeSinceLastMemoryAvailable,
			aivenConfig.MetricResolutionSeconds,
		)
		lastMemoryAvailablePercentage = aivenState.LastMemoryAvailablePercentage
	} else {
		adapter.Logger().Debugf(
			"Memory check %s ago, higher than the resolution of %s, fetching new value",
			timeSinceLastMemoryAvailable,
			aivenConfig.MetricResolutionSeconds,
		)
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
		aivenState.LastMemoryAvailableTime = memAvailableMetric.Timestamp
	}

	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")
	aivenState.LastGuardrailCheck = time.Now()

	memoryAvailablePercentage := lastMemoryAvailablePercentage

	if memoryAvailablePercentage < aivenConfig.GuardrailMemoryAvailablePercentageThreshold {
		adapter.Logger().Warnf(
			"Memory available: %.2f%% is under threshold %.2f%%, triggering critical guardrail",
			memoryAvailablePercentage,
			aivenConfig.GuardrailMemoryAvailablePercentageThreshold,
		)
		critical := agent.Critical
		return &critical
	} else {
		adapter.Logger().Infof(
			"Memory available: %.2f%% larger than threshold %.2f%%, no guardrail triggered",
			memoryAvailablePercentage,
			aivenConfig.GuardrailMemoryAvailablePercentageThreshold,
		)
	}

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
		return nil
	}

	// Calculate usage percentages
	connectionUsagePercent := (float64(connectionCount) / float64(maxConnections)) * 100

	if connectionUsagePercent > aivenConfig.GuardrailConnectionUsagePercentageThreshold {
		adapter.Logger().Warnf(
			"Connection usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			connectionUsagePercent,
			aivenConfig.GuardrailConnectionUsagePercentageThreshold,
		)
		critical := agent.Critical
		return &critical
	} else {
		adapter.Logger().Infof(
			"Connection usage: %.2f%% smaller than threshold %.2f%%, no guardrail triggered",
			connectionUsagePercent,
			aivenConfig.GuardrailConnectionUsagePercentageThreshold,
		)
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
