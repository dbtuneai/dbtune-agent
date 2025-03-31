package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aiven/aiven-go-client/v2"
	"github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"

	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	DefaultPostgreSQLAdapter
	aivenConfig     adeptersinterfaces.AivenConfig
	aivenClient     *aiven.Client
	state           *adeptersinterfaces.AivenState
}

// planMemoryMapping maps Aiven plan types to memory in bytes
var planMemoryMapping = map[string]int64{
	"hobbyist":  int64(1 * 1024 * 1024 * 1024),   // 1GB
	"startup-4": int64(4 * 1024 * 1024 * 1024),   // 4GB
	"startup-8": int64(8 * 1024 * 1024 * 1024),   // 8GB
	"business-4": int64(4 * 1024 * 1024 * 1024),  // 4GB
	"business-8": int64(8 * 1024 * 1024 * 1024),  // 8GB
	"business-16": int64(16 * 1024 * 1024 * 1024), // 16GB
	"premium-8": int64(8 * 1024 * 1024 * 1024),   // 8GB
	"premium-16": int64(16 * 1024 * 1024 * 1024), // 16GB
	"premium-32": int64(32 * 1024 * 1024 * 1024), // 32GB
}

// planCPUMapping maps Aiven plan types to CPU count
var planCPUMapping = map[string]int{
	"hobbyist":  1,
	"startup-4": 2,
	"startup-8": 2,
	"business-4": 2,
	"business-8": 4,
	"business-16": 4,
	"premium-8": 4,
	"premium-16": 8,
	"premium-32": 12,
}

// aivenModifiableParams defines which PostgreSQL parameters can be modified on Aiven
var aivenModifiableParams = map[string]bool{
    "bgwriter_lru_maxpages":         true,
    "bgwriter_delay":                true,
    "max_parallel_workers_per_gather": true,
    "max_parallel_workers":          true,
    
    // Known to be restricted (for documentation)
    "work_mem":                      false,
    "max_wal_size":                  false,
    "min_wal_size":                  false,
    "random_page_cost":              false,
    "seq_page_cost":                 false,
    "checkpoint_completion_target":  false,
    "effective_io_concurrency":      false,
    "shared_buffers":                false,
    "max_worker_processes":          false,
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
	aivenClient, err := aiven.NewTokenClient(aivenConfig.APIToken, "dbtune-agent")
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
		aivenConfig:             aivenConfig,
		aivenClient:             aivenClient,
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

// Interface implementation methods
func (adapter *AivenPostgreSQLAdapter) GetAivenState() *adeptersinterfaces.AivenState {
	return adapter.state
}

func (adapter *AivenPostgreSQLAdapter) GetAivenConfig() *adeptersinterfaces.AivenConfig {
	return &adapter.aivenConfig
}

func (adapter *AivenPostgreSQLAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

func (adapter *AivenPostgreSQLAdapter) APIClient() *retryablehttp.Client {
	return adapter.APIClient()
}

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Println("Collecting Aiven system info")

	var systemInfo []utils.FlatValue

	// Get service information from Aiven API
	service, err := adapter.aivenClient.Services.Get(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get service info: %v", err)
	}

	// Extract CPU and memory information based on service plan
	plan := service.Plan
	var numCPUs int
	var totalMemoryBytes int64

	// Get CPU count from plan mapping
	if cpuCount, ok := planCPUMapping[plan]; ok {
		numCPUs = cpuCount
	} else {
		// Default to 2 CPUs if plan is not recognized
		numCPUs = 2
		adapter.Logger().Warnf("Unknown Aiven plan %s, using default CPU count: %d", plan, numCPUs)
	}

	// Get memory from plan mapping
	if memory, ok := planMemoryMapping[plan]; ok {
		totalMemoryBytes = memory
	} else {
		// Default to 4GB if plan is not recognized
		totalMemoryBytes = 4 * 1024 * 1024 * 1024
		adapter.Logger().Warnf("Unknown Aiven plan %s, using default memory: %d bytes", plan, totalMemoryBytes)
	}

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

	// Get the current service configuration
	service, err := adapter.aivenClient.Services.Get(
		context.Background(),
		adapter.aivenConfig.ProjectName, 
		adapter.aivenConfig.ServiceName,
	)
	if err != nil {
		return fmt.Errorf("failed to get service configuration: %v", err)
	}

	// Start with current user_config or create a new one
	userConfig := service.UserConfig
	if userConfig == nil {
		userConfig = make(map[string]interface{})
	}

	// Ensure pg configuration section exists
	pgParams, ok := userConfig["pg"]
	if !ok || pgParams == nil {
		pgParams = make(map[string]interface{})
	}

	pgParamsMap, ok := pgParams.(map[string]interface{})
	if !ok {
		pgParamsMap = make(map[string]interface{})
	}

	// Apply the proposed config changes
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}
		
		// Check if parameter is known to be modifiable
		if modifiable, exists := aivenModifiableParams[knobConfig.Name]; exists && !modifiable {
			adapter.Logger().Warnf("Parameter %s is known to be restricted by Aiven, skipping", knobConfig.Name)
			continue
		} else if !exists {
			adapter.Logger().Warnf("Parameter %s has unknown modifiability status on Aiven, attempting anyway", knobConfig.Name)
		}
		
		// Convert setting to the appropriate type and format
		switch v := knobConfig.Setting.(type) {
		case float64:
			// For numeric parameters that should be integer, convert to integer format
			// Try to determine if this is an integer parameter based on the value
			if v == float64(int64(v)) {
				pgParamsMap[knobConfig.Name] = int64(v)
			} else {
				pgParamsMap[knobConfig.Name] = v
			}
		case bool:
			pgParamsMap[knobConfig.Name] = v
		case string:
			pgParamsMap[knobConfig.Name] = v
		default:
			// For other types, convert to string
			pgParamsMap[knobConfig.Name] = fmt.Sprintf("%v", v)
		}
	}

	// Update the user_config with modified pg parameters
	userConfig["pg"] = pgParamsMap
	
	// Create the update request
	updateReq := aiven.UpdateServiceRequest{
		UserConfig: userConfig,
	}

	// For parameters that require restart, we need to power cycle the service
	needsRestart := proposedConfig.KnobApplication == "restart"
	
	// Apply the configuration update
	_, err = adapter.aivenClient.Services.Update(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
		updateReq,
	)
	if err != nil {
		return fmt.Errorf("failed to update PostgreSQL parameters: %v", err)
	}

	// If restart is required, power-cycle the service
	if needsRestart {
		adapter.Logger().Info("Restarting Aiven PostgreSQL service")
        
		// First, power down the service
		_, err = adapter.aivenClient.Services.Update(
			context.Background(),
			adapter.aivenConfig.ProjectName,
			adapter.aivenConfig.ServiceName,
			aiven.UpdateServiceRequest{
				Powered: false,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to power down service: %v", err)
		}

		// Wait for the service to be powered down
		err = adapter.waitForServiceState("POWEROFF")
		if err != nil {
			return err
		}

		// Power the service back up
		_, err = adapter.aivenClient.Services.Update(
			context.Background(),
			adapter.aivenConfig.ProjectName,
			adapter.aivenConfig.ServiceName,
			aiven.UpdateServiceRequest{
				Powered: true,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to power up service: %v", err)
		}
	}

	// Wait for the service to be running again
	err = adapter.waitForServiceState("RUNNING")
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
func (adapter *AivenPostgreSQLAdapter) waitForServiceState(targetState string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for service to reach state %s", targetState)
		case <-time.After(10 * time.Second):
			service, err := adapter.aivenClient.Services.Get(
				context.Background(),
				adapter.aivenConfig.ProjectName,
				adapter.aivenConfig.ServiceName,
			)
			if err != nil {
				adapter.Logger().Warnf("Failed to get service status: %v", err)
				continue
			}

			if service.State == targetState {
				adapter.Logger().Infof("Service reached state: %s", targetState)
				return nil
			}
			adapter.Logger().Debugf("Service state: %s, waiting for: %s", service.State, targetState)
		}
	}
}

// Guardrails implements resource usage guardrails
func (adapter *AivenPostgreSQLAdapter) Guardrails() *agent.GuardrailType {
	if time.Since(adapter.state.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	
	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")

	if adapter.state.Hardware == nil {
		adapter.Logger().Warn("Hardware information not available, skipping guardrails")
		return nil
	}

	adapter.state.LastGuardrailCheck = time.Now()

	// Query database size for memory usage approximation
	var memoryUsage int64
	err := adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT COALESCE(sum(pg_database_size(datname)), 0) FROM pg_database`).Scan(&memoryUsage)
	
	if err != nil {
		// On error, try just the current database
		adapter.Logger().Warnf("Failed to get full memory usage, falling back to current database: %v", err)
		err = adapter.pgDriver.QueryRow(context.Background(), 
			`SELECT pg_database_size(current_database())`).Scan(&memoryUsage)
		
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage: %v", err)
			return nil
		}
	}

	// Get connection count for CPU usage approximation
	var connectionCount int
	err = adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle'`).Scan(&connectionCount)
	
	if err != nil {
		adapter.Logger().Errorf("Failed to get connection count: %v", err)
		connectionCount = 0
	}

	// Get max connections
	var maxConnections int
	err = adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT current_setting('max_connections')::int`).Scan(&maxConnections)
	
	if err != nil {
		adapter.Logger().Errorf("Failed to get max connections: %v", err)
		maxConnections = 100 // Default
	}

	// Calculate usage percentages
	memoryUsagePercent := (float64(memoryUsage) / float64(adapter.state.Hardware.TotalMemoryBytes)) * 100
	connectionUsagePercent := (float64(connectionCount) / float64(maxConnections)) * 100

	adapter.Logger().Infof("Memory usage: %.2f%%, Connection usage: %.2f%%", memoryUsagePercent, connectionUsagePercent)
	
	// If memory usage is over 90% or connection usage is over 90%, trigger critical guardrail
	if memoryUsagePercent > 90 || connectionUsagePercent > 90 {
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
			Collector:  collectors.AivenQueryRuntime(adapter), 
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
			Collector:  collectors.DatabaseSize(adapter), 
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
			Collector:  AivenHardwareInfo(adapter),
		},
	}
}

// AivenHardwareInfo collects hardware metrics for Aiven PostgreSQL
func AivenHardwareInfo(adapter *AivenPostgreSQLAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// For connection count as CPU usage indicator
		var connectionCount int
		err := adapter.pgDriver.QueryRow(ctx, 
			`SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle'`).Scan(&connectionCount)
		
		if err != nil {
			adapter.Logger().Warnf("Failed to get connection count: %v", err)
			connectionCount = 0
		}

		// Get max connections
		var maxConnections int
		err = adapter.pgDriver.QueryRow(ctx, 
			`SELECT current_setting('max_connections')::int`).Scan(&maxConnections)
		
		if err != nil {
			adapter.Logger().Warnf("Failed to get max connections: %v", err)
			maxConnections = 100 // Default
		}

		// Calculate CPU usage as connection percentage
		cpuUsage := (float64(connectionCount) / float64(maxConnections)) * 100
		cpuUsageMetric, _ := utils.NewMetric("node_cpu_usage", cpuUsage, utils.Float)
		state.AddMetric(cpuUsageMetric)

		// For memory used, check database size
		var memoryUsed int64
		err = adapter.pgDriver.QueryRow(ctx, 
			`SELECT COALESCE(sum(pg_database_size(datname)), 0) FROM pg_database`).Scan(&memoryUsed)
		
		if err != nil {
			// On error, try just the current database
			adapter.Logger().Warnf("Failed to get full memory usage, falling back to current database: %v", err)
			err = adapter.pgDriver.QueryRow(ctx, 
				`SELECT pg_database_size(current_database())`).Scan(&memoryUsed)
			
			if err != nil {
				adapter.Logger().Warnf("Failed to get memory usage: %v", err)
				memoryUsed = 0
			}
		}
		
		memoryUsedMetric, _ := utils.NewMetric("node_memory_used", memoryUsed, utils.Int)
		state.AddMetric(memoryUsedMetric)

		// For memory available, calculate from total minus used
		if adapter.state.Hardware != nil {
			memoryAvailable := adapter.state.Hardware.TotalMemoryBytes - memoryUsed
			if memoryAvailable < 0 {
				memoryAvailable = 0
			}
			
			memoryAvailableMetric, _ := utils.NewMetric("node_memory_available", memoryAvailable, utils.Int)
			state.AddMetric(memoryAvailableMetric)
		}

		return nil
	}
}
