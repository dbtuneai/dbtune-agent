package adapters

import (
	"context"
	"fmt"
	"math"
	"strconv"
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
	DEFAULT_SHARED_BUFFERS_PERCENTAGE               = 0.20
	DEFAULT_PG_STAT_MONITOR_ENABLE                  = false
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

type ModifyLevel string

const (
	NoModify           ModifyLevel = "no_modify"      // Can not modify at all, usually due to 'parameter "x" cannot be changed now'
	ModifyAlterDB      ModifyLevel = "alter_db"       // Can modify with ALTER DATABASE <dbname> SET <param> = <value>
	ModifyUserPGConfig ModifyLevel = "user_pg_config" // Can modify via user config Aiven API, prefer over ModifyAlterDB
	ModifyServiceLevel ModifyLevel = "service_level"  // Can modify via user config Aiven API
)

// Ideally, we can remove the restart from most of these
var aivenModifiableParams = map[string]struct {
	ModifyLevel     ModifyLevel
	RequiresRestart bool
}{
	"shared_buffers_percentage":       {ModifyServiceLevel, true},
	"bgwriter_lru_maxpages":           {ModifyUserPGConfig, true},
	"bgwriter_delay":                  {ModifyUserPGConfig, true},
	"max_parallel_workers_per_gather": {ModifyUserPGConfig, true},
	"max_parallel_workers":            {ModifyUserPGConfig, true},
	"work_mem":                        {ModifyAlterDB, true},
	"random_page_cost":                {ModifyAlterDB, true},
	"seq_page_cost":                   {ModifyAlterDB, true},
	"effective_io_concurrency":        {ModifyAlterDB, true},
	"checkpoint_completion_target":    {NoModify, false},
	"max_wal_size":                    {NoModify, true},
	"min_wal_size":                    {NoModify, true},
	"shared_buffers":                  {NoModify, true}, // Done through shared_buffers_percentage
	"max_worker_processes":            {NoModify, true}, // BUG: Cannot decrease on Aiven's end
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
	dbtuneConfig.BindEnv("database_name", "DBT_AIVEN_DATABASE_NAME")
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

	initialServiceLevelParameters, err := getInitialServiceLevelParameters(
		&aivenClient,
		aivenConfig.ProjectName,
		aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial values: %v", err)
	}
	state := &adeptersinterfaces.AivenState{
		InitialSharedBuffersPercentage: initialServiceLevelParameters.InitialSharedBuffersPercentage,
		LastKnownPGStatMonitorEnable:   initialServiceLevelParameters.InitialPGStatMonitorEnable,
		LastAppliedConfig:              time.Time{},
		LastGuardrailCheck:             time.Time{},
		LastMemoryAvailableTime:        time.Time{},
		LastMemoryAvailablePercentage:  100.0,
		LastHardwareInfoTime:           time.Time{},
	}
	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		aivenConfig:              aivenConfig,
		aivenClient:              aivenClient,
		state:                    state,
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

	// Update the last known PGStatMonitorEnable state
	pgStatMonitorEnable, ok := service.UserConfig["pg_stat_monitor_enable"]
	if ok {
		adapter.state.LastKnownPGStatMonitorEnable = pgStatMonitorEnable.(bool)
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
// NOTE: This function does not handle restart knobs at the database level at the moment.
// The only known Knobs that we work with right now, that would require a restart are:
// - max_worker_processes (Bugged currently on Aiven's end)
// - shared_buffers_percentage
// Both of these can be set at the service level, which does the reload for us.
// However, they also alter them system wide, as opposed to just the database.
// This leads to an inconsistency if we were to tune multiple databases in the same cluster.
// We will need to revisit this in the future.
func (adapter *AivenPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying config")

	// List of knobs to be applied
	userConfig := make(map[string]any)
	pgSettings := make(map[string]any)
	restartRequired := false

	// Sort the knobs into dbLevelKnobs and serviceLevelKnobs
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}

		knobModifiability, ok := aivenModifiableParams[knobConfig.Name]
		if !ok {
			return fmt.Errorf("parameter %s has unknown modifiability status on Aiven. Skipping on applying the configuration", knobConfig.Name)
		}
		switch knobModifiability.ModifyLevel {
		case ModifyAlterDB:
			query := fmt.Sprintf(
				`ALTER DATABASE "%s" SET "%s" = %s;`,
				adapter.aivenConfig.DatabaseName,
				knobConfig.Name,
				strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64),
			)
			adapter.Logger().Debugf("Applying ALTER DATABASE query: %s", query)
			_, err := adapter.pgDriver.Exec(context.Background(), query)
			if err != nil {
				return err
			}

			// HACK: Toggling this plugin forces the service to restart PG quickly, causing connections
			// to reset and ALTER DATABASE queries to take effect for all new connections. If we have
			// any ALTER DB statements, then we insert this into the Aiven API call.
			userConfig["pg_stat_monitor_enable"] = !adapter.state.LastKnownPGStatMonitorEnable
			restartRequired = true
		case ModifyUserPGConfig:
			pgSettings[knobConfig.Name] = knobConfig.Setting
		case ModifyServiceLevel:
			// If it requires a restart, it will be automatically handled.
			userConfig[knobConfig.Name] = knobConfig.Setting
			if knobModifiability.RequiresRestart {
				restartRequired = true
			}
		case NoModify:
			return fmt.Errorf("parameter %s can not be modified on Aiven. Skipping on applying the configuration", knobConfig.Name)
		}
	}

	if len(pgSettings) > 0 {
		userConfig["pg"] = pgSettings
	}
	if len(userConfig) > 0 {
		adapter.Logger().Debugf("User config to be sent to Aiven: %+v", userConfig)

		// Apply the configuration update
		_, err := adapter.aivenClient.ServiceUpdate(
			context.Background(),
			adapter.aivenConfig.ProjectName,
			adapter.aivenConfig.ServiceName,
			&service.ServiceUpdateIn{UserConfig: &userConfig, Powered: boolPtr(true)},
		)
		if err != nil {
			return fmt.Errorf("failed to update PostgreSQL parameters: %v", err)
		}
	}

	if restartRequired {
		adapter.Logger().Info("Restart was required, checking if Aiven PostgreSQL service has restarted")
		err := adapter.waitForServiceState(service.ServiceStateTypeRunning)
		if err != nil {
			return err
		}
	}

	adapter.state.LastAppliedConfig = time.Now()
	return nil
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

type InitialServiceLevelParameters struct {
	InitialSharedBuffersPercentage float64
	InitialPGStatMonitorEnable     bool
}

func getInitialServiceLevelParameters(client *aiven.Client, projectName string, serviceName string) (InitialServiceLevelParameters, error) {
	aivenClient := *client
	service, err := aivenClient.ServiceGet(
		context.Background(),
		projectName,
		serviceName,
		[2]string{"include_secrets", "false"},
	)
	if err != nil {
		return InitialServiceLevelParameters{}, fmt.Errorf("failed to get service info: %v", err)
	}

	userConfig := service.UserConfig
	initialSharedBuffersPercentage, ok := userConfig["shared_buffers_percentage"]
	if !ok {
		initialSharedBuffersPercentage = DEFAULT_SHARED_BUFFERS_PERCENTAGE
	}
	initialPGStatMonitorEnable, ok := userConfig["pg_stat_monitor_enable"]
	if !ok {
		initialPGStatMonitorEnable = DEFAULT_PG_STAT_MONITOR_ENABLE
	}

	return InitialServiceLevelParameters{
		InitialSharedBuffersPercentage: initialSharedBuffersPercentage.(float64),
		InitialPGStatMonitorEnable:     initialPGStatMonitorEnable.(bool),
	}, nil
}

// GetActiveConfig returns the active configuration for the Aiven PostgreSQL service
// Specifically, this version does so at the database level, and also gets the
// `shared_buffers_percentage` parameter from the Aiven API.
func (adapter *AivenPostgreSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	logger := adapter.Logger()
	logger.Debug("Getting active config for Aiven PostgreSQL")
	// The primary changes in this function are to exclude
	// the `shared_buffers` pgDriver query,
	// and instead retrieve `shared_buffers_percentage` from Aiven.
	var configRows agent.ConfigArraySchema
	pgDriver := adapter.PGDriver()

	numericRows, err := pgDriver.Query(context.Background(), `
		SELECT name, setting::numeric as setting, unit, vartype, context 
		FROM pg_settings 
		WHERE vartype IN ('real', 'integer')
		AND name NOT IN ('shared_buffers');`)

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
	nonNumericRows, err := pgDriver.Query(context.Background(), `
		SELECT name, setting, unit, vartype, context 
		FROM pg_settings 
		WHERE vartype NOT IN ('real', 'integer')
		AND name NOT IN ('shared_buffers');`)

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

	// We further need to query Aiven's service for the `shared_buffers_percentage`
	// and the `work_mem` parameters.
	// The problem here is that until we modify the `shared_buffers_percentage`
	// or `work_mem`, we don't get anything back from Aiven for this GET request for them.
	// Hence we need to revert back to their default values.
	// ... but first, let's try the API
	client := *adapter.GetAivenClient()
	config := adapter.GetAivenConfig()
	state := adapter.GetAivenState()
	service, err := client.ServiceGet(
		context.Background(),
		config.ProjectName,
		config.ServiceName,
		[2]string{"include_secrets", "false"},
	)
	if err != nil {
		return nil, err
	}
	userConfig := service.UserConfig

	// Update the last known PGStatMonitorEnable state if we have it.
	pgStatMonitorEnable, ok := userConfig["pg_stat_monitor_enable"]
	if ok {
		state.LastKnownPGStatMonitorEnable = pgStatMonitorEnable.(bool)
	}

	// Try and get the `shared_buffers_percentage` from the user config
	// If they don't exist, use the default values.
	sharedBuffersPercentage, ok := userConfig["shared_buffers_percentage"]
	if !ok {
		sharedBuffersPercentage = state.InitialSharedBuffersPercentage
	}
	row := agent.PGConfigRow{
		Name:    "shared_buffers_percentage",
		Setting: sharedBuffersPercentage,
		Unit:    "percentage",
		Vartype: "real",
		Context: "service",
	}
	configRows = append(configRows, row)

	return configRows, nil
}
