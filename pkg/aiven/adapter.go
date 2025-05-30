package aiven

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	aivenclient "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/keywords"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/pg"

	"github.com/jackc/pgx/v5/pgtype"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

const (
	DEFAULT_SHARED_BUFFERS_PERCENTAGE = 20.0
	DEFAULT_PG_STAT_MONITOR_ENABLE    = false
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	agent.CommonAgent
	Config     Config
	Client     aivenclient.Client
	State      *State
	Gaurdrails guardrails.Config
	PGDriver   *pgPool.Pool
}

// CreateAivenPostgreSQLAdapter creates a new Aiven PostgreSQL adapter
func CreateAivenPostgreSQLAdapter() (*AivenPostgreSQLAdapter, error) {
	// Validate required configuration
	aivenConfig, err := ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for gaurdrails %w", err)
	}

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	pgPool, err := pgPool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	// Create Aiven client
	aivenClient, err := aivenclient.NewClient(aivenclient.TokenOpt(aivenConfig.APIToken))
	if err != nil {
		return nil, fmt.Errorf("failed to create Aiven client: %v", err)
	}

	initialServiceLevelParameters, err := getInitialServiceLevelParameters(
		&aivenClient,
		aivenConfig.ProjectName,
		aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial values: %v", err)
	}
	state := &State{
		InitialSharedBuffersPercentage: initialServiceLevelParameters.InitialSharedBuffersPercentage,
		LastKnownPGStatMonitorEnable:   initialServiceLevelParameters.InitialPGStatMonitorEnable,
		LastAppliedConfig:              time.Time{},
		LastGuardrailCheck:             time.Time{},
		LastMemoryAvailableTime:        time.Time{},
		LastMemoryAvailablePercentage:  100.0,
		LastHardwareInfoTime:           time.Time{},
	}

	commonAgent := agent.CreateCommonAgent()

	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		CommonAgent: *commonAgent,
		Config:      aivenConfig,
		Client:      aivenClient,
		State:       state,
		Gaurdrails:  guardrailSettings,
		PGDriver:    pgPool,
	}

	// Initialize collectors
	adapter.InitCollectors(AivenCollectors(adapter))

	return adapter, nil
}

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Info("Collecting Aiven system info")

	var systemInfo []utils.FlatValue

	// Get service information from Aiven API
	service, err := adapter.Client.ServiceGet(
		context.Background(),
		adapter.Config.ProjectName,
		adapter.Config.ServiceName,
		[2]string{"include_secrets", "false"},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get service info: %v", err)
	}

	numCPUs := *service.NodeCpuCount
	nodeMemoryMb := *service.NodeMemoryMb
	totalMemoryBytes := int64(math.Round(nodeMemoryMb * 1024.0 * 1024.0))

	// Store hardware information in state
	adapter.State.Hardware = Hardware{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          numCPUs,
		LastChecked:      time.Now(),
	}

	// Update the last known PGStatMonitorEnable state
	pgStatMonitorEnable, ok := service.UserConfig["pg_stat_monitor_enable"]
	if ok {
		adapter.State.LastKnownPGStatMonitorEnable = pgStatMonitorEnable.(bool)
	}

	// Get PostgreSQL version and max connections from database
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	// Create metrics
	totalMemory, _ := utils.NewMetric(keywords.NodeMemoryTotal, totalMemoryBytes, utils.Int)
	noCPUsMetric, _ := utils.NewMetric(keywords.NodeCPUCount, numCPUs, utils.Int)
	version, _ := utils.NewMetric(keywords.PGVersion, pgVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric(keywords.PGMaxConnections, maxConnections, utils.Int)

	// Aiven uses SSD storage
	// TODO: Verify this? Can't find anything in their API or website that says this, but it's a reasonable assumption
	diskTypeMetric, _ := utils.NewMetric(keywords.NodeStorageType, "SSD", utils.String)

	systemInfo = append(systemInfo, version, totalMemory, maxConnectionsMetric, noCPUsMetric, diskTypeMetric)

	return systemInfo, nil
}

// ApplyConfig applies configuration changes to the Aiven PostgreSQL service
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

		adapter.Logger().Debugf("Knob config: %+v", knobConfig)

		knobModifiability, ok := aivenModifiableParams[knobConfig.Name]
		if !ok {
			return fmt.Errorf("parameter %s has unknown modifiability status on Aiven. Skipping on applying the configuration", knobConfig.Name)
		}
		switch knobModifiability.ModifyLevel {
		case ModifyAlterDB:
			// NOTE: We may wish to have this, I'm not sure. In the
			// meantime it's left here. However I believe this should
			// really be a `panic()`, as if we hit this,
			// no tuning will happen but this will manifest as the
			// tuning session just stopping or hanging. To activate
			// this hack, you need to set the `DBT_AIVEN_DATABASE_NAME` in the config file.
			databaseName := adapter.Config.DatabaseName
			if databaseName == "" {
				return fmt.Errorf(
					"the ALTER DATABASE technique for setting some parameters is not officially supported. If you unexpectedly encounter this error, please contact DBtune support",
				)
			}
			err := pg.AlterDatabase(adapter.PGDriver, databaseName, knobConfig.Name, strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64))
			if err != nil {
				return fmt.Errorf("failed to alter database: %v", err)
			}

			// HACK: Toggling this plugin forces the service to restart PG quickly, causing connections
			// to reset and ALTER DATABASE queries to take effect for all new connections. If we have
			// any ALTER DB statements, then we insert this into the Aiven API call.
			userConfig["pg_stat_monitor_enable"] = !adapter.State.LastKnownPGStatMonitorEnable
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
		_, err := adapter.Client.ServiceUpdate(
			context.Background(),
			adapter.Config.ProjectName,
			adapter.Config.ServiceName,
			&service.ServiceUpdateIn{UserConfig: &userConfig, Powered: boolPtr(true)},
		)
		if err != nil {
			return fmt.Errorf("failed to update PostgreSQL parameters: %v", err)
		}
	} else {
		adapter.Logger().Warnf("ApplyConfig was called with no changes to apply")
	}

	if restartRequired {
		adapter.Logger().Info("Restart was required, checking if Aiven PostgreSQL service has restarted")
		err := adapter.waitForServiceState(service.ServiceStateTypeRunning)
		if err != nil {
			return err
		}
	}

	adapter.State.LastAppliedConfig = time.Now()
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
			serviceResponse, err := adapter.Client.ServiceGet(
				context.Background(),
				adapter.Config.ProjectName,
				adapter.Config.ServiceName,
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
func (adapter *AivenPostgreSQLAdapter) Guardrails() *guardrails.Signal {

	timeSinceLastGuardrailCheck := time.Since(adapter.State.LastGuardrailCheck)
	if timeSinceLastGuardrailCheck < adapter.Config.MetricResolution {
		adapter.Logger().Debugf(
			"Guardrails checked %s ago, lower than the resolution of %s, skipping",
			timeSinceLastGuardrailCheck,
			adapter.Config.MetricResolution,
		)
		return nil
	}

	// NOTE: We use the latest obtained during HardwareInfo() if it's recent, otherwise,
	// re-fetch the metrics.
	var lastMemoryAvailablePercentage float64

	timeSinceLastMemoryAvailable := time.Since(adapter.State.LastMemoryAvailableTime)
	if timeSinceLastMemoryAvailable < adapter.Config.MetricResolution {
		adapter.Logger().Debugf(
			"Memory check %s ago, lower than the resolution of %s, using cached value",
			timeSinceLastMemoryAvailable,
			adapter.Config.MetricResolution,
		)
		lastMemoryAvailablePercentage = adapter.State.LastMemoryAvailablePercentage
	} else {
		adapter.Logger().Debugf(
			"Memory check %s ago, higher than the resolution of %s, fetching new value",
			timeSinceLastMemoryAvailable,
			adapter.Config.MetricResolution,
		)
		metrics, err := GetFetchedMetrics(
			context.Background(),
			FetchedMetricsIn{
				Client:      &adapter.Client,
				ProjectName: adapter.Config.ProjectName,
				ServiceName: adapter.Config.ServiceName,
				Logger:      adapter.Logger(),
				Period:      service.PeriodTypeHour,
			},
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get fetched metric for gaurdrail: %v", err)
			return nil
		}
		memAvailableMetric := metrics[MEM_AVAILABLE_KEY]
		lastMemoryAvailablePercentage = memAvailableMetric.Value.(float64)
		adapter.State.LastMemoryAvailableTime = memAvailableMetric.Timestamp
	}

	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")
	adapter.State.LastGuardrailCheck = time.Now()

	memoryAvailablePercentage := lastMemoryAvailablePercentage

	if memoryAvailablePercentage > adapter.Gaurdrails.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			memoryAvailablePercentage,
			adapter.Gaurdrails.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

// AivenCollectors returns the metrics collectors for Aiven PostgreSQL
func AivenCollectors(adapter *AivenPostgreSQLAdapter) []agent.MetricCollector {
	pgDriver := adapter.PGDriver
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
			Collector:  pg.DatabaseSize(pgDriver), // Use standard collector for consistency
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  pg.Autovacuum(pgDriver),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  pg.Uptime(pgDriver),
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
			Collector: AivenHardwareInfo(
				&adapter.Client,
				adapter.Config.ProjectName,
				adapter.Config.ServiceName,
				adapter.Config.MetricResolution,
				adapter.Config,
				adapter.State,
				adapter.Logger(),
			),
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

func getInitialServiceLevelParameters(client *aivenclient.Client, projectName string, serviceName string) (InitialServiceLevelParameters, error) {
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

// GetActiveConfig returns the active configuration for the Aiven API
// as well as through PostgreSQL
func (adapter *AivenPostgreSQLAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	// Main differences from the PostgreSQL version:
	// * We need to query Aiven's service for the `shared_buffers_percentage` parameter.
	// The problem here is that until we modify `shared_buffers_percentage`, we don't get
	// anything back from Aiven.  In that case, we use a known default of 20%.
	// * While we set `work_mem` through the Aiven API, it seems that querying for it
	// does not always return the most recent value, causing us to get a mismatch. Hence
	// we parse from PostgreSQL directly and convert to the MB, the units Aiven uses.
	logger := adapter.Logger()
	logger.Debug("Getting active config for Aiven PostgreSQL")
	var configRows agent.ConfigArraySchema
	service, err := adapter.Client.ServiceGet(
		context.Background(),
		adapter.Config.ProjectName,
		adapter.Config.ServiceName,
		[2]string{"include_secrets", "false"},
	)
	if err != nil {
		return nil, err
	}

	userConfig := service.UserConfig

	// HACK: This isn't for configRows, it's purely to keep track of
	// this parameter, which we use to trigger session restarts.
	// There's no point re-querying the service for this information
	// elsewhere so we do it here.
	pgStatMonitorEnable, ok := userConfig["pg_stat_monitor_enable"]
	if ok {
		adapter.State.LastKnownPGStatMonitorEnable = pgStatMonitorEnable.(bool)
	}

	// Try and get `shared_buffers_percentage` and `work_mem` from the user config
	// If they don't exist, it's likely it's unmodified from the defaults, and will be empty.
	sharedBuffersPercentage, ok := userConfig["shared_buffers_percentage"]
	if !ok {
		sharedBuffersPercentage = adapter.State.InitialSharedBuffersPercentage
	}
	configRows = append(configRows, agent.PGConfigRow{
		Name:    "shared_buffers_percentage",
		Setting: sharedBuffersPercentage,
		Unit:    "percentage",
		Vartype: "real",
		Context: "service",
	})

	numericRows, err := adapter.PGDriver.Query(context.Background(), pg.SELECT_NUMERIC_SETTINGS)

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

		if row.Name == "work_mem" {
			workMemKb := row.Setting.(pgtype.Numeric)
			workMemKbInt, err := workMemKb.Int64Value()
			if err != nil {
				adapter.Logger().Error("Error converting work_mem to int64", err)
				continue
			}
			// Convert KB to MB using integer division
			workMemMB := int(workMemKbInt.Int64 / 1024)
			configRows = append(configRows, agent.PGConfigRow{
				Name:    "work_mem",
				Setting: workMemMB,
				Unit:    "MB",
				Vartype: "integer",
				Context: "service",
			})
		} else {
			configRows = append(configRows, row)
		}

	}

	// Query for non-numeric types
	nonNumericRows, err := adapter.PGDriver.Query(context.Background(), pg.SELECT_NON_NUMERIC_SETTINGS)
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
