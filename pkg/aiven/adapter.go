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
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

const (
	DEFAULT_SHARED_BUFFERS_PERCENTAGE = 20.0
	DEFAULT_PG_STAT_MONITOR_ENABLE    = false
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	Client            aivenclient.Client
	State             *State
	GuardrailSettings guardrails.Config
	pgConfig          pg.Config
	PGVersion         string
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
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
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
		return nil, fmt.Errorf("failed to create Aiven client: %w", err)
	}

	initialServiceLevelParameters, err := getInitialServiceLevelParameters(
		&aivenClient,
		aivenConfig.ProjectName,
		aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial values: %w", err)
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

	PGVersion, err := queries.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	pgMajorVersion := pg.ParsePgMajorVersion(PGVersion)
	collectorsConfig, err := pg.CollectorsConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to load collectors config: %w", err)
	}
	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		CommonAgent: *commonAgent,
		CatalogGetter: agent.CatalogGetter{
			PGPool:           pgPool,
			PGMajorVersion:   pgMajorVersion,
			PGConfig:         pgConfig,
			CollectorsConfig: collectorsConfig,
			HealthGate:       pg.NewHealthGate(pgPool, commonAgent.Logger()),
		},
		Config:            aivenConfig,
		Client:            aivenClient,
		State:             state,
		GuardrailSettings: guardrailSettings,
		pgConfig:          pgConfig,
		PGVersion:         PGVersion,
	}

	// Initialize collectors
	adapter.InitCollectors(AivenCollectors(adapter))

	return adapter, nil
}

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Info("Collecting Aiven system info")

	// Get service information from Aiven API
	service, err := adapter.Client.ServiceGet(
		ctx,
		adapter.Config.ProjectName,
		adapter.Config.ServiceName,
		[2]string{"include_secrets", "false"},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get service info: %w", err)
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
	pgVersion, err := queries.PGVersion(adapter.PGPool)
	if err != nil {
		return nil, err
	}

	maxConnections, err := queries.MaxConnections(adapter.PGPool)
	if err != nil {
		return nil, err
	}

	// Create metrics
	totalMemory, err := metrics.NodeMemoryTotal.AsFlatValue(totalMemoryBytes)
	if err != nil {
		adapter.Logger().Errorf("Error creating total memory metric: %v", err)
		return nil, err
	}

	noCPUsMetric, err := metrics.NodeCPUCount.AsFlatValue(numCPUs)
	if err != nil {
		adapter.Logger().Errorf("Error creating number of CPUs metric: %v", err)
		return nil, err
	}
	version, err := metrics.PGVersion.AsFlatValue(pgVersion)
	if err != nil {
		adapter.Logger().Errorf("Error creating PostgreSQL version metric: %v", err)
		return nil, err
	}

	// Aiven uses SSD storage
	// TODO: Verify this? Can't find anything in their API or website that says this, but it's a reasonable assumption
	diskTypeMetric, err := metrics.NodeStorageType.AsFlatValue("SSD")
	if err != nil {
		adapter.Logger().Errorf("Error creating disk type metric: %v", err)
		return nil, err
	}

	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		adapter.Logger().Errorf("Error creating max connections metric: %v", err)
		return nil, err
	}

	systemInfo := []metrics.FlatValue{
		version,
		totalMemory,
		maxConnectionsMetric,
		noCPUsMetric,
		diskTypeMetric,
	}

	return systemInfo, nil
}

// ApplyConfig applies configuration changes to the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) ApplyConfig(ctx context.Context, proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying config")

	// List of knobs to be applied
	userConfig := make(map[string]any)
	pgSettings := make(map[string]any)
	restartRequired := false

	// Sort the knobs into dbLevelKnobs and serviceLevelKnobs
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %w", err)
		}

		adapter.Logger().Debugf("Knob config: %+v", knobConfig)

		knobModifiability, ok := aivenModifiableParams[knobConfig.Name]
		if !ok {
			return fmt.Errorf("parameter %s has unknown modifiability status on Aiven. Skipping on applying the configuration", knobConfig.Name)
		}
		switch knobModifiability.ModifyLevel {
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
			ctx,
			adapter.Config.ProjectName,
			adapter.Config.ServiceName,
			&service.ServiceUpdateIn{UserConfig: &userConfig, Powered: boolPtr(true)},
		)
		if err != nil {
			return fmt.Errorf("failed to update PostgreSQL parameters: %w", err)
		}
	} else {
		adapter.Logger().Warnf("ApplyConfig was called with no changes to apply")
	}

	if restartRequired {
		if !agent.IsRestartAllowed() {
			return &agent.RestartNotAllowedError{
				Message: "restart is not allowed in the agent",
			}
		}
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
func (adapter *AivenPostgreSQLAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
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
			ctx,
			FetchedMetricsIn{
				Client:      &adapter.Client,
				ProjectName: adapter.Config.ProjectName,
				ServiceName: adapter.Config.ServiceName,
				Logger:      adapter.Logger(),
				Period:      service.PeriodTypeHour,
			},
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get fetched metric for guardrail: %v", err)
			return nil
		}
		memAvailableMetric := metrics[MEM_AVAILABLE_KEY]
		if memAvailableMetric.Value != nil {
			if perc, ok := memAvailableMetric.Value.(float64); ok {
				lastMemoryAvailablePercentage = perc
				adapter.State.LastMemoryAvailableTime = memAvailableMetric.Timestamp
			} else {
				adapter.Logger().Warnf("memAvailableMetric.Value is not a float64: %v", memAvailableMetric.Value)
				return nil
			}
		} else {
			adapter.Logger().Warn("memAvailableMetric.Value is nil")
			return nil
		}
	}

	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")
	adapter.State.LastGuardrailCheck = time.Now()

	memoryAvailablePercentage := lastMemoryAvailablePercentage

	if memoryAvailablePercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			memoryAvailablePercentage,
			adapter.GuardrailSettings.MemoryThreshold,
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
	collectors := agent.DefaultMetricCollectors(adapter.PGPool, adapter.pgConfig)
	return append(collectors, agent.MetricCollector{
		Key: "hardware",
		Collector: AivenHardwareInfo(
			&adapter.Client,
			adapter.Config.ProjectName,
			adapter.Config.ServiceName,
			adapter.Config.MetricResolution,
			adapter.Config,
			adapter.State,
			adapter.Logger(),
		),
	})
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
		return InitialServiceLevelParameters{}, fmt.Errorf("failed to get service info: %w", err)
	}

	userConfig := service.UserConfig

	var sharedBuffers float64
	if val, ok := userConfig["shared_buffers_percentage"]; ok && val != nil {
		if f, ok := val.(float64); ok {
			sharedBuffers = f
		} else {
			sharedBuffers = DEFAULT_SHARED_BUFFERS_PERCENTAGE
		}
	} else {
		sharedBuffers = DEFAULT_SHARED_BUFFERS_PERCENTAGE
	}

	var pgStatMonitorEnable bool
	if val, ok := userConfig["pg_stat_monitor_enable"]; ok && val != nil {
		if b, ok := val.(bool); ok {
			pgStatMonitorEnable = b
		} else {
			pgStatMonitorEnable = DEFAULT_PG_STAT_MONITOR_ENABLE
		}
	} else {
		pgStatMonitorEnable = DEFAULT_PG_STAT_MONITOR_ENABLE
	}

	return InitialServiceLevelParameters{
		InitialSharedBuffersPercentage: sharedBuffers,
		InitialPGStatMonitorEnable:     pgStatMonitorEnable,
	}, nil
}

// GetActiveConfig returns the active configuration for the Aiven API
// as well as through PostgreSQL
func (adapter *AivenPostgreSQLAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
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
		ctx,
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
	if val, ok := userConfig["pg_stat_monitor_enable"]; ok && val != nil {
		if b, ok := val.(bool); ok {
			adapter.State.LastKnownPGStatMonitorEnable = b
		}
	}

	// Try and get `shared_buffers_percentage` and `work_mem` from the user config
	// If they don't exist, it's likely it's unmodified from the defaults, and will be empty.
	var sharedBuffersPercentage float64
	if val, ok := userConfig["shared_buffers_percentage"]; ok && val != nil {
		if f, ok := val.(float64); ok {
			sharedBuffersPercentage = f
		} else {
			return nil, fmt.Errorf("shared_buffers_percentage is not convertable to float64! %v", val)
		}
	} else {
		sharedBuffersPercentage = DEFAULT_SHARED_BUFFERS_PERCENTAGE
	}

	configRows = append(configRows, agent.PGConfigRow{
		Name:    "shared_buffers_percentage",
		Setting: sharedBuffersPercentage,
		Unit:    "percentage",
		Vartype: "real",
		Context: "service",
	})

	rows, err := utils.QueryWithPrefix(adapter.PGPool, ctx, queries.SelectAllSettings)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var row agent.PGConfigRow
		err := rows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			adapter.Logger().Error("Error scanning pg_settings row", err)
			continue
		}

		row.Setting = queries.InferNumericType(row.Setting)

		if row.Name == "work_mem" {
			var workMemKb int64
			switch v := row.Setting.(type) {
			case int64:
				workMemKb = v
			case string:
				parsed, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					adapter.Logger().Error("Error converting work_mem to int64", err)
					continue
				}
				workMemKb = parsed
			default:
				adapter.Logger().Error("Error: unexpected work_mem type")
				continue
			}
			// Convert KB to MB using integer division
			workMemMB := int(workMemKb / 1024)
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

	return configRows, nil
}
