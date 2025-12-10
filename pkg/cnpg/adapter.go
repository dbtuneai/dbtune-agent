package cnpg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/kubernetes"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// CNPG Kubernetes API constants
	CNPGAPIGroup     = "postgresql.cnpg.io"
	CNPGAPIVersion   = "v1"
	CNPGResourceName = "clusters"

	// CNPG label for cluster identification
	CNPGClusterLabel = "cnpg.io/cluster"

	// CNPG parameters path in the Cluster CRD spec
	CNPGParametersPath = "spec.postgresql.parameters"

	// Configuration application limits
	ConfigApplyMaxRetries = 5
	ConfigApplyRetryDelay = 5 * time.Second

	// Rolling restart wait times - increased for production scenarios where
	// replicas may take longer to catch up with WAL during heavy workloads
	MaxWaitTimeClusterHealthy  = 60 * time.Minute
	PollIntervalClusterHealthy = 5 * time.Second
)

type CNPGAdapter struct {
	agent.CommonAgent
	GuardrailSettings guardrails.Config
	Config            Config
	PGDriver          *pgxpool.Pool
	PGVersion         string
	K8sClient         kubernetes.Client
	State             *State
}

func CreateCNPGAdapter() (*CNPGAdapter, error) {
	// Load CNPG config
	config, err := ConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to load CNPG config: %w", err)
	}

	// Load guardrail settings
	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load guardrail settings: %w", err)
	}

	// Create Kubernetes client
	client, err := kubernetes.CreateClient(config.KubeconfigPath, config.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Resolve pod name - either use explicit config or auto-discover primary
	podName := config.PodName
	if podName == "" {
		if config.ClusterName == "" {
			return nil, fmt.Errorf("either pod_name or cluster_name must be specified in CNPG config")
		}
		// Auto-discover primary pod from cluster
		ctx := context.Background()
		discoveredPod, err := client.FindCNPGPrimaryPod(ctx, config.ClusterName)
		if err != nil {
			return nil, fmt.Errorf("failed to auto-discover primary pod: %w", err)
		}
		podName = discoveredPod
		log.Infof("Auto-discovered primary pod: %s", podName)
	}
	// Update config with resolved pod name
	config.PodName = podName

	// Create PostgreSQL connection pool
	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load PostgreSQL config: %w", err)
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Get PostgreSQL version
	pgVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	adapter := &CNPGAdapter{
		CommonAgent:       *agent.CreateCommonAgent(),
		Config:            config,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         pgVersion,
		K8sClient:         client,
		State:             &State{LastGuardrailCheck: time.Now()},
	}

	// Initialize operations context for cancellation support
	adapter.State.CreateOperationsContext()

	// Initialize collectors
	// Use cluster name (not pod name) so metrics always come from current primary
	adapter.InitCollectors(Collectors(
		dbpool,
		client,
		config.ClusterName,
		config.ContainerName,
		pgVersion,
		adapter.Logger(),
	))

	return adapter, nil
}

func (adapter *CNPGAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// https://cloudnative-pg.io/documentation/1.27/postgresql_conf/#changing-configuration
	ctx := context.Background()
	logger := adapter.Logger()

	logger.Infof("[FAILOVER_RECOVERY] ApplyConfig called: knob_application=%s, num_params=%d",
		proposedConfig.KnobApplication, len(proposedConfig.Config))

	// Check for failover before applying new configuration
	// This ensures we don't apply tuning parameters after a failover has occurred
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Warnf("[FAILOVER_RECOVERY] Failover check BLOCKED config application: %s", failoverErr.Message)
			// Return error to block this config application
			return failoverErr
		}
		// Other error checking failover status - log but continue
		logger.Warnf("Failed to check for failover: %v", err)
	}

	logger.Infof("[FAILOVER_RECOVERY] Failover check PASSED - proceeding with config application")

	// Get cluster name from pod's CNPG cluster label
	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster name: %w", err)
	}
	logger.Infof("Applying configuration to CNPG cluster: %s (knob_application: %s)", clusterName, proposedConfig.KnobApplication)

	// Parse and validate all knobs upfront
	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return fmt.Errorf("failed to parse knob configurations: %w", err)
	}

	if len(parsedKnobs) == 0 {
		logger.Info("No configuration changes to apply")
		return nil
	}

	// Get current configuration from PostgreSQL to compare
	// This prevents unnecessary restarts by only applying parameters that actually changed
	logger.Info("Fetching current configuration from database for comparison")
	currentConfig, err := adapter.GetCurrentConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current config for comparison: %w", err)
	}
	logger.Infof("Successfully retrieved %d current parameter values", len(currentConfig))

	// Build parameters map for the patch
	// CNPG requires memory parameters in human-readable format (e.g., "2GB" not "262144")
	parametersMap := make(map[string]string)
	skippedUnchanged := 0
	for _, knob := range parsedKnobs {
		// Skip CNPG-managed parameters that should not be modified by tuning
		// CRITICAL: Backend sends these (e.g., shared_preload_libraries="")
		// which would break the cluster by removing pg_stat_statements!
		if IsCNPGManagedParameter(knob.Name) {
			logger.Warnf("Skipping CNPG-managed parameter: %s (value: %s)", knob.Name, knob.SettingValue)
			continue
		}

		// Convert PostgreSQL values to CNPG format (handles memory unit conversion)
		cnpgValue := ConvertToCNPGFormat(knob.Name, knob.SettingValue, nil)

		// Compare with current value - only apply if changed
		// This prevents CNPG from auto-triggering restart when it sees unchanged restart params
		currentValue, exists := currentConfig[knob.Name]
		if exists {
			// Normalize for comparison (handles "2GB" vs "2048MB" etc)
			if NormalizeValue(currentValue) == NormalizeValue(cnpgValue) {
				logger.Debugf("Skipping unchanged parameter: %s = %s (current: %s)", knob.Name, cnpgValue, currentValue)
				skippedUnchanged++
				continue
			}
			logger.Infof("Will change %s: %s → %s", knob.Name, currentValue, cnpgValue)
		} else {
			logger.Infof("Will set %s = %s (new parameter)", knob.Name, cnpgValue)
		}

		parametersMap[knob.Name] = cnpgValue
	}

	if skippedUnchanged > 0 {
		logger.Infof("Skipped %d unchanged parameters to avoid unnecessary restart", skippedUnchanged)
	}

	if len(parametersMap) == 0 {
		logger.Info("No configuration changes needed - all parameters already at target values")
		return nil
	}

	logger.Infof("Applying %d changed parameters to cluster", len(parametersMap))

	// Check if any of the CHANGED parameters require restart
	requiresRestart, restartRequiredParams, err := adapter.CheckRestartRequired(ctx, parametersMap)
	if err != nil {
		logger.Warnf("Failed to check restart requirement: %v. Will trigger restart to be safe.", err)
		requiresRestart = proposedConfig.KnobApplication == "restart"
	}

	if proposedConfig.KnobApplication == "reload" && requiresRestart {
		logger.Warnf("Reload-only mode: skipping %d restart-required parameters", len(restartRequiredParams))
		for _, param := range restartRequiredParams {
			logger.Warnf("Skipping restart-required parameter: %s (value: %s)", param, parametersMap[param])
			delete(parametersMap, param)
		}

		if len(parametersMap) == 0 {
			logger.Warn("No reload-able parameters to apply after filtering out restart-required ones")
			return nil
		}

		logger.Infof("Will apply %d reload-only parameters", len(parametersMap))
		requiresRestart = false
	}

	if requiresRestart {
		logger.Info("Configuration changes include restart-required parameters")
	} else {
		logger.Info("Configuration changes only include reload parameters (no restart needed)")
	}

	// Apply the patch using generic Kubernetes patching function
	err = kubernetes.PatchCRDParameters(kubernetes.CRDPatchRequest{
		Ctx:    ctx,
		Client: adapter.K8sClient,
		GVR: schema.GroupVersionResource{
			Group:    CNPGAPIGroup,
			Version:  CNPGAPIVersion,
			Resource: CNPGResourceName,
		},
		ResourceName:   clusterName,
		ParametersPath: CNPGParametersPath,
		Parameters:     parametersMap,
		MaxRetries:     ConfigApplyMaxRetries,
		RetryDelay:     ConfigApplyRetryDelay,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("failed to apply configuration patch: %w", err)
	}

	if requiresRestart {
		// Trigger rolling restart for restart-required parameters
		err = kubernetes.TriggerCNPGRollingRestart(ctx, adapter.K8sClient, clusterName, logger)
		if err != nil {
			return fmt.Errorf("failed to trigger rolling restart: %w", err)
		}

		// Wait for cluster to become healthy
		err = kubernetes.WaitForCNPGClusterHealthy(
			ctx,
			adapter.K8sClient,
			clusterName,
			MaxWaitTimeClusterHealthy,
			PollIntervalClusterHealthy,
			logger,
		)
		if err != nil {
			return fmt.Errorf("cluster did not become healthy after rolling restart: %w", err)
		}

		// Re-discover the current primary pod (may have changed during restart)
		newPrimaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(ctx, clusterName)
		if err != nil {
			return fmt.Errorf("failed to find primary pod after restart: %w", err)
		}
		if newPrimaryPod != adapter.Config.PodName {
			logger.Infof("Primary pod changed from %s to %s after rolling restart", adapter.Config.PodName, newPrimaryPod)
			adapter.Config.PodName = newPrimaryPod
		}

		// Wait for PostgreSQL to accept connections
		logger.Info("Waiting for PostgreSQL to accept connections...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			return fmt.Errorf("PostgreSQL did not become ready after restart: %w", err)
		}
		logger.Info("PostgreSQL is accepting connections - configuration applied")
	} else {
		// For reload-only parameters, CNPG automatically reloads config
		logger.Info("Configuration patch applied (reload-only parameters, no restart needed)")

		// Wait for PostgreSQL to accept connections and verify health
		logger.Info("Waiting for PostgreSQL to accept connections after reload...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			return fmt.Errorf("PostgreSQL did not become ready after config reload: %w", err)
		}

		// Verify pod is still ready
		podClient := adapter.K8sClient.PodClient(adapter.Config.PodName)
		ready, err := podClient.IsPodReady(ctx)
		if err != nil {
			return fmt.Errorf("failed to verify pod status after config reload: %w", err)
		}
		if !ready {
			return fmt.Errorf("pod became not ready after config reload")
		}
		logger.Info("Configuration reload completed successfully - PostgreSQL is healthy")
	}

	// Update state to track when we last applied config
	adapter.State.UpdateLastAppliedConfig()

	return nil
}

// getClusterName returns the CNPG cluster name from config
// This is reliable even during failover when pods may be deleted/recreated
func (adapter *CNPGAdapter) getClusterName(ctx context.Context) (string, error) {
	if adapter.Config.ClusterName == "" {
		return "", fmt.Errorf("cluster name not configured")
	}
	return adapter.Config.ClusterName, nil
}

func (adapter *CNPGAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	logger := adapter.Logger()

	// Check for failover before querying PostgreSQL
	// During recovery, PostgreSQL may be unavailable or unreliable
	if err := adapter.CheckForFailover(context.Background()); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			return agent.ConfigArraySchema{}, failoverErr
		}
		// Non-failover error (cluster status check failed, pod not found, etc.)
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetActiveConfig due to cluster check failure: %v", err)
		return agent.ConfigArraySchema{}, err
	}

	// Get operations context AFTER CheckForFailover passes
	// This ensures we get the fresh context created by recovery completion, not the pre-cancelled one
	ctx := adapter.State.GetOperationsContext()

	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx, logger)
	if err != nil {
		// If context was cancelled (failover detected mid-query), return FailoverDetectedError
		if ctx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetActiveConfig aborted due to context cancellation")
			return agent.ConfigArraySchema{}, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(context cancelled)",
				Message:    "operation cancelled due to failover",
			}
		}

		// Check if error indicates PostgreSQL is shutting down (failover in progress)
		// This can happen before CNPG cluster status reflects the failover
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "database system is shutting down") ||
			strings.Contains(errStr, "database system is starting up") ||
			strings.Contains(errStr, "cannot execute") && strings.Contains(errStr, "recovery") {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL reports failover in progress: %v", err)

			// Trigger failover detection immediately
			// Don't wait for CNPG cluster status to update (can take 5+ seconds)
			if adapter.State.TimeSinceLastFailover() == 0 {
				// First detection - record failover time
				adapter.State.SetLastFailoverTime(time.Now())
				adapter.State.CancelOperations()
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error (before CNPG status update)")

				// Send failover notification
				errorPayload := agent.ErrorPayload{
					ErrorMessage: "Failover detected: " + err.Error(),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
			}

			// Return FailoverDetectedError - runner will skip sending error and config
			logger.Infof("[FAILOVER_RECOVERY] Returning FailoverDetectedError to prevent config_error")
			return agent.ConfigArraySchema{}, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(PostgreSQL shutting down)",
				Message:    err.Error(),
			}
		}

		// For other errors, return as-is
		return agent.ConfigArraySchema{}, err
	}

	return config, nil
}

// GetMetrics overrides CommonAgent.GetMetrics to check for failover before collecting metrics
func (adapter *CNPGAdapter) GetMetrics() ([]metrics.FlatValue, error) {
	logger := adapter.Logger()

	// Check for failover before collecting metrics
	// During recovery, PostgreSQL and primary pod may be unavailable
	if err := adapter.CheckForFailover(context.Background()); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return FailoverDetectedError - runner will skip sending error and metrics
			return []metrics.FlatValue{}, failoverErr
		}
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetMetrics due to cluster check failure: %v", err)
		return []metrics.FlatValue{}, err
	}

	// Cluster is healthy - proceed with normal metrics collection
	// Note: collectors internally use their own context, but this is the entry point check
	return adapter.CommonAgent.GetMetrics()
}

func (adapter *CNPGAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	logger := adapter.Logger()
	var flatValues []metrics.FlatValue

	// Check for failover before collecting system info
	// During recovery, PostgreSQL and primary pod may be unavailable
	if err := adapter.CheckForFailover(context.Background()); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return FailoverDetectedError - runner will skip sending error and system info
			return []metrics.FlatValue{}, failoverErr
		}
		// Non-failover error (cluster status check failed, pod not found, etc.)
		// Still block to avoid sending errors during unstable cluster state
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetSystemInfo due to cluster check failure: %v", err)
		return []metrics.FlatValue{}, err
	}

	// Get operations context AFTER CheckForFailover passes
	// This ensures we get the fresh context created by recovery completion, not the pre-cancelled one
	ctx := adapter.State.GetOperationsContext()

	// Get PostgreSQL version
	pgVersionMetric, err := metrics.PGVersion.AsFlatValue(adapter.PGVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG version metric: %w", err)
	}
	flatValues = append(flatValues, pgVersionMetric)

	// Get max_connections from PostgreSQL
	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		// Check if context was cancelled (failover during operation)
		if ctx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetSystemInfo aborted due to context cancellation")
			return []metrics.FlatValue{}, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(context cancelled)",
				Message:    "operation cancelled due to failover",
			}
		}

		// Check if error indicates PostgreSQL failover (before CNPG detects it)
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "database system is shutting down") ||
			strings.Contains(errStr, "database system is starting up") ||
			strings.Contains(errStr, "cannot execute") && strings.Contains(errStr, "recovery") {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected in GetSystemInfo: %v", err)
			return []metrics.FlatValue{}, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(PostgreSQL shutting down)",
				Message:    err.Error(),
			}
		}

		return nil, fmt.Errorf("failed to get max_connections: %w", err)
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to create max_connections metric: %w", err)
	}
	flatValues = append(flatValues, maxConnectionsMetric)

	// Dynamically discover current primary pod for system info
	primaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(ctx, adapter.Config.ClusterName)
	if err != nil {
		// Check if context was cancelled (failover during operation)
		if ctx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetSystemInfo aborted due to context cancellation")
			return []metrics.FlatValue{}, nil
		}

		// Check if error indicates failover (no primary pod during transition)
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no primary pod") || strings.Contains(errStr, "not found") {
			logger.Warnf("[FAILOVER_RECOVERY] Primary pod not found in GetSystemInfo (likely failover): %v", err)
			// Return empty to suppress error
			return []metrics.FlatValue{}, nil
		}

		return nil, fmt.Errorf("failed to find CNPG primary pod: %w", err)
	}

	containerClient := adapter.K8sClient.ContainerClient(primaryPod, adapter.Config.ContainerName)
	systemInfo, err := containerClient.GetContainerSystemInfo(ctx)
	if err != nil {
		// Check if context was cancelled (failover during operation)
		if ctx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetSystemInfo aborted due to context cancellation")
			return []metrics.FlatValue{}, nil
		}
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}
	flatValues = append(flatValues, systemInfo...)

	return flatValues, nil
}

func (adapter *CNPGAdapter) Guardrails() *guardrails.Signal {
	// Check if enough time has passed since the last guardrail check
	if adapter.State.TimeSinceLastGuardrailCheck() < 5*time.Second {
		return nil
	}
	adapter.Logger().Debugf("Checking Guardrails")
	adapter.State.UpdateLastGuardrailCheck()

	// Get pod resources to determine memory limit
	ctx := context.Background()

	// This provides continuous failover detection even when ApplyConfig isn't running
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			// Only call HandleFailoverDetected for NEW failovers, not during recovery status checks
			if adapter.State.TimeSinceLastFailover() == 0 {
				// NEW failover detected
				adapter.HandleFailoverDetected(ctx, failoverErr)
			}
			// Return nil signal since guardrails can't continue during failover
			return nil
		}
	}

	// Dynamically discover current primary pod for guardrail checks
	primaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(ctx, adapter.Config.ClusterName)
	if err != nil {
		adapter.Logger().Errorf("Failed to find CNPG primary pod for guardrail: %v", err)
		return nil
	}

	containerClient := adapter.K8sClient.ContainerClient(primaryPod, adapter.Config.ContainerName)

	memoryBytesTotal, err := containerClient.MemoryLimitBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory limit for guardrail: %v", err)
		return nil
	}
	if memoryBytesTotal == 0 {
		adapter.Logger().Errorf("Memory limit bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	memoryBytesUsage, err := containerClient.MemoryUsageBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory usage for guardrail: %v", err)
		return nil
	}

	if memoryBytesUsage == 0 {
		adapter.Logger().Errorf("Memory usage bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	if memoryBytesUsage > memoryBytesTotal {
		adapter.Logger().Errorf(
			"Memory usage of %d exceeds memory limit of %d, likely something is wrong with monitoring",
			memoryBytesUsage,
			memoryBytesTotal,
		)
		// We still continue gaurdrails for safety purposes.
	}

	usedPercentage := float64(memoryBytesUsage) / float64(memoryBytesTotal) * 100

	// Check if memory usage exceeds the threshold
	if usedPercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			usedPercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func Collectors(pool *pgxpool.Pool, kubeClient kubernetes.Client, clusterName string, containerName string, PGVersion string, logger *log.Logger) []agent.MetricCollector {
	// Get PG config for query settings
	pgConfig, _ := pg.ConfigFromViper(nil)

	collectors := []agent.MetricCollector{
		// TODO: Re-enable pg_role collector once backend supports it
		// {
		// 	Key:        "postgresql_role",
		// 	MetricType: "string",
		// 	Collector:  pg.PostgreSQLRole(pool),
		// },
		{
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pool, pgConfig.IncludeQueries, pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pool),
		},
		{
			Key:       "database_connections",
			Collector: pg.Connections(pool),
		},
		{
			Key:       "system_db_size",
			Collector: pg.DatabaseSize(pool),
		},
		{
			Key:       "database_autovacuum_count",
			Collector: pg.Autovacuum(pool),
		},
		{
			Key:       "server_uptime",
			Collector: pg.UptimeMinutes(pool),
		},
		{
			Key:       "pg_database",
			Collector: pg.PGStatDatabase(pool),
		},
		{
			Key:       "pg_user_tables",
			Collector: pg.PGStatUserTables(pool),
		},
		{
			Key:       "pg_bgwriter",
			Collector: pg.PGStatBGwriter(pool),
		},
		{
			Key:       "pg_wal",
			Collector: pg.PGStatWAL(pool),
		},
		{
			Key:       "database_wait_events",
			Collector: pg.WaitEvents(pool),
		},
		{
			Key:       "hardware",
			Collector: kubernetes.CNPGContainerMetricsCollector(kubeClient, clusterName, containerName),
		},
	}

	// Add pg_checkpointer for PostgreSQL 17+
	majorVersion := strings.Split(PGVersion, ".")
	if len(majorVersion) > 0 {
		intMajorVersion, err := strconv.Atoi(majorVersion[0])
		if err != nil {
			logger.Warn("Failed to parse PostgreSQL major version, skipping pg_checkpointer collector", "version", PGVersion, "error", err)
		} else if intMajorVersion >= 17 {
			collectors = append(collectors, agent.MetricCollector{
				Key:       "pg_checkpointer",
				Collector: pg.PGStatCheckpointer(pool),
			})
		}
	}

	return collectors
}

// extractSettingValue converts row.Setting to string for comparison.
// This is necessary because pg.GetActiveConfig() uses `::numeric` cast in queries,
// which causes pgx to return pgtype.Numeric struct instead of primitive types.
// We convert back to string to enable value comparison in GetCurrentConfig().
func extractSettingValue(setting interface{}) string {
	switch v := setting.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case pgtype.Numeric:
		// pgtype.Numeric is returned by pgx when scanning PostgreSQL numeric types
		// Convert to int64 if it's a valid integer
		if v.Valid {
			// Try to convert to int64 first (most PostgreSQL settings are integers)
			if i64Val, err := v.Int64Value(); err == nil && i64Val.Valid {
				return fmt.Sprintf("%d", i64Val.Int64)
			}
			// Fall back to float64 for real numbers
			if f64Val, err := v.Float64Value(); err == nil && f64Val.Valid {
				return fmt.Sprintf("%v", f64Val.Float64)
			}
		}
		// Fallback: use string representation for unexpected numeric formats
		return fmt.Sprintf("%v", v)
	default:
		// For any other type, use fmt.Sprintf as fallback
		return fmt.Sprintf("%v", v)
	}
}

// GetCurrentConfig retrieves the current PostgreSQL configuration values.
// Returns a map of parameter name to current value in CNPG format (e.g., "2GB").
// This is used to compare against proposed config to avoid applying unchanged parameters.
func (adapter *CNPGAdapter) GetCurrentConfig(ctx context.Context) (map[string]string, error) {
	configRows, err := pg.GetActiveConfig(adapter.PGDriver, ctx, adapter.Logger())
	if err != nil {
		return nil, fmt.Errorf("failed to get active config: %w", err)
	}

	config := make(map[string]string)
	for _, rawRow := range configRows {
		row, ok := rawRow.(agent.PGConfigRow)
		if !ok {
			continue
		}

		// Extract the actual value from row.Setting (handles pgtype.Numeric and other types)
		settingStr := extractSettingValue(row.Setting)

		// Convert PostgreSQL format to CNPG format for comparison
		// PostgreSQL returns: setting="262144", unit="8kB"
		// We need: "2GB" (to match what CNPG expects)
		if row.Unit != nil {
			if unitStr, ok := row.Unit.(string); ok && unitStr != "" {
				config[row.Name] = ConvertWithUnit(settingStr, unitStr)
				continue
			}
		}

		// For non-memory parameters (integers, booleans, strings, etc.)
		config[row.Name] = settingStr
	}

	return config, nil
}

// NormalizeValue normalizes parameter values for comparison.
// Handles different representations: "2GB" == "2048MB" == "2097152kB"
func NormalizeValue(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))

	// Try to parse as memory value and convert to bytes
	if bytes := parseMemoryToBytes(normalized); bytes > 0 {
		return fmt.Sprintf("%d", bytes) // Return as bytes string
	}

	// For non-memory values, return normalized string
	return normalized
}

// parseMemoryToBytes parses memory value and returns bytes.
// Handles: "2GB", "256MB", "64kB"
func parseMemoryToBytes(value string) int64 {
	value = strings.TrimSpace(value)

	// Extract number and unit
	var numStr string
	var multiplier int64

	if strings.HasSuffix(value, "gb") {
		numStr = strings.TrimSpace(value[:len(value)-2])
		multiplier = 1024 * 1024 * 1024
	} else if strings.HasSuffix(value, "mb") {
		numStr = strings.TrimSpace(value[:len(value)-2])
		multiplier = 1024 * 1024
	} else if strings.HasSuffix(value, "kb") {
		numStr = strings.TrimSpace(value[:len(value)-2])
		multiplier = 1024
	} else {
		return 0
	}

	// Parse number (handles decimals like "2.5")
	numValue, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0 // Not a valid number
	}

	return int64(numValue * float64(multiplier))
}

// CheckRestartRequired checks if any of the changed parameters require database restart.
// Returns:
//   - bool: true if any parameter requires restart
//   - []string: list of parameter names that require restart
//   - error: any error encountered
func (adapter *CNPGAdapter) CheckRestartRequired(ctx context.Context, changedParams map[string]string) (bool, []string, error) {
	if len(changedParams) == 0 {
		return false, nil, nil
	}

	// Build list of changed parameter names for query
	paramNames := make([]string, 0, len(changedParams))
	for name := range changedParams {
		paramNames = append(paramNames, name)
	}

	// Query which of the changed parameters require restart
	// Uses centralized prefix utility to avoid pg_stat_statements pollution
	query := `SELECT name FROM pg_settings WHERE name = ANY($1) AND context = 'postmaster'`
	rows, err := utils.QueryWithPrefix(adapter.PGDriver, ctx, query, paramNames)
	if err != nil {
		return false, nil, fmt.Errorf("failed to query restart-required parameters: %w", err)
	}
	defer rows.Close()

	// Collect all restart-required parameter names
	restartParams := make([]string, 0)
	logger := adapter.Logger()
	for rows.Next() {
		var paramName string
		rows.Scan(&paramName)
		logger.Infof("Parameter '%s' requires restart (context=postmaster)", paramName)
		restartParams = append(restartParams, paramName)
	}

	return len(restartParams) > 0, restartParams, nil
}
