package cnpg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/backend"
	"github.com/dbtuneai/agent/pkg/internal/kubernetes"
	"github.com/dbtuneai/agent/pkg/tuning"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (adapter *CNPGAdapter) ApplyConfig(ctx context.Context, proposedConfig *tuning.ProposedConfigResponse) tuning.ApplyConfigError {
	// https://cloudnative-pg.io/documentation/1.27/postgresql_conf/#changing-configuration
	logger := adapter.Logger()

	logger.Infof("[FAILOVER_RECOVERY] ApplyConfig called: knob_application=%s, num_params=%d",
		proposedConfig.KnobApplication, len(proposedConfig.Config))

	// Check for failover before applying new configuration
	// This ensures we don't apply tuning parameters after a failover has occurred
	if err := adapter.CheckForFailover(ctx); err != nil {
		var failoverErr *FailoverDetectedError
		if errors.As(err, &failoverErr) {
			logger.Warnf("[FAILOVER_RECOVERY] Failover check BLOCKED config application: %s", failoverErr.Message)
			// Return error to block this config application
			return &tuning.ConfigApplyError{Err: failoverErr}
		}
		// Other error checking failover status - log but continue
		logger.Warnf("Failed to check for failover: %v", err)
	}

	logger.Infof("[FAILOVER_RECOVERY] Failover check PASSED - proceeding with config application")

	// Get cluster name from pod's CNPG cluster label
	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to get cluster name: %w", err)}
	}
	logger.Infof("Applying configuration to CNPG cluster: %s (knob_application: %s)", clusterName, proposedConfig.KnobApplication)

	// Parse and validate all knobs upfront
	parsedKnobs, err := tuning.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to parse knob configurations: %w", err)}
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
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to get current config for comparison: %w", err)}
	}
	logger.Infof("Successfully retrieved %d current parameter values", len(currentConfig))

	// Build parameters map for the patch
	// CNPG requires memory parameters in human-readable format (e.g., "2GB" not "262144")
	parametersMap := make(map[string]string)
	skippedUnchanged := 0
	for _, knob := range parsedKnobs {
		// Skip CNPG-managed parameters that must not be modified by tuning.
		// CRITICAL: these can arrive set to values (e.g. shared_preload_libraries="")
		// that would break the cluster by removing pg_stat_statements.
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
			if tuning.NormalizeValue(currentValue) == tuning.NormalizeValue(cnpgValue) {
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

	// CNPG auto-restarts whenever a postmaster-context parameter changes in
	// spec.postgresql.parameters. There is no way to ask it for a reload-only
	// apply, so we must look up which of the changed params would force a
	// restart before patching, and refuse to partially apply.
	paramNames := make([]string, 0, len(parametersMap))
	for name := range parametersMap {
		paramNames = append(paramNames, name)
	}
	requiresRestart, err := tuning.ValidateRestartPolicy(adapter.PGDriver, ctx, paramNames, proposedConfig.KnobApplication)
	if err != nil {
		return &tuning.ConfigApplyError{Err: err}
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
		return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to apply configuration patch: %w", err)}
	}

	if requiresRestart {
		// Trigger rolling restart for restart-required parameters
		err = kubernetes.TriggerCNPGRollingRestart(ctx, adapter.K8sClient, clusterName, logger)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to trigger rolling restart: %w", err)}
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
			return &tuning.ConfigApplyError{Err: fmt.Errorf("cluster did not become healthy after rolling restart: %w", err)}
		}

		// Re-discover the current primary pod (may have changed during restart)
		newPrimaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(ctx, clusterName)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to find primary pod after restart: %w", err)}
		}
		if newPrimaryPod != adapter.Config.PodName {
			logger.Infof("Primary pod changed from %s to %s after rolling restart", adapter.Config.PodName, newPrimaryPod)
			adapter.Config.PodName = newPrimaryPod
		}

		// Wait for PostgreSQL to accept connections
		logger.Info("Waiting for PostgreSQL to accept connections...")
		err = tuning.WaitPostgresReady(adapter.PGDriver, ctx)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("PostgreSQL did not become ready after restart: %w", err)}
		}
		logger.Info("PostgreSQL is accepting connections - configuration applied")
	} else {
		// For reload-only parameters, CNPG automatically reloads config
		logger.Info("Configuration patch applied (reload-only parameters, no restart needed)")

		// Wait for PostgreSQL to accept connections and verify health
		logger.Info("Waiting for PostgreSQL to accept connections after reload...")
		err = tuning.WaitPostgresReady(adapter.PGDriver, ctx)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("PostgreSQL did not become ready after config reload: %w", err)}
		}

		// Verify pod is still ready
		podClient := adapter.K8sClient.PodClient(adapter.Config.PodName)
		ready, err := podClient.IsPodReady(ctx)
		if err != nil {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("failed to verify pod status after config reload: %w", err)}
		}
		if !ready {
			return &tuning.ConfigApplyError{Err: fmt.Errorf("pod became not ready after config reload")}
		}
		logger.Info("Configuration reload completed successfully - PostgreSQL is healthy")
	}

	// Update state to track when we last applied config
	adapter.State.UpdateLastAppliedConfig()

	return nil
}

func (adapter *CNPGAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error) {
	logger := adapter.Logger()

	// Check for failover before querying PostgreSQL
	// During recovery, PostgreSQL may be unavailable or unreliable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		var failoverErr *FailoverDetectedError
		if errors.As(err, &failoverErr) {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			return tuning.ConfigArraySchema{}, failoverErr
		}
		// Non-failover error (cluster status check failed, pod not found, etc.)
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetActiveConfig due to cluster check failure: %v", err)
		return tuning.ConfigArraySchema{}, err
	}

	// Get operations context AFTER CheckForFailover passes
	// This ensures we get the fresh context created by recovery completion, not the pre-cancelled one
	dbCtx := adapter.State.GetOperationsContext()

	config, err := tuning.GetActiveConfig(adapter.PGDriver, dbCtx)
	if err != nil {
		// If context was cancelled (failover detected mid-query), return FailoverDetectedError
		if dbCtx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetActiveConfig aborted due to context cancellation")
			return tuning.ConfigArraySchema{}, &FailoverDetectedError{
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
				errorPayload := backend.ErrorPayload{
					ErrorMessage: "Failover detected: " + err.Error(),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				if sendErr := adapter.SendError(ctx, errorPayload); sendErr != nil {
					logger.Errorf("failed to send error report: %v", sendErr)
				}
			}

			// Return FailoverDetectedError - runner will skip sending error and config
			logger.Infof("[FAILOVER_RECOVERY] Returning FailoverDetectedError to prevent config_error")
			return tuning.ConfigArraySchema{}, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: "(PostgreSQL shutting down)",
				Message:    err.Error(),
			}
		}

		// For other errors, return as-is
		return tuning.ConfigArraySchema{}, err
	}

	return config, nil
}

// extractSettingValue converts row.Setting to string for comparison.
func extractSettingValue(setting interface{}) string {
	switch v := setting.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GetCurrentConfig retrieves the current PostgreSQL configuration values.
// Returns a map of parameter name to current value in CNPG format (e.g., "2GB").
// This is used to compare against proposed config to avoid applying unchanged parameters.
func (adapter *CNPGAdapter) GetCurrentConfig(ctx context.Context) (map[string]string, error) {
	configRows, err := tuning.GetActiveConfig(adapter.PGDriver, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active config: %w", err)
	}

	config := make(map[string]string)
	for _, rawRow := range configRows {
		row, ok := rawRow.(tuning.PGConfigRow)
		if !ok {
			continue
		}

		// Extract the actual value from row.Setting
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
