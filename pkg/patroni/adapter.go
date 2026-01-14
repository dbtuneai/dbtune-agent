package patroni

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

type PatroniAdapter struct {
	agent.CommonAgent
	PatroniConfig   Config
	PGDriver        *pgxpool.Pool
	GuardrailConfig guardrails.Config
	pgConfig        pg.Config
	PGVersion       string
	HTTPClient      *http.Client
	State           *State
}

func CreatePatroniAdapter() (*PatroniAdapter, error) {
	ctx := context.Background()

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	patroniConfig, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	pgPool, err := pgxpool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	pgVersion, err := pg.PGVersion(pgPool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	guardrailConfig, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails: %w", err)
	}

	common := agent.CreateCommonAgent()
	adpt := PatroniAdapter{
		CommonAgent:     *common,
		PatroniConfig:   patroniConfig,
		PGDriver:        pgPool,
		GuardrailConfig: guardrailConfig,
		pgConfig:        pgConfig,
		PGVersion:       pgVersion,
		HTTPClient:      &http.Client{Timeout: 60 * time.Second}, // Increased for restart operations
		State:           &State{},
	}

	adpt.InitCollectors(adpt.Collectors())

	// Initialize operations context for PostgreSQL queries
	// This context will be cancelled during failover to abort in-flight operations
	adpt.State.CreateOperationsContext()
	common.Logger().Info("Initialized operations context for PostgreSQL queries")

	// Initialize primary tracking at startup to establish baseline
	// This prevents false failover detection on first ApplyConfig call
	common.Logger().Info("Initializing primary node tracking...")
	if err := adpt.initializePrimaryTracking(ctx); err != nil {
		common.Logger().Warnf("Failed to initialize primary tracking: %v", err)
		// Don't fail agent startup - tracking will be established on first check
	}
	isStandby, err := adpt.isStandbyNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if node is standby: %w", err)
	}
	if isStandby {
		common.Logger().Warn("WARNING: Agent is running on a Standby node. Tuning commands will be rejected until this node becomes Primary.")
	}

	return &adpt, nil

}

// initializePrimaryTracking establishes the initial primary node tracking
// This should be called once during adapter initialization
func (adapter *PatroniAdapter) initializePrimaryTracking(ctx context.Context) error {
	logger := adapter.Logger()

	clusterStatus, err := adapter.getPatroniClusterStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get initial cluster status: %w", err)
	}

	if clusterStatus.CurrentPrimary != "" {
		adapter.State.SetLastKnownPrimary(clusterStatus.CurrentPrimary)
		logger.Infof("Initial primary node tracked: %s", clusterStatus.CurrentPrimary)
	} else {
		logger.Warn("No primary node detected during initialization")
	}

	return nil
}

const PGInRecoveryQuery = "SELECT pg_is_in_recovery()"

// isRestartAlreadyInProgress checks if the error indicates a restart is already in progress
func isRestartAlreadyInProgress(err error) bool {
	return strings.Contains(err.Error(), "restart already in progress") ||
		strings.Contains(err.Error(), "reinitialize already in progress")
}

// checkFailoverBeforeOperation checks for failover before starting an operation.
// Returns an error if failover is detected or cluster check fails.
func (adapter *PatroniAdapter) checkFailoverBeforeOperation(ctx context.Context, operation string) error {
	logger := adapter.Logger()
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			return failoverErr
		}
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING %s due to cluster check failure: %v", operation, err)
		return err
	}
	return nil
}

// handlePossibleFailoverError checks if an error indicates a PostgreSQL failover,
// records it if first detection, sends notification, and returns a FailoverDetectedError.
// Returns nil if the error is not a failover error.
func (adapter *PatroniAdapter) handlePossibleFailoverError(err error, context string) *FailoverDetectedError {
	if !isPostgreSQLFailoverError(err) {
		return nil
	}

	logger := adapter.Logger()
	logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected %s: %v", context, err)

	if adapter.State.TimeSinceLastFailover() == 0 {
		adapter.State.SetLastFailoverTime(time.Now())
		logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error %s", context)

		errorPayload := agent.ErrorPayload{
			ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
			ErrorType:    "failover_detected",
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
		}
		adapter.SendError(errorPayload)
		logger.Info("Failover notification sent to backend")
	}

	return &FailoverDetectedError{
		OldPrimary: adapter.State.GetLastKnownPrimary(),
		NewPrimary: NoPrimaryDetected,
		Message:    fmt.Sprintf("PostgreSQL error: %v", err),
	}
}

// Recognizes if the current node is a standby (replica) node
func (adapter *PatroniAdapter) isStandbyNode(ctx context.Context) (bool, error) {
	var inRecovery bool
	err := utils.QueryRowWithPrefix(adapter.PGDriver, ctx, PGInRecoveryQuery).Scan(&inRecovery)
	if err != nil {
		return false, fmt.Errorf("failed to check recovery status: %w", err)
	}
	return inRecovery, nil
}

func (adapter *PatroniAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// Use operations context that will be cancelled during failover
	// This ensures queries during config application are aborted if failover occurs
	ctx := adapter.State.GetOperationsContext()
	logger := adapter.Logger()

	logger.Infof("[FAILOVER_RECOVERY] ApplyConfig called: knob_application=%s, num_params=%d",
		proposedConfig.KnobApplication, len(proposedConfig.Config))

	// Check if we're already in a grace period from a previous failover
	// This needs to be checked BEFORE CheckForFailover to avoid race conditions
	timeSinceFailover := adapter.State.TimeSinceLastFailover()
	recentFailoverGracePeriod := 5 * time.Minute
	hadRecentFailover := timeSinceFailover > 0 && timeSinceFailover < recentFailoverGracePeriod

	// Check for failover before applying new configuration
	// This ensures we don't apply tuning parameters after a failover has occurred
	failoverCheckErr := adapter.CheckForFailover(ctx)
	if failoverCheckErr != nil {
		if failoverErr, ok := failoverCheckErr.(*FailoverDetectedError); ok {
			// If we're in stabilization period, allow the config application to proceed
			// This is critical for applying baseline configuration after failover
			if failoverErr.InStabilization {
				logger.Infof("[FAILOVER_RECOVERY] In stabilization period - allowing config application to proceed (baseline revert)")
				logger.Infof("[FAILOVER_RECOVERY] Skipping standby check during stabilization to allow baseline application")
				// Skip standby check and continue with config application
			} else if hadRecentFailover {
				// We're in grace period from a PREVIOUS failover, but a NEW failover just occurred
				// Allow this config to proceed (it's likely baseline from the previous failover)
				// The new failover will trigger its own baseline later
				logger.Warnf("[FAILOVER_RECOVERY] New failover detected during grace period from previous failover")
				logger.Infof("[FAILOVER_RECOVERY] Allowing config application to proceed (baseline from previous failover)")
				logger.Infof("[FAILOVER_RECOVERY] New failover will be handled in next iteration")
				// Skip standby check and continue with config application
			} else {
				// Not in stabilization or grace period - block config application
				logger.Warnf("[FAILOVER_RECOVERY] Failover check BLOCKED config application: %s", failoverErr.Message)
				// HandleFailoverDetected was already called by CheckForFailover
				return failoverErr
			}
		} else {
			// Other error checking failover status - log but continue
			logger.Warnf("Failed to check for failover: %v", failoverCheckErr)
		}
	}

	// Re-check failover status after CheckForFailover (may have been updated by new failover)
	timeSinceFailover = adapter.State.TimeSinceLastFailover()
	hadRecentFailover = timeSinceFailover > 0 && timeSinceFailover < recentFailoverGracePeriod

	// Clear failover time if grace period has expired
	if timeSinceFailover > 0 && timeSinceFailover >= recentFailoverGracePeriod {
		logger.Infof("[FAILOVER_RECOVERY] Grace period expired (%.0fs since failover) - clearing failover tracking and resuming normal standby checks",
			timeSinceFailover.Seconds())
		adapter.State.ClearFailoverTime()
		hadRecentFailover = false // Now we can perform standby check
	}

	// Only perform standby check if:
	// 1. No failover detected currently (failoverCheckErr == nil)
	// 2. No recent failover in the grace period
	if failoverCheckErr == nil && !hadRecentFailover {
		// Normal operation - check if we're on a standby node
		// This prevents tuning sessions from starting on standby nodes
		isStandby, err := adapter.isStandbyNode(ctx)
		if err == nil && isStandby {
			errMsg := "Tuning operation rejected: Agent is running on a Standby node."
			logger.Error(errMsg)

			errorPayload := agent.ErrorPayload{
				ErrorMessage: errMsg,
				ErrorType:    "standby_node_detected",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return fmt.Errorf("%s", errMsg)
		} else if err != nil {
			logger.Warnf("Failed to check if node is standby: %v", err)
		}
	} else if hadRecentFailover {
		logger.Infof("[FAILOVER_RECOVERY] Skipping standby check due to recent failover (%.0fs ago, grace period: %.0fs)",
			timeSinceFailover.Seconds(), recentFailoverGracePeriod.Seconds())
	}

	logger.Infof("[FAILOVER_RECOVERY] Failover check PASSED - proceeding with config application")

	// Parse and validate all knobs upfront (following CNPG pattern)
	allKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return fmt.Errorf("failed to parse knob configurations: %w", err)
	}

	// Filter out Patroni-managed parameters upfront
	var parsedKnobs []parameters.ParsedKnob
	for _, knob := range allKnobs {
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping Patroni-managed parameter: %s (value: %s)", knob.Name, knob.SettingValue)
			continue
		}
		parsedKnobs = append(parsedKnobs, knob)
	}

	if len(parsedKnobs) == 0 {
		logger.Info("No configuration changes to apply")
		return nil
	}

	// Step 1: Clear postgresql.auto.conf by resetting each parameter that will be changed
	// This ensures no local postgresql.auto.conf settings override Patroni DCS settings
	logger.Info("Clearing postgresql.auto.conf by resetting parameters...")
	for _, knob := range parsedKnobs {
		logger.Infof("Resetting parameter: %s", knob.Name)
		if err := pg.AlterSystemReset(adapter.PGDriver, knob.Name); err != nil {
			logger.Errorf("Failed to reset parameter %s: %v", knob.Name, err)
			return fmt.Errorf("failed to reset parameter %s: %w", knob.Name, err)
		}
	}

	// Reload configuration after reset
	logger.Info("Reloading PostgreSQL configuration after reset...")
	if err := pg.ReloadConfig(adapter.PGDriver); err != nil {
		logger.Errorf("Failed to reload configuration: %v", err)
		return fmt.Errorf("failed to reload configuration after reset: %w", err)
	}

	// Step 2: Build parameters map for Patroni REST API
	pgParameters := make(map[string]any)
	for _, knob := range parsedKnobs {
		logger.Infof("Will set %s = %s", knob.Name, knob.SettingValue)
		pgParameters[knob.Name] = knob.SettingValue
	}

	// Build the JSON payload in Patroni's expected format
	patchRequest := PatroniPatchRequest{}
	patchRequest.PostgreSQL.Parameters = pgParameters

	// Convert to JSON
	jsonData, err := json.Marshal(patchRequest)
	if err != nil {
		logger.Errorf("Error marshaling JSON: %v", err)
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	logger.Debugf("Patroni PATCH payload: %s", string(jsonData))

	// Step 3: Apply configuration via Patroni REST API
	configURL := fmt.Sprintf("%s/config", adapter.PatroniConfig.PatroniAPIURL)

	// Create HTTP PATCH request with context
	// This allows the request to be cancelled if failover occurs
	req, err := http.NewRequestWithContext(ctx, "PATCH", configURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Errorf("Error creating request: %v", err)
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set Content-Type header
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		logger.Errorf("Error sending request: %v", err)
		return fmt.Errorf("failed to send HTTP PATCH request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("Error reading response: %v", err)
		return fmt.Errorf("failed to read response body: %w", err)
	}

	logger.Infof("Patroni API Response Status: %s", resp.Status)
	logger.Debugf("Patroni API Response Body: %s", string(responseBody))

	// Check if the request was successful (2xx status codes)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("Patroni API returned error status: %d, body: %s", resp.StatusCode, string(responseBody))
		return fmt.Errorf("patroni API returned error status %d: %s", resp.StatusCode, string(responseBody))
	}

	logger.Info("Configuration successfully applied via Patroni REST API")

	// Step 4: Check if any parameters require PostgreSQL restart and handle restart if needed
	// Respect backend's KnobApplication setting:
	// - "restart" = Allow restarts
	// - "reload" = NO restarts (reload only, even if params require restart)
	// - other = Check dynamically if restart is needed

	needsRestart := false

	switch proposedConfig.KnobApplication {
	case "restart":
		// Explicitly flagged as restart by backend
		needsRestart = true
		logger.Info("Backend requested restart (KnobApplication='restart')")
	case "reload":
		// Backend explicitly disabled restarts - respect it
		needsRestart = false
		logger.Info("Backend disabled restarts (KnobApplication='reload') - will only reload config")
	default:
		// NOTE(eddie): Unlikely
		// Not explicitly specified - check dynamically using pg_settings
		paramNames := make([]string, len(parsedKnobs))
		for i, knob := range parsedKnobs {
			paramNames[i] = knob.Name
		}

		logger.Infof("Checking if any of %d parameters require restart: %v", len(paramNames), paramNames)

		// Check if any parameter requires restart (context='postmaster')
		restartRequired, err := adapter.requiresPostgreSQLRestart(ctx, paramNames)
		if err != nil {
			logger.Errorf("Failed to check for restart-required parameters: %v", err)
			// Continue without restart check if query fails
		} else if restartRequired {
			needsRestart = true
			logger.Info("Detected parameters that require PostgreSQL restart (context='postmaster')")
		} else {
			logger.Info("No restart-required parameters detected")
		}
	}

	// If restart is needed, trigger PostgreSQL restart via Patroni API
	if needsRestart {
		logger.Info("Configuration requires restart, triggering PostgreSQL restart via Patroni API...")

		// Mark that we're entering an intentional restart window
		// This prevents CheckForFailover from sending failover notifications during planned restart
		adapter.State.SetInRestartWindow(true)
		defer adapter.State.SetInRestartWindow(false) // Clear flag when done (success or failure)

		// Trigger the restart
		err = adapter.triggerPostgreSQLRestart(ctx)
		if err != nil {
			return fmt.Errorf("failed to trigger PostgreSQL restart: %w", err)
		}

		// Wait for PostgreSQL to come back online
		logger.Info("Waiting for PostgreSQL to be ready after restart...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			if failoverErr := adapter.handlePossibleFailoverError(err, "during restart wait"); failoverErr != nil {
				return failoverErr
			}
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
		logger.Info("PostgreSQL is ready after restart")

		// Create a new operations context after successful restart
		// The old context may have been used during restart and needs to be refreshed
		adapter.State.CreateOperationsContext()
		ctx = adapter.State.GetOperationsContext()
		logger.Info("Created new operations context after PostgreSQL restart")
	}

	// Step 5: Verify that all parameters were applied correctly by reading pg_settings
	logger.Info("Waiting for Patroni to apply configuration (15s delay)...")
	time.Sleep(15 * time.Second)
	logger.Info("Verifying applied configuration in pg_settings...")
	for _, knob := range parsedKnobs {
		query := `SELECT setting, unit FROM pg_settings WHERE name = $1`
		var actualSetting string
		var unit *string
		err := utils.QueryRowWithPrefix(adapter.PGDriver, ctx, query, knob.Name).Scan(&actualSetting, &unit)
		if err != nil {
			if failoverErr := adapter.handlePossibleFailoverError(err, "during verification"); failoverErr != nil {
				return failoverErr
			}
			return fmt.Errorf("failed to verify parameter %s in pg_settings: %w", knob.Name, err)
		}

		// Reconstruct the actual value with unit if present
		actualValue := actualSetting
		if unit != nil && *unit != "" {
			actualValue = actualSetting + *unit
		}

		expectedValue := knob.SettingValue

		// Try to match expected value with actual value
		// This handles both exact matches and unit variations
		if actualValue == expectedValue || actualSetting == expectedValue {
			logger.Infof("✓ Verified parameter %s = %s", knob.Name, actualValue)
		} else if parameters.NormalizeValue(actualValue) == parameters.NormalizeValue(expectedValue) {
			// Values match after normalization (e.g., "16384kB" vs "16MB")
			logger.Infof("✓ Verified parameter %s = %s (matches expected %s)",
				knob.Name, actualValue, expectedValue)
		} else {
			// True mismatch - this is a critical error
			logger.Errorf("Parameter verification FAILED for %s: expected '%s', but got '%s' from pg_settings",
				knob.Name, expectedValue, actualValue)
			return fmt.Errorf("configuration verification failed: parameter %s has value '%s' but expected '%s'",
				knob.Name, actualValue, expectedValue)
		}
	}

	logger.Info("Configuration application and verification complete")

	// Final failover check after applying configuration
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			// Failover detected after config application
			// CheckForFailover already sent notification and updated LastKnownPrimary
			return failoverErr
		}
		logger.Warnf("Post-application failover check encountered error: %v", err)
	}

	return nil
}

// requiresPostgreSQLRestart checks if any of the parameters being applied require a PostgreSQL restart
// by querying pg_settings to see if context='postmaster' for those parameters
func (adapter *PatroniAdapter) requiresPostgreSQLRestart(ctx context.Context, parameterNames []string) (bool, error) {
	if len(parameterNames) == 0 {
		return false, nil
	}

	// Query pg_settings to check if any parameter has context='postmaster'
	// context='postmaster' means the parameter requires a server restart
	query := `SELECT 1 FROM pg_settings WHERE name = ANY($1) AND context = 'postmaster' LIMIT 1`

	rows, err := utils.QueryWithPrefix(adapter.PGDriver, ctx, query, parameterNames)
	if err != nil {
		return false, fmt.Errorf("failed to query pg_settings for restart-required parameters: %w", err)
	}
	defer rows.Close()

	hasRows := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("error iterating pg_settings results: %w", err)
	}
	return hasRows, nil
}

// triggerPostgreSQLRestart triggers a PostgreSQL restart for all cluster nodes
// This ensures the entire cluster is restarted when restart-required parameters are applied
func (adapter *PatroniAdapter) triggerPostgreSQLRestart(ctx context.Context) error {
	logger := adapter.Logger()

	logger.Info("Triggering PostgreSQL restart for all cluster nodes...")

	// Get full cluster information including all members
	clusterURL := fmt.Sprintf("%s/cluster", adapter.PatroniConfig.PatroniAPIURL)
	req, err := http.NewRequestWithContext(ctx, "GET", clusterURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create cluster info request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster API returned non-200 status: %s", resp.Status)
	}

	var clusterInfo PatroniClusterResponse
	if err := json.NewDecoder(resp.Body).Decode(&clusterInfo); err != nil {
		return fmt.Errorf("failed to decode cluster info: %w", err)
	}

	logger.Infof("Found %d nodes in cluster to restart", len(clusterInfo.Members))

	// Strategy: Restart replicas first, then primary last
	// This minimizes downtime and avoids unnecessary failover
	var replicas []PatroniMember
	var leader *PatroniMember

	for i, member := range clusterInfo.Members {
		if member.Role == "leader" || member.Role == "master" {
			leader = &clusterInfo.Members[i]
			logger.Infof("Found leader: %s (host: %s)", member.Name, member.Host)
		} else {
			replicas = append(replicas, member)
			logger.Infof("Found replica: %s (host: %s, role: %s)", member.Name, member.Host, member.Role)
		}
	}

	// Restart replicas first (safe, no downtime)
	// Wait between restarts to avoid "restart already in progress" errors
	for i, replica := range replicas {
		logger.Infof("Restarting replica %d/%d: %s", i+1, len(replicas), replica.Name)
		if err := adapter.restartSingleNode(ctx, replica); err != nil {
			if isRestartAlreadyInProgress(err) {
				logger.Infof("Restart already in progress for replica %s (Patroni handling it)", replica.Name)
			} else {
				logger.Errorf("Failed to restart replica %s: %v", replica.Name, err)
			}
		} else {
			logger.Infof("Successfully initiated restart for replica: %s", replica.Name)
		}

		// Wait 3 seconds between replica restarts to avoid conflicts
		if i < len(replicas)-1 {
			logger.Debugf("Waiting 3s before restarting next replica...")
			time.Sleep(3 * time.Second)
		}
	}

	// Wait before restarting leader to ensure replicas are being handled
	if len(replicas) > 0 && leader != nil {
		logger.Info("Waiting 5s for replica restarts to stabilize before restarting leader...")
		time.Sleep(5 * time.Second)
	}

	// Restart leader last (this may cause brief connection interruption)
	if leader != nil {
		logger.Infof("Restarting leader: %s", leader.Name)
		if err := adapter.restartSingleNode(ctx, *leader); err != nil {
			if isRestartAlreadyInProgress(err) {
				logger.Infof("Restart already in progress for leader %s (Patroni handling it)", leader.Name)
			} else {
				return fmt.Errorf("failed to restart leader: %w", err)
			}
		} else {
			logger.Infof("Successfully initiated restart for leader: %s", leader.Name)
		}
	}

	logger.Info("PostgreSQL restart initiated for all cluster nodes")
	return nil
}

// restartSingleNode restarts a single node via its Patroni API
func (adapter *PatroniAdapter) restartSingleNode(ctx context.Context, member PatroniMember) error {
	logger := adapter.Logger()

	// Build the restart URL for this specific member
	// Extract port from configured PatroniAPIURL to use the same port on all nodes
	parsedURL, err := url.Parse(adapter.PatroniConfig.PatroniAPIURL)
	if err != nil {
		return fmt.Errorf("failed to parse PatroniAPIURL: %w", err)
	}

	port := parsedURL.Port()
	if port == "" {
		port = strconv.Itoa(DEFAULT_API_PORT)
	}

	memberRestartURL := fmt.Sprintf("%s://%s:%s/restart", parsedURL.Scheme, member.Host, port)

	logger.Debugf("Sending POST to Patroni restart endpoint: %s for node %s", memberRestartURL, member.Name)

	req, err := http.NewRequestWithContext(ctx, "POST", memberRestartURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create restart request: %w", err)
	}

	resp, err := adapter.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send restart request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read restart response: %w", err)
	}

	logger.Debugf("Restart API response for %s: status=%s, body=%s", member.Name, resp.Status, string(responseBody))

	// Patroni returns 202 Accepted for successful restart initiation
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("patroni restart API returned error status %d: %s", resp.StatusCode, string(responseBody))
	}

	return nil
}

// PatroniPatchRequest represents the structure for PATCH request to Patroni's /config endpoint
type PatroniPatchRequest struct {
	PostgreSQL struct {
		Parameters map[string]any `json:"parameters"`
	} `json:"postgresql"`
}

func (adapter *PatroniAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	logger := adapter.Logger()
	ctx := adapter.State.GetOperationsContext()

	if err := adapter.checkFailoverBeforeOperation(ctx, "GetActiveConfig"); err != nil {
		return nil, err
	}

	// CRITICAL: The database itself is the only reliable source of truth for the
	// currently active configuration. The Patroni DCS can have stale data or not
	// reflect local overrides in postgresql.auto.conf, so we must query PostgreSQL directly.
	logger.Info("Fetching active configuration from PostgreSQL")
	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx, logger)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(err, "in GetActiveConfig"); failoverErr != nil {
			logger.Infof("[FAILOVER_RECOVERY] Returning failover error to prevent empty data being sent")
			return nil, failoverErr
		}
		return nil, err
	}

	// Filter out Patroni-managed parameters to prevent false "unexpected config change" alerts
	// These parameters are managed by Patroni (e.g., during failover) and should not trigger
	// backend notifications about configuration drift
	filteredConfig := make(agent.ConfigArraySchema, 0, len(config))
	for _, param := range config {
		// Type assert to PGConfigRow to access the Name field
		if row, ok := param.(agent.PGConfigRow); ok {
			if !IsPatroniManagedParameter(row.Name) {
				filteredConfig = append(filteredConfig, param)
			} else {
				logger.Debugf("Filtering out Patroni-managed parameter from active config: %s", row.Name)
			}
		} else {
			// If type assertion fails, include the parameter to be safe
			filteredConfig = append(filteredConfig, param)
		}
	}

	return filteredConfig, nil
}

func (adapter *PatroniAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	logger := adapter.Logger()
	ctx := adapter.State.GetOperationsContext()

	logger.Println("Collecting system info for Patroni cluster")

	if err := adapter.checkFailoverBeforeOperation(ctx, "GetSystemInfo"); err != nil {
		return nil, err
	}

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(err, "in GetSystemInfo (PGVersion)"); failoverErr != nil {
			return nil, failoverErr
		}
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(err, "in GetSystemInfo (MaxConnections)"); failoverErr != nil {
			return nil, failoverErr
		}
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

	systemInfo := []metrics.FlatValue{
		version,
		totalMemory,
		hostOS,
		platformVersion,
		platform,
		maxConnectionsMetric,
		noCPUsMetric,
	}

	return systemInfo, nil
}

func (adapter *PatroniAdapter) Guardrails() *guardrails.Signal {
	// Get memory info
	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		adapter.Logger().Error("Failed to get memory info:", err)
		return nil
	}

	// Calculate memory usage percentage
	memoryUsagePercent := float64(memoryInfo.Total-memoryInfo.Available) / float64(memoryInfo.Total) * 100

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than threshold (default 90%), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func (adapter *PatroniAdapter) Collectors() []agent.MetricCollector {
	pool := adapter.PGDriver
	collectors := []agent.MetricCollector{
		{
			Key:       "database_average_query_runtime",
			Collector: pg.PGStatStatements(pool, adapter.pgConfig.IncludeQueries, adapter.pgConfig.MaximumQueryTextLength),
		},
		{
			Key:       "database_transactions_per_second",
			Collector: pg.TransactionsPerSecond(pool),
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
			Collector: HardwareInfoPatroni(),
		},
	}

	majorVersion := strings.Split(adapter.PGVersion, ".")
	intMajorVersion, err := strconv.Atoi(majorVersion[0])
	if err != nil {
		adapter.Logger().Warnf("Could not parse major version from version string %s: %v", adapter.PGVersion, err)
		return collectors
	}
	if intMajorVersion >= 17 {
		collectors = append(collectors, agent.MetricCollector{
			Key:       "pg_checkpointer",
			Collector: pg.PGStatCheckpointer(pool),
		})
	}

	return collectors
}
