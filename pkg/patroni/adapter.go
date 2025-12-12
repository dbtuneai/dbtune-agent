package patroni

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
		HTTPClient:      &http.Client{Timeout: 10 * time.Second},
		State:           &State{},
	}

	adpt.InitCollectors(adpt.Collectors())

	// Initialize primary tracking at startup to establish baseline
	// This prevents false failover detection on first ApplyConfig call
	common.Logger().Info("Initializing primary node tracking...")
	if err := adpt.initializePrimaryTracking(ctx); err != nil {
		common.Logger().Warnf("Failed to initialize primary tracking: %v", err)
		// Don't fail agent startup - tracking will be established on first check
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

func (adapter *PatroniAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	ctx := context.Background()
	logger := adapter.Logger()

	logger.Infof("[FAILOVER_RECOVERY] ApplyConfig called: knob_application=%s, num_params=%d",
		proposedConfig.KnobApplication, len(proposedConfig.Config))

	// Check for failover before applying new configuration
	// This ensures we don't apply tuning parameters after a failover has occurred
	if err := adapter.CheckForFailover(ctx); err != nil {
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Warnf("[FAILOVER_RECOVERY] Failover check BLOCKED config application: %s", failoverErr.Message)
			// Return error to block this config application (matches CNPG behavior)
			// HandleFailoverDetected was already called by CheckForFailover
			return failoverErr
		}
		// Other error checking failover status - log but continue
		logger.Warnf("Failed to check for failover: %v", err)
	}

	logger.Infof("[FAILOVER_RECOVERY] Failover check PASSED - proceeding with config application")

	// Parse and validate all knobs upfront (following CNPG pattern)
	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return fmt.Errorf("failed to parse knob configurations: %w", err)
	}

	if len(parsedKnobs) == 0 {
		logger.Info("No configuration changes to apply")
		return nil
	}

	// Step 1: Clear postgresql.auto.conf by resetting each parameter that will be changed
	// This ensures no local postgresql.auto.conf settings override Patroni DCS settings
	logger.Info("Clearing postgresql.auto.conf by resetting parameters...")
	for _, knob := range parsedKnobs {
		// Skip resetting Patroni-managed parameters
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping reset for Patroni-managed parameter: %s", knob.Name)
			continue
		}
		logger.Infof("Resetting parameter: %s", knob.Name)
		err := pg.AlterSystemReset(adapter.PGDriver, knob.Name)
		if err != nil {
			logger.Errorf("Failed to reset parameter %s: %v", knob.Name, err)
			return fmt.Errorf("failed to reset parameter %s: %w", knob.Name, err)
		}
		logger.Infof("Successfully reset parameter: %s", knob.Name)
	}

	// Reload configuration after reset
	logger.Info("Reloading PostgreSQL configuration after reset...")
	err = pg.ReloadConfig(adapter.PGDriver)
	if err != nil {
		logger.Errorf("Failed to reload configuration: %v", err)
		return fmt.Errorf("failed to reload configuration after reset: %w", err)
	}

	// Step 2: Build parameters map for Patroni REST API
	pgParameters := make(map[string]interface{})
	for _, knob := range parsedKnobs {
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping Patroni-managed parameter: %s (value: %s)", knob.Name, knob.SettingValue)
			continue
		}
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

	// Create HTTP PATCH request
	req, err := http.NewRequest("PATCH", configURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Errorf("Error creating request: %v", err)
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set Content-Type header
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
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

	// Step 4: Wait for PostgreSQL if restart is needed
	if proposedConfig.KnobApplication == "restart" {
		logger.Warn("Configuration requires restart, waiting for PostgreSQL to be ready...")
		err = pg.WaitPostgresReady(adapter.PGDriver)
		if err != nil {
			// Check if error indicates failover during restart
			if isPostgreSQLFailoverError(err) {
				logger.Warnf("PostgreSQL failover detected during restart wait: %v", err)

				// Send failover notification if this is first detection
				if adapter.State.TimeSinceLastFailover() == 0 {
					adapter.State.SetLastFailoverTime(time.Now())
					errorPayload := agent.ErrorPayload{
						ErrorMessage: fmt.Sprintf("failover detected: PostgreSQL restart interrupted by failover: %v", err),
						ErrorType:    "failover_detected",
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					}
					adapter.SendError(errorPayload)
					logger.Info("Failover notification sent to backend (during restart wait)")
				}

				return &FailoverDetectedError{
					OldPrimary: adapter.State.GetLastKnownPrimary(),
					NewPrimary: NoPrimaryDetected,
					Message:    fmt.Sprintf("PostgreSQL restart interrupted by failover: %v", err),
				}
			}
			return fmt.Errorf("failed to wait for PostgreSQL to be back online: %w", err)
		}
		logger.Info("PostgreSQL is ready after restart")
	}

	// Step 5: Verify that all parameters were applied correctly by reading pg_settings
	logger.Info("Waiting for Patroni to apply configuration (15s delay)...")
	time.Sleep(15 * time.Second)
	logger.Info("Verifying applied configuration in pg_settings...")
	for _, knob := range parsedKnobs {
		// Skip verification for Patroni-managed parameters
		if IsPatroniManagedParameter(knob.Name) {
			logger.Warnf("Skipping verification for Patroni-managed parameter: %s", knob.Name)
			continue
		}

		query := `SELECT setting, unit FROM pg_settings WHERE name = $1`
		var actualSetting string
		var unit *string
		err := utils.QueryRowWithPrefix(adapter.PGDriver, ctx, query, knob.Name).Scan(&actualSetting, &unit)
		if err != nil {
			// Check if error indicates PostgreSQL failover (before Patroni detects it)
			if isPostgreSQLFailoverError(err) {
				logger.Warnf("PostgreSQL failover detected during verification: %v", err)

				// Send failover notification if this is first detection
				if adapter.State.TimeSinceLastFailover() == 0 {
					adapter.State.SetLastFailoverTime(time.Now())
					errorPayload := agent.ErrorPayload{
						ErrorMessage: fmt.Sprintf("failover detected: PostgreSQL shutting down: %v", err),
						ErrorType:    "failover_detected",
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					}
					adapter.SendError(errorPayload)
					logger.Info("Failover notification sent to backend (during verification)")
				}

				return &FailoverDetectedError{
					OldPrimary: adapter.State.GetLastKnownPrimary(),
					NewPrimary: NoPrimaryDetected,
					Message:    fmt.Sprintf("PostgreSQL shutting down: %v", err),
				}
			}
			logger.Errorf("Failed to verify parameter %s: %v", knob.Name, err)
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
		} else if normalizeValue(actualValue) == normalizeValue(expectedValue) {
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

// PatroniPatchRequest represents the structure for PATCH request to Patroni's /config endpoint
type PatroniPatchRequest struct {
	PostgreSQL struct {
		Parameters map[string]interface{} `json:"parameters"`
	} `json:"postgresql"`
}

// normalizeValue normalizes configuration values for comparison
// Converts values to bytes for memory/size units to enable proper comparison
// e.g., "16MB" and "16384kB" both normalize to the same value in kB
func normalizeValue(value string) string {
	// Remove whitespace
	value = strings.TrimSpace(value)
	value = strings.ToUpper(value)

	// Try to parse memory/size units and convert to kB for comparison
	// This handles: kB, MB, GB, TB
	if strings.HasSuffix(value, "TB") {
		numStr := strings.TrimSuffix(value, "TB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024*1024)
		}
	}
	if strings.HasSuffix(value, "GB") {
		numStr := strings.TrimSuffix(value, "GB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024*1024)
		}
	}
	if strings.HasSuffix(value, "MB") {
		numStr := strings.TrimSuffix(value, "MB")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fKB", num*1024)
		}
	}
	if strings.HasSuffix(value, "KB") {
		// Already in kB, just return uppercase version
		return value
	}

	// Try time units: ms, s, min, h, d
	if strings.HasSuffix(value, "D") {
		numStr := strings.TrimSuffix(value, "D")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*24*60*60*1000)
		}
	}
	if strings.HasSuffix(value, "H") {
		numStr := strings.TrimSuffix(value, "H")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*60*1000)
		}
	}
	if strings.HasSuffix(value, "MIN") {
		numStr := strings.TrimSuffix(value, "MIN")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*60*1000)
		}
	}
	if strings.HasSuffix(value, "S") && !strings.HasSuffix(value, "MS") {
		numStr := strings.TrimSuffix(value, "S")
		if num, err := strconv.ParseFloat(numStr, 64); err == nil {
			return fmt.Sprintf("%.0fMS", num*1000)
		}
	}
	if strings.HasSuffix(value, "MS") {
		// Already in ms
		return value
	}

	// Return as-is for values without units or unrecognized units
	return value
}

func (adapter *PatroniAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	logger := adapter.Logger()
	ctx := context.Background()

	// Check for failover before querying PostgreSQL
	// During recovery, PostgreSQL may be unavailable or unreliable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return the failover error (not nil) so runner's isRecoveryError() can skip sending
			// This prevents sending empty config to backend which would stop tuning session
			return nil, failoverErr
		}
		// Non-failover error (cluster status check failed, etc.)
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetActiveConfig due to cluster check failure: %v", err)
		return nil, err
	}

	// CRITICAL: The database itself is the only reliable source of truth for the
	// currently active configuration. The Patroni DCS can have stale data or not
	// reflect local overrides in postgresql.auto.conf, so we must query PostgreSQL directly.
	logger.Info("Fetching active configuration from PostgreSQL")
	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx, logger)
	if err != nil {
		// Check if error indicates PostgreSQL failover (before Patroni detects it)
		// This can happen before Patroni cluster status reflects the failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL reports failover in progress: %v", err)

			// Trigger failover detection immediately
			// Don't wait for Patroni cluster status to update (can take 5+ seconds)
			if adapter.State.TimeSinceLastFailover() == 0 {
				// First detection - record failover time and send notification
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error (before Patroni status update)")

				// Send failover notification (don't call HandleFailoverDetected as it would update LastKnownPrimary)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}

			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			// This prevents sending empty config to backend which would stop tuning session
			logger.Infof("[FAILOVER_RECOVERY] Returning failover error to prevent empty data being sent")
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
		}
		// For other errors, return as-is
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
				adapter.Logger().Debugf("Filtering out Patroni-managed parameter from active config: %s", row.Name)
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
	ctx := context.Background()

	logger.Println("Collecting system info for Patroni cluster")

	// Check for failover before collecting system info
	// During recovery, PostgreSQL may be unavailable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		if failoverErr, ok := err.(*FailoverDetectedError); ok {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return the failover error (not nil) so runner's isRecoveryError() can skip sending
			// This prevents sending empty system info to backend which would stop tuning session
			return nil, failoverErr
		}
		// Non-failover error (cluster status check failed, etc.)
		// Still block to avoid sending errors during unstable cluster state
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetSystemInfo due to cluster check failure: %v", err)
		return nil, err
	}

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		// Check if error indicates PostgreSQL failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected in GetSystemInfo: %v", err)
			// Record failover and send notification if this is first detection
			if adapter.State.TimeSinceLastFailover() == 0 {
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error in GetSystemInfo")

				// Send failover notification (don't call HandleFailoverDetected)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}
			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
		}
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		// Check if error indicates PostgreSQL failover
		if isPostgreSQLFailoverError(err) {
			logger.Warnf("[FAILOVER_RECOVERY] PostgreSQL failover detected in GetSystemInfo: %v", err)
			// Record failover and send notification if this is first detection
			if adapter.State.TimeSinceLastFailover() == 0 {
				adapter.State.SetLastFailoverTime(time.Now())
				logger.Errorf("[FAILOVER_RECOVERY] Failover detected via PostgreSQL error in GetSystemInfo")

				// Send failover notification (don't call HandleFailoverDetected)
				errorPayload := agent.ErrorPayload{
					ErrorMessage: fmt.Sprintf("Failover detected: %s", err.Error()),
					ErrorType:    "failover_detected",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
				logger.Info("Failover notification sent to backend")
			}
			// Return FailoverDetectedError so runner's isRecoveryError() can skip sending
			return nil, &FailoverDetectedError{
				OldPrimary: adapter.State.GetLastKnownPrimary(),
				NewPrimary: NoPrimaryDetected,
				Message:    fmt.Sprintf("PostgreSQL error: %v", err),
			}
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
