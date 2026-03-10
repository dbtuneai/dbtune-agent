package runner

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"

	"github.com/sirupsen/logrus"
)

// isRecoveryError checks if an error indicates the system is in recovery/failover
func isRecoveryError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "failover detected") ||
		strings.Contains(errMsg, "recovery in progress")
}

func runWithTicker(ctx context.Context, ticker *time.Ticker, name string, logger *logrus.Logger, skipFirst bool, fn func() error) {
	// Run immediately
	if !skipFirst {
		if err := fn(); err != nil {
			logger.Errorf("initial %s error: %v", name, err)
		}
	}
	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(); err != nil {
				logger.Errorf("%s error: %v", name, err)
			}
		}
	}
}

// Runner is the main entry point for the agent
// that executes the different tasks
func Runner(adapter agent.AgentLooper) {
	logger := adapter.Logger()

	// Create tickers for different intervals
	metricsTicker := time.NewTicker(5 * time.Second)
	systemMetricsTicker := time.NewTicker(1 * time.Minute)
	configTicker := time.NewTicker(5 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)
	guardrailTicker := time.NewTicker(1 * time.Second)
	ddlTicker := time.NewTicker(1 * time.Minute)
	pgStatisticTicker := time.NewTicker(1 * time.Minute)
	pgStatUserTablesTicker := time.NewTicker(1 * time.Minute)
	pgClassTicker := time.NewTicker(1 * time.Minute)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat goroutine
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, true, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, false, func() error {
		data, err := adapter.GetMetrics()
		if err != nil {
			// If error indicates recovery/failover, skip sending error and data
			if isRecoveryError(err) {
				logger.Debugf("Skipping metrics during recovery: %v", err)
				return nil // Return nil to prevent error logging in runWithTicker
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect metrics: " + err.Error(),
				ErrorType:    "metrics_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			if sendErr := adapter.SendError(errorPayload); sendErr != nil {
				logger.Errorf("failed to send error report: %v", sendErr)
			}
			return err
		}
		return adapter.SendMetrics(data)
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, systemMetricsTicker, "system info", logger, false, func() error {
		data, err := adapter.GetSystemInfo()
		if err != nil {
			// If error indicates recovery/failover, skip sending error and data
			if isRecoveryError(err) {
				logger.Debugf("Skipping system info during recovery: %v", err)
				return nil // Return nil to prevent error logging in runWithTicker
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect system information: " + err.Error(),
				ErrorType:    "system_info_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			if sendErr := adapter.SendError(errorPayload); sendErr != nil {
				logger.Errorf("failed to send error report: %v", sendErr)
			}
			return err
		}
		return adapter.SendSystemInfo(data)
	})

	// Config management goroutine
	go runWithTicker(ctx, configTicker, "config", logger, false, func() error {
		config, err := adapter.GetActiveConfig()
		if err != nil {
			// If error indicates recovery/failover, skip sending error and config
			if isRecoveryError(err) {
				logger.Debugf("Skipping config during recovery: %v", err)
				return nil // Return nil to prevent error logging in runWithTicker
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to get active configuration: " + err.Error(),
				ErrorType:    "config_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			if sendErr := adapter.SendError(errorPayload); sendErr != nil {
				logger.Errorf("failed to send error report: %v", sendErr)
			}
			return err
		}

		// Check for proposed configs BEFORE sending current config
		// This is critical after failover recovery - we need to apply baseline first
		proposedConfig, err := adapter.GetProposedConfig()
		if err != nil {
			return err
		}

		if proposedConfig != nil {
			err := adapter.ApplyConfig(proposedConfig)
			if err != nil {
				errorType := "config_apply_error"
				var restartErr *agent.RestartNotAllowedError
				if errors.As(err, &restartErr) {
					errorType = "restart_not_allowed"
				}
				errorPayload := agent.ErrorPayload{
					ErrorMessage: "Failed to apply configuration: " + err.Error(),
					ErrorType:    errorType,
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				if sendErr := adapter.SendError(errorPayload); sendErr != nil {
					logger.Errorf("failed to send error report: %v", sendErr)
				}
				return err
			}

			// Re-fetch config after applying proposed config
			// This ensures we send the newly applied config
			config, err = adapter.GetActiveConfig()
			if err != nil {
				if isRecoveryError(err) {
					logger.Debugf("Skipping config during recovery after apply: %v", err)
					return nil
				}
				return err
			}
		}

		// Now send the config (either original or newly applied)
		if err := adapter.SendActiveConfig(config); err != nil {
			return err
		}

		return nil
	})

	// Guardrail check goroutine
	// Time is kept in a pointer to keep a persistent reference
	// May need to refactor this for testing
	var lastCheck *time.Time
	go runWithTicker(ctx, guardrailTicker, "guardrail", logger, false, func() error {
		if lastCheck != nil && time.Since(*lastCheck) < 15*time.Second {
			return nil
		}
		signal := adapter.Guardrails()
		if signal != nil {
			if err := adapter.SendGuardrailSignal(*signal); err != nil {
				now := time.Now()
				lastCheck = &now
				return err
			}
			now := time.Now()
			lastCheck = &now
		}
		return nil
	})

	// DDL collection goroutine
	var lastDDLHash string
	go runWithTicker(ctx, ddlTicker, "ddl", logger, false, func() error {
		data, err := adapter.GetDDL()
		if err != nil {
			if isRecoveryError(err) {
				logger.Debugf("Skipping DDL during recovery: %v", err)
				return nil
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect DDL: " + err.Error(),
				ErrorType:    "ddl_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		if data.Hash == lastDDLHash {
			return nil
		}
		if err := adapter.SendDDL(data); err != nil {
			return err
		}
		lastDDLHash = data.Hash
		return nil
	})

	// pg_statistic collection goroutine
	go runWithTicker(ctx, pgStatisticTicker, "pg_statistic", logger, false, func() error {
		data, err := adapter.GetPgStatistic()
		if err != nil {
			if isRecoveryError(err) {
				logger.Debugf("Skipping pg_statistic during recovery: %v", err)
				return nil
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect pg_statistic: " + err.Error(),
				ErrorType:    "pg_statistic_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		return adapter.SendPgStatistic(data)
	})

	// pg_stat_user_tables collection goroutine
	go runWithTicker(ctx, pgStatUserTablesTicker, "pg_stat_user_tables", logger, false, func() error {
		data, err := adapter.GetPgStatUserTables()
		if err != nil {
			if isRecoveryError(err) {
				logger.Debugf("Skipping pg_stat_user_tables during recovery: %v", err)
				return nil
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect pg_stat_user_tables: " + err.Error(),
				ErrorType:    "pg_stat_user_tables_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		return adapter.SendPgStatUserTables(data)
	})

	// pg_class collection goroutine
	go runWithTicker(ctx, pgClassTicker, "pg_class", logger, false, func() error {
		data, err := adapter.GetPgClass()
		if err != nil {
			if isRecoveryError(err) {
				logger.Debugf("Skipping pg_class during recovery: %v", err)
				return nil
			}
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect pg_class: " + err.Error(),
				ErrorType:    "pg_class_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		return adapter.SendPgClass(data)
	})

	// pg_stat_activity collection goroutine
	pgStatActivityTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatActivityTicker, "pg_stat_activity", logger, false, func() error {
		data, err := adapter.GetPgStatActivity()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_activity: " + err.Error(), ErrorType: "pg_stat_activity_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatActivity(data)
	})

	// pg_stat_database (raw) collection goroutine
	pgStatDatabaseAllTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatDatabaseAllTicker, "pg_stat_database", logger, false, func() error {
		data, err := adapter.GetPgStatDatabaseAll()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_database: " + err.Error(), ErrorType: "pg_stat_database_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatDatabaseAll(data)
	})

	// pg_stat_database_conflicts collection goroutine
	pgStatDbConflictsTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatDbConflictsTicker, "pg_stat_database_conflicts", logger, false, func() error {
		data, err := adapter.GetPgStatDatabaseConflicts()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_database_conflicts: " + err.Error(), ErrorType: "pg_stat_database_conflicts_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatDatabaseConflicts(data)
	})

	// pg_stat_archiver collection goroutine
	pgStatArchiverTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatArchiverTicker, "pg_stat_archiver", logger, false, func() error {
		data, err := adapter.GetPgStatArchiver()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_archiver: " + err.Error(), ErrorType: "pg_stat_archiver_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatArchiver(data)
	})

	// pg_stat_bgwriter (raw) collection goroutine
	pgStatBgwriterAllTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatBgwriterAllTicker, "pg_stat_bgwriter_all", logger, false, func() error {
		data, err := adapter.GetPgStatBgwriterAll()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_bgwriter: " + err.Error(), ErrorType: "pg_stat_bgwriter_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatBgwriterAll(data)
	})

	// pg_stat_checkpointer (raw) collection goroutine
	pgStatCheckpointerAllTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatCheckpointerAllTicker, "pg_stat_checkpointer_all", logger, false, func() error {
		data, err := adapter.GetPgStatCheckpointerAll()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_checkpointer: " + err.Error(), ErrorType: "pg_stat_checkpointer_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		if data == nil || data.Rows == nil { return nil } // version-gated, may be nil
		return adapter.SendPgStatCheckpointerAll(data)
	})

	// pg_stat_wal (raw) collection goroutine
	pgStatWalAllTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatWalAllTicker, "pg_stat_wal_all", logger, false, func() error {
		data, err := adapter.GetPgStatWalAll()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_wal: " + err.Error(), ErrorType: "pg_stat_wal_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		if data == nil || data.Rows == nil { return nil }
		return adapter.SendPgStatWalAll(data)
	})

	// pg_stat_io collection goroutine
	pgStatIOTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatIOTicker, "pg_stat_io", logger, false, func() error {
		data, err := adapter.GetPgStatIO()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_io: " + err.Error(), ErrorType: "pg_stat_io_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		if data == nil || data.Rows == nil { return nil }
		return adapter.SendPgStatIO(data)
	})

	// pg_stat_replication collection goroutine
	pgStatReplicationTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatReplicationTicker, "pg_stat_replication", logger, false, func() error {
		data, err := adapter.GetPgStatReplication()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_replication: " + err.Error(), ErrorType: "pg_stat_replication_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatReplication(data)
	})

	// pg_stat_replication_slots collection goroutine
	pgStatReplSlotsTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatReplSlotsTicker, "pg_stat_replication_slots", logger, false, func() error {
		data, err := adapter.GetPgStatReplicationSlots()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_replication_slots: " + err.Error(), ErrorType: "pg_stat_replication_slots_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		if data == nil || data.Rows == nil { return nil }
		return adapter.SendPgStatReplicationSlots(data)
	})

	// pg_stat_slru collection goroutine
	pgStatSlruTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatSlruTicker, "pg_stat_slru", logger, false, func() error {
		data, err := adapter.GetPgStatSlru()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_slru: " + err.Error(), ErrorType: "pg_stat_slru_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatSlru(data)
	})

	// pg_stat_user_indexes collection goroutine
	pgStatUserIndexesTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatUserIndexesTicker, "pg_stat_user_indexes", logger, false, func() error {
		data, err := adapter.GetPgStatUserIndexes()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_user_indexes: " + err.Error(), ErrorType: "pg_stat_user_indexes_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatUserIndexes(data)
	})

	// pg_statio_user_tables collection goroutine
	pgStatioUserTablesTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatioUserTablesTicker, "pg_statio_user_tables", logger, false, func() error {
		data, err := adapter.GetPgStatioUserTables()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_statio_user_tables: " + err.Error(), ErrorType: "pg_statio_user_tables_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatioUserTables(data)
	})

	// pg_statio_user_indexes collection goroutine
	pgStatioUserIndexesTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatioUserIndexesTicker, "pg_statio_user_indexes", logger, false, func() error {
		data, err := adapter.GetPgStatioUserIndexes()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_statio_user_indexes: " + err.Error(), ErrorType: "pg_statio_user_indexes_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatioUserIndexes(data)
	})

	// pg_stat_user_functions collection goroutine
	pgStatUserFunctionsTicker := time.NewTicker(1 * time.Minute)
	go runWithTicker(ctx, pgStatUserFunctionsTicker, "pg_stat_user_functions", logger, false, func() error {
		data, err := adapter.GetPgStatUserFunctions()
		if err != nil {
			if isRecoveryError(err) { return nil }
			adapter.SendError(agent.ErrorPayload{ErrorMessage: "Failed to collect pg_stat_user_functions: " + err.Error(), ErrorType: "pg_stat_user_functions_error", Timestamp: time.Now().UTC().Format(time.RFC3339)})
			return err
		}
		return adapter.SendPgStatUserFunctions(data)
	})

	// Block forever
	select {}
}
