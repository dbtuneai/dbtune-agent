package runner

import (
	"context"
	"errors"
	"fmt"
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

func runWithTicker(ctx context.Context, ticker *time.Ticker, name string, logger *logrus.Logger, skipFirst bool, fn func(ctx context.Context) error) {
	// Run immediately
	if !skipFirst {
		if err := fn(ctx); err != nil {
			logger.Errorf("initial %s error: %v", name, err)
		}
	}
	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				logger.Errorf("%s error: %v", name, err)
			}
		}
	}
}

// handleGetError handles errors from catalog collector get operations.
// Returns nil if the error is a recovery error (suppresses it),
// otherwise sends an error report and returns the error.
func handleGetError(ctx context.Context, adapter agent.AgentLooper, logger *logrus.Logger, name string, err error) error {
	if isRecoveryError(err) {
		logger.Debugf("Skipping %s during recovery: %v", name, err)
		return nil
	}
	adapter.SendError(ctx, agent.ErrorPayload{
		ErrorMessage: fmt.Sprintf("Failed to collect %s: %s", name, err.Error()),
		ErrorType:    name + "_error",
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	})
	return err
}

// catalogTask defines a periodic get-then-send task for a catalog view.
type catalogTask struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) error
}

// Runner is the main entry point for the agent
// that executes the different tasks
func Runner(ctx context.Context, adapter agent.AgentLooper) {
	logger := adapter.Logger()

	// Heartbeat goroutine
	go runWithTicker(ctx, time.NewTicker(15*time.Second), "heartbeat", logger, true, func(ctx context.Context) error {
		return adapter.SendHeartbeat(ctx)
	})

	// Metrics collection goroutine
	go runWithTicker(ctx, time.NewTicker(5*time.Second), "metrics", logger, false, func(ctx context.Context) error {
		data, err := adapter.GetMetrics(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "metrics", err)
		}
		return adapter.SendMetrics(ctx, data)
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, time.NewTicker(1*time.Minute), "system info", logger, false, func(ctx context.Context) error {
		data, err := adapter.GetSystemInfo(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "system_info", err)
		}
		return adapter.SendSystemInfo(ctx, data)
	})

	// Config management goroutine
	go runWithTicker(ctx, time.NewTicker(5*time.Second), "config", logger, false, func(ctx context.Context) error {
		config, err := adapter.GetActiveConfig(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "config", err)
		}

		// Check for proposed configs BEFORE sending current config
		// This is critical after failover recovery - we need to apply baseline first
		proposedConfig, err := adapter.GetProposedConfig(ctx)
		if err != nil {
			return err
		}

		if proposedConfig != nil {
			err := adapter.ApplyConfig(ctx, proposedConfig)
			if err != nil {
				errorType := "config_apply_error"
				var restartErr *agent.RestartNotAllowedError
				if errors.As(err, &restartErr) {
					errorType = "restart_not_allowed"
				}
				if sendErr := adapter.SendError(ctx, agent.ErrorPayload{
					ErrorMessage: "Failed to apply configuration: " + err.Error(),
					ErrorType:    errorType,
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}); sendErr != nil {
					logger.Errorf("failed to send error report: %v", sendErr)
				}
				return err
			}

			// Re-fetch config after applying proposed config
			// This ensures we send the newly applied config
			config, err = adapter.GetActiveConfig(ctx)
			if err != nil {
				if isRecoveryError(err) {
					logger.Debugf("Skipping config during recovery after apply: %v", err)
					return nil
				}
				return err
			}
		}

		return adapter.SendActiveConfig(ctx, config)
	})

	// Guardrail check goroutine
	var lastCheck *time.Time
	go runWithTicker(ctx, time.NewTicker(1*time.Second), "guardrail", logger, false, func(ctx context.Context) error {
		if lastCheck != nil && time.Since(*lastCheck) < 15*time.Second {
			return nil
		}
		signal := adapter.Guardrails(ctx)
		if signal != nil {
			if err := adapter.SendGuardrailSignal(ctx, *signal); err != nil {
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
	go runWithTicker(ctx, time.NewTicker(1*time.Minute), "ddl", logger, false, func(ctx context.Context) error {
		data, err := adapter.GetDDL(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "ddl", err)
		}
		if data.Hash == lastDDLHash {
			return nil
		}
		if err := adapter.SendDDL(ctx, data); err != nil {
			return err
		}
		lastDDLHash = data.Hash
		return nil
	})

	// Catalog view collection goroutines — all follow the same get→send pattern.
	catalogTasks := []catalogTask{
		{"pg_statistic", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatistic(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_statistic", err) }
			return adapter.SendPgStatistic(ctx, data)
		}},
		{"pg_stat_user_tables", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatUserTables(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_user_tables", err) }
			return adapter.SendPgStatUserTables(ctx, data)
		}},
		{"pg_class", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgClass(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_class", err) }
			return adapter.SendPgClass(ctx, data)
		}},
		{"pg_stat_activity", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatActivity(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_activity", err) }
			return adapter.SendPgStatActivity(ctx, data)
		}},
		{"pg_stat_database", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatDatabaseAll(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_database", err) }
			return adapter.SendPgStatDatabaseAll(ctx, data)
		}},
		{"pg_stat_database_conflicts", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatDatabaseConflicts(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_database_conflicts", err) }
			return adapter.SendPgStatDatabaseConflicts(ctx, data)
		}},
		{"pg_stat_archiver", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatArchiver(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_archiver", err) }
			return adapter.SendPgStatArchiver(ctx, data)
		}},
		{"pg_stat_bgwriter", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatBgwriterAll(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_bgwriter", err) }
			return adapter.SendPgStatBgwriterAll(ctx, data)
		}},
		{"pg_stat_checkpointer", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatCheckpointerAll(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_checkpointer", err) }
			if data == nil || data.Rows == nil { return nil }
			return adapter.SendPgStatCheckpointerAll(ctx, data)
		}},
		{"pg_stat_wal", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatWalAll(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_wal", err) }
			if data == nil || data.Rows == nil { return nil }
			return adapter.SendPgStatWalAll(ctx, data)
		}},
		{"pg_stat_io", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatIO(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_io", err) }
			if data == nil || data.Rows == nil { return nil }
			return adapter.SendPgStatIO(ctx, data)
		}},
		{"pg_stat_replication", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatReplication(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_replication", err) }
			return adapter.SendPgStatReplication(ctx, data)
		}},
		{"pg_stat_replication_slots", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatReplicationSlots(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_replication_slots", err) }
			if data == nil || data.Rows == nil { return nil }
			return adapter.SendPgStatReplicationSlots(ctx, data)
		}},
		{"pg_stat_slru", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatSlru(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_slru", err) }
			return adapter.SendPgStatSlru(ctx, data)
		}},
		{"pg_stat_user_indexes", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatUserIndexes(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_user_indexes", err) }
			return adapter.SendPgStatUserIndexes(ctx, data)
		}},
		{"pg_statio_user_tables", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatioUserTables(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_statio_user_tables", err) }
			return adapter.SendPgStatioUserTables(ctx, data)
		}},
		{"pg_statio_user_indexes", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatioUserIndexes(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_statio_user_indexes", err) }
			return adapter.SendPgStatioUserIndexes(ctx, data)
		}},
		{"pg_stat_user_functions", 1 * time.Minute, func(ctx context.Context) error {
			data, err := adapter.GetPgStatUserFunctions(ctx)
			if err != nil { return handleGetError(ctx, adapter, logger, "pg_stat_user_functions", err) }
			return adapter.SendPgStatUserFunctions(ctx, data)
		}},
	}

	for _, task := range catalogTasks {
		go runWithTicker(ctx, time.NewTicker(task.interval), task.name, logger, false, task.fn)
	}

	// Block forever
	select {}
}
