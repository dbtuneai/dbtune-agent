package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg"

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

// withHealthGate wraps a function with health gate check and error reporting.
// If the gate is closed (database unreachable), fn is skipped. If fn returns
// an error, the gate inspects it for connection-level failures. Recovery errors
// are suppressed (returned as nil) to avoid noisy logging in runWithTicker.
func withHealthGate(hg *agent.HealthGate, logger *logrus.Logger, fn func() error) error {
	if hg.IsClosed() {
		logger.Debugf("Skipping, health gate closed")
		return nil
	}
	err := fn()
	if err != nil {
		hg.ReportError(err)
		if isRecoveryError(err) {
			logger.Debugf("Skipping during recovery: %v", err)
			return nil
		}
	}
	return err
}

// Runner is the main entry point for the agent
// that executes the different tasks
func Runner(adapter agent.AgentLooper) {
	logger := adapter.Logger()

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a HealthGate to short-circuit DB-hitting calls when the database is unreachable.
	hg := agent.NewHealthGate(ctx, adapter.Pool(), pg.IsConnectionError, logger)

	// Create tickers for different intervals
	metricsTicker := time.NewTicker(5 * time.Second)
	systemMetricsTicker := time.NewTicker(1 * time.Minute)
	configTicker := time.NewTicker(5 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)
	guardrailTicker := time.NewTicker(1 * time.Second)

	// Heartbeat goroutine
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, true, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			data, err := adapter.GetMetrics()
			if err != nil {
				// Send partial data even when some collectors failed.
				if len(data) > 0 {
					if sendErr := adapter.SendMetrics(data); sendErr != nil {
						logger.Errorf("failed to send partial metrics: %v", sendErr)
					}
				}
				return fmt.Errorf("failed to collect metrics: %w", err)
			}
			return adapter.SendMetrics(data)
		})
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, systemMetricsTicker, "system info", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			data, err := adapter.GetSystemInfo()
			if err != nil {
				return fmt.Errorf("failed to collect system information: %w", err)
			}
			return adapter.SendSystemInfo(data)
		})
	})

	// Config management goroutine
	go runWithTicker(ctx, configTicker, "config", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			config, err := adapter.GetActiveConfig()
			if err != nil {
				return fmt.Errorf("failed to get active configuration: %w", err)
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
					return err
				}
			}

			return adapter.SendActiveConfig(config)
		})
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

	// Block forever
	select {}
}
