package runner

import (
	"context"
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

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat goroutine
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, true, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, false, func() error {
		data, errs := adapter.GetMetrics()
		if errs != nil && len(errs) > 0 {
			// Format all errors into a single message
			var errMessages []string

			for _, err := range errs {
				// If error indicates recovery/failover, skip sending error and data
				if isRecoveryError(err) {
					logger.Debugf("Skipping metrics during recovery: %v", err)
					return nil // Return nil to prevent error logging in runWithTicker
				}

				errMessages = append(errMessages, err.Error())
			}
			combinedError := strings.Join(errMessages, "; ")

			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect metrics: " + combinedError,
				ErrorType:    "metrics_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return fmt.Errorf("metrics collection failed: %s", combinedError)
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
			adapter.SendError(errorPayload)
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
			adapter.SendError(errorPayload)
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
				errorPayload := agent.ErrorPayload{
					ErrorMessage: "Failed to apply configuration: " + err.Error(),
					ErrorType:    "config_apply_error",
					Timestamp:    time.Now().UTC().Format(time.RFC3339),
				}
				adapter.SendError(errorPayload)
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

	// Block forever
	select {}
}
