package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"

	"github.com/sirupsen/logrus"
)

// isRecoveryError checks if an error indicates the system is in recovery/failover
func isRecoveryError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, pg.ErrDatabaseDown) {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "failover detected") ||
		strings.Contains(errMsg, "recovery in progress")
}

func runWithTicker(ctx context.Context, interval time.Duration, name string, logger *logrus.Logger, skipFirst bool, fn func(ctx context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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

// Runner is the main entry point for the agent
// that executes the different tasks
func Runner(ctx context.Context, adapter agent.AgentLooper) {
	logger := adapter.Logger()

	// Heartbeat goroutine
	go runWithTicker(ctx, 15*time.Second, "heartbeat", logger, true, func(ctx context.Context) error {
		return adapter.SendHeartbeat(ctx)
	})

	// Metrics collection goroutine
	go runWithTicker(ctx, 5*time.Second, "metrics", logger, false, func(ctx context.Context) error {
		data, err := adapter.GetMetrics(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "metrics", err)
		}
		return adapter.SendMetrics(ctx, data)
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, 1*time.Minute, "system info", logger, false, func(ctx context.Context) error {
		data, err := adapter.GetSystemInfo(ctx)
		if err != nil {
			return handleGetError(ctx, adapter, logger, "system_info", err)
		}
		return adapter.SendSystemInfo(ctx, data)
	})

	// Config management goroutine
	go runWithTicker(ctx, 5*time.Second, "config", logger, false, func(ctx context.Context) error {
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
	go runWithTicker(ctx, 1*time.Second, "guardrail", logger, false, func(ctx context.Context) error {
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

	// Start the health gate (if the adapter supports it) so catalog collectors
	// short-circuit when the database is unreachable.
	type healthGater interface {
		StartHealthGate(ctx context.Context)
	}
	if hg, ok := adapter.(healthGater); ok {
		hg.StartHealthGate(ctx)
	}

	// Catalog view collection goroutines — all follow the same get→send pattern.
	// Stagger start times so their tickers are permanently offset, spreading
	// load evenly across the interval instead of all firing at once.
	collectors := adapter.CatalogCollectors()
	const stagger = 1 * time.Second
	for i, c := range collectors {
		c := c
		delay := time.Duration(i) * stagger
		var lastHash string
		go func() {
			if delay > 0 {
				logger.Debugf("staggering %s by %s", c.Name, delay)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
			}
			runWithTicker(ctx, c.Interval, c.Name, logger, false, func(ctx context.Context) error {
				data, err := c.Collect(ctx)
				if err != nil {
					return handleGetError(ctx, adapter, logger, c.Name, err)
				}
				if data == nil {
					return nil
				}
				if c.SkipUnchanged {
					hash, err := metrics.HashJSON(data)
					if err != nil {
						logger.Warnf("%s: hash failed, sending anyway: %v", c.Name, err)
					} else if hash == lastHash {
						logger.Debugf("%s: unchanged, skipping send", c.Name)
						return nil
					}
					if err := adapter.SendCatalogPayload(ctx, c.Name, data); err != nil {
						return err
					}
					if hash != "" {
						lastHash = hash
					}
					return nil
				}
				return adapter.SendCatalogPayload(ctx, c.Name, data)
			})
		}()
	}

	// Block forever
	select {}
}
