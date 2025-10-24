package runner

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"

	"github.com/sirupsen/logrus"
)

func runWithTicker(ctx context.Context, ticker *time.Ticker, name string, logger *logrus.Logger, fn func() error) {
	// Run immediately
	if err := fn(); err != nil {
		logger.Errorf("initial %s error: %v", name, err)
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
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, func() error {
		data, err := adapter.GetMetrics()
		if err != nil {
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to collect metrics: " + err.Error(),
				ErrorType:    "metrics_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		return adapter.SendMetrics(data)
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, systemMetricsTicker, "system info", logger, func() error {
		data, err := adapter.GetSystemInfo()
		if err != nil {
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
	go runWithTicker(ctx, configTicker, "config", logger, func() error {
		config, err := adapter.GetActiveConfig()
		if err != nil {
			errorPayload := agent.ErrorPayload{
				ErrorMessage: "Failed to get active configuration: " + err.Error(),
				ErrorType:    "config_error",
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
			}
			adapter.SendError(errorPayload)
			return err
		}
		if err := adapter.SendActiveConfig(config); err != nil {
			return err
		}

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
		}
		return nil
	})

	// Guardrail check goroutine
	// Time is kept in a pointer to keep a persistent reference
	// May need to refactor this for testing
	var lastCheck *time.Time
	go runWithTicker(ctx, guardrailTicker, "guardrail", logger, func() error {
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
