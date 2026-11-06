package aiven

import (
	"context"
	"time"

	"github.com/aiven/go-client-codegen/handler/service"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
)

// Guardrails implements resource usage guardrails
// Aiven only provides 30 second resolution data for hardware info, which we
// need for guardrails.
func (adapter *AivenPostgreSQLAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
	timeSinceLastGuardrailCheck := time.Since(adapter.State.LastGuardrailCheck)
	if timeSinceLastGuardrailCheck < adapter.Config.MetricResolution {
		adapter.Logger().Debugf(
			"Guardrails checked %s ago, lower than the resolution of %s, skipping",
			timeSinceLastGuardrailCheck,
			adapter.Config.MetricResolution,
		)
		return nil
	}

	// NOTE: We use the latest obtained during HardwareInfo() if it's recent, otherwise,
	// re-fetch the metrics.
	var lastMemoryAvailablePercentage float64

	timeSinceLastMemoryAvailable := time.Since(adapter.State.LastMemoryAvailableTime)
	if timeSinceLastMemoryAvailable < adapter.Config.MetricResolution {
		adapter.Logger().Debugf(
			"Memory check %s ago, lower than the resolution of %s, using cached value",
			timeSinceLastMemoryAvailable,
			adapter.Config.MetricResolution,
		)
		lastMemoryAvailablePercentage = adapter.State.LastMemoryAvailablePercentage
	} else {
		adapter.Logger().Debugf(
			"Memory check %s ago, higher than the resolution of %s, fetching new value",
			timeSinceLastMemoryAvailable,
			adapter.Config.MetricResolution,
		)
		metrics, err := GetFetchedMetrics(
			ctx,
			FetchedMetricsIn{
				Client:      &adapter.Client,
				ProjectName: adapter.Config.ProjectName,
				ServiceName: adapter.Config.ServiceName,
				Logger:      adapter.Logger(),
				Period:      service.PeriodTypeHour,
			},
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get fetched metric for guardrail: %v", err)
			return nil
		}
		memAvailableMetric := metrics[MEM_AVAILABLE_KEY]
		if memAvailableMetric.Value != nil {
			if perc, ok := memAvailableMetric.Value.(float64); ok {
				lastMemoryAvailablePercentage = perc
				adapter.State.LastMemoryAvailableTime = memAvailableMetric.Timestamp
			} else {
				adapter.Logger().Warnf("memAvailableMetric.Value is not a float64: %v", memAvailableMetric.Value)
				return nil
			}
		} else {
			adapter.Logger().Warn("memAvailableMetric.Value is nil")
			return nil
		}
	}

	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")
	adapter.State.LastGuardrailCheck = time.Now()

	memoryAvailablePercentage := lastMemoryAvailablePercentage

	if memoryAvailablePercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			memoryAvailablePercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}
