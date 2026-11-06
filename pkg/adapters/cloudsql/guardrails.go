package cloudsql

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/guardrails"
)

func (adapter *CloudSQLAdapter) Guardrails(_ context.Context) *guardrails.Signal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Debugf("Checking Guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	memoryStats, err := GetMemoryMetrics(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory metric for guardrail: %v", err)
		return nil
	}

	if memoryStats.UsedPercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			memoryStats.UsedPercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}
