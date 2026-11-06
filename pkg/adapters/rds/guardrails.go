package rds

import (
	"context"
	"time"

	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
)

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *RDSAdapter) Guardrails(_ context.Context) *guardrails.Signal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Info("Checking guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	totalMemoryBytes, err := adapter.State.DBInfo.TotalMemoryBytes()
	if err != nil {
		adapter.Logger().Errorf("Failed to get total memory bytes: %v", err)
		return nil
	}

	if adapter.State.DBInfo.PerformanceInsightsEnabled() {
		resourceID, err := adapter.State.DBInfo.ResourceID()
		if err != nil {
			adapter.Logger().Errorf("Failed to get resource ID: %v", err)
			return nil
		}

		memoryUsageBytes, err := GetMemoryUsageFromPI(
			&adapter.AWSClients,
			resourceID,
			adapter.Logger(),
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from PI: %v", err)
			return nil
		}

		memoryUsagePercent := (float64(memoryUsageBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Memory usage: %.2f%%", memoryUsagePercent)
		if memoryUsagePercent > adapter.GuardrailSettings.MemoryThreshold {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.Memory,
			}
		}
	} else {
		freeableMemoryBytes, err := GetFreeableMemoryFromCW(
			adapter.Config.RDSDatabaseIdentifier,
			&adapter.AWSClients,
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from CloudWatch: %v", err)
			return nil
		}
		freeableMemoryPercent := (float64(freeableMemoryBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Freeable memory: %.2f%%", freeableMemoryPercent)
		if freeableMemoryPercent < (100 - adapter.GuardrailSettings.MemoryThreshold) {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.FreeableMemory,
			}
		}
	}

	return nil
}
