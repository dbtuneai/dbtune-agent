package azureflex

import (
	"context"

	"github.com/dbtuneai/agent/pkg/guardrails"
)

func (adapter *AzureFlexAdapter) Guardrails(_ context.Context) *guardrails.Signal {
	memoryUsagePercent, err := MemoryPercent(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName)()
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory metric for guardrail: %v", err)
		return nil
	}

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than 90% (default), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}
