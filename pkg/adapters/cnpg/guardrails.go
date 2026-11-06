package cnpg

import (
	"context"
	"errors"
	"time"

	"github.com/dbtuneai/agent/pkg/guardrails"
)

func (adapter *CNPGAdapter) Guardrails(ctx context.Context) *guardrails.Signal {
	// Check if enough time has passed since the last guardrail check
	if adapter.State.TimeSinceLastGuardrailCheck() < 5*time.Second {
		return nil
	}
	adapter.Logger().Debugf("Checking Guardrails")
	adapter.State.UpdateLastGuardrailCheck()

	// This provides continuous failover detection even when ApplyConfig isn't running
	if err := adapter.CheckForFailover(ctx); err != nil {
		var failoverErr *FailoverDetectedError
		if errors.As(err, &failoverErr) {
			// Only call HandleFailoverDetected for NEW failovers, not during recovery status checks
			if adapter.State.TimeSinceLastFailover() == 0 {
				// NEW failover detected
				adapter.HandleFailoverDetected(ctx, failoverErr) //nolint:errcheck // always returns the failoverErr as a signal, already logged internally
			}
			// Return nil signal since guardrails can't continue during failover
			return nil
		}
	}

	// Dynamically discover current primary pod for guardrail checks
	primaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(ctx, adapter.Config.ClusterName)
	if err != nil {
		adapter.Logger().Errorf("Failed to find CNPG primary pod for guardrail: %v", err)
		return nil
	}

	containerClient := adapter.K8sClient.ContainerClient(primaryPod, adapter.Config.ContainerName)

	memoryBytesTotal, err := containerClient.MemoryLimitBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory limit for guardrail: %v", err)
		return nil
	}
	if memoryBytesTotal == 0 {
		adapter.Logger().Errorf("Memory limit bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	memoryBytesUsage, err := containerClient.MemoryUsageBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory usage for guardrail: %v", err)
		return nil
	}

	if memoryBytesUsage == 0 {
		adapter.Logger().Errorf("Memory usage bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	if memoryBytesUsage > memoryBytesTotal {
		adapter.Logger().Errorf(
			"Memory usage of %d exceeds memory limit of %d, likely something is wrong with monitoring",
			memoryBytesUsage,
			memoryBytesTotal,
		)
		// We still continue gaurdrails for safety purposes.
	}

	usedPercentage := float64(memoryBytesUsage) / float64(memoryBytesTotal) * 100

	// Check if memory usage exceeds the threshold
	if usedPercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			usedPercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}
