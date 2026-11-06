package cnpg

import (
	"context"
	"errors"

	"github.com/dbtuneai/agent/pkg/internal/kubernetes"
	"github.com/dbtuneai/agent/pkg/metrics"
)

// GetMetrics overrides CommonAgent.GetMetrics to check for failover before collecting metrics
func (adapter *CNPGAdapter) GetMetrics(ctx context.Context) ([]metrics.FlatValue, error) {
	logger := adapter.Logger()

	// Check for failover before collecting metrics
	// During recovery, PostgreSQL and primary pod may be unavailable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		var failoverErr *FailoverDetectedError
		if errors.As(err, &failoverErr) {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return FailoverDetectedError - runner will skip sending error and metrics
			return []metrics.FlatValue{}, failoverErr
		}
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetMetrics due to cluster check failure: %v", err)
		return []metrics.FlatValue{}, err
	}

	// Cluster is healthy - proceed with normal metrics collection
	// Note: collectors internally use their own context, but this is the entry point check
	return adapter.CommonAgent.GetMetrics(ctx)
}

func Collectors(kubeClient kubernetes.Client, clusterName string, containerName string) []metrics.Collector {
	return []metrics.Collector{
		{
			Key:       "hardware",
			Collector: kubernetes.CNPGContainerMetricsCollector(kubeClient, clusterName, containerName),
		},
	}
}
