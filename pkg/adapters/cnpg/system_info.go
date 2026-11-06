package cnpg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
)

func (adapter *CNPGAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	logger := adapter.Logger()
	var flatValues []metrics.FlatValue

	// Check for failover before collecting system info
	// During recovery, PostgreSQL and primary pod may be unavailable
	if err := adapter.CheckForFailover(ctx); err != nil {
		// Block operation for ANY error from CheckForFailover
		var failoverErr *FailoverDetectedError
		if errors.As(err, &failoverErr) {
			logger.Infof("[FAILOVER_RECOVERY] Operations blocked during recovery: %s", failoverErr.Message)
			// Return FailoverDetectedError - runner will skip sending error and system info
			return []metrics.FlatValue{}, failoverErr
		}
		// Non-failover error (cluster status check failed, pod not found, etc.)
		// Still block to avoid sending errors during unstable cluster state
		logger.Warnf("[FAILOVER_RECOVERY] BLOCKING GetSystemInfo due to cluster check failure: %v", err)
		return []metrics.FlatValue{}, err
	}

	// Get operations context AFTER CheckForFailover passes
	// This ensures we get the fresh context created by recovery completion, not the pre-cancelled one
	dbCtx := adapter.State.GetOperationsContext()

	// Get PostgreSQL version (queried each cycle so rolling minor upgrades show up)
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.failoverErrorFromQuery(dbCtx, err); failoverErr != nil {
			return []metrics.FlatValue{}, failoverErr
		}
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	pgVersionMetric, err := metrics.PGVersion.AsFlatValue(pgVersion.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create PG version metric: %w", err)
	}
	flatValues = append(flatValues, pgVersionMetric)

	// Get max_connections from PostgreSQL
	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.failoverErrorFromQuery(dbCtx, err); failoverErr != nil {
			return []metrics.FlatValue{}, failoverErr
		}
		return nil, fmt.Errorf("failed to get max_connections: %w", err)
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to create max_connections metric: %w", err)
	}
	flatValues = append(flatValues, maxConnectionsMetric)

	// Current database and user-database count
	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		return nil, fmt.Errorf("failed to create database system info metrics: %w", err)
	}
	flatValues = append(flatValues, databaseInfo...)

	// Dynamically discover current primary pod for system info
	primaryPod, err := adapter.K8sClient.FindCNPGPrimaryPod(dbCtx, adapter.Config.ClusterName)
	if err != nil {
		// Check if context was cancelled (failover during operation)
		if dbCtx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetSystemInfo aborted due to context cancellation")
			return []metrics.FlatValue{}, nil
		}

		// Check if error indicates failover (no primary pod during transition)
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no primary pod") || strings.Contains(errStr, "not found") {
			logger.Warnf("[FAILOVER_RECOVERY] Primary pod not found in GetSystemInfo (likely failover): %v", err)
			// Return empty to suppress error
			return []metrics.FlatValue{}, nil
		}

		return nil, fmt.Errorf("failed to find CNPG primary pod: %w", err)
	}

	containerClient := adapter.K8sClient.ContainerClient(primaryPod, adapter.Config.ContainerName)
	systemInfo, err := containerClient.GetContainerSystemInfo(dbCtx)
	if err != nil {
		// Check if context was cancelled (failover during operation)
		if dbCtx.Err() == context.Canceled {
			logger.Infof("[FAILOVER_RECOVERY] GetSystemInfo aborted due to context cancellation")
			return []metrics.FlatValue{}, nil
		}
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}
	flatValues = append(flatValues, systemInfo...)

	return flatValues, nil
}
