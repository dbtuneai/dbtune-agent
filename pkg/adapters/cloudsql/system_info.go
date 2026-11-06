package cloudsql

import (
	"context"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
)

func (adapter *CloudSQLAdapter) GetSystemInfo(_ context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Debugf("Getting System Info")

	// Get PostgreSQL version and max connections from database
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion.String())
	if err != nil {
		adapter.Logger().Errorf("Error creating PostgreSQL version metric: %v", err)
		return nil, err
	}

	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		adapter.Logger().Errorf("Error creating max connections metric: %v", err)
		return nil, err
	}

	cpuCount, err := GetCPUCount(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("Error fetching CPU count: %v", err)
		return nil, err
	}

	cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(int64(cpuCount.GetDoubleValue()))
	if err != nil {
		adapter.Logger().Errorf("Error creating NodeCPUCount metric: %v", err)
		return nil, err
	}

	memoryTotal, err := GetMemoryTotal(adapter.CloudMonitoringClient, adapter.CloudSQLConfig.ProjectID, adapter.CloudSQLConfig.DatabaseName)
	if err != nil {
		adapter.Logger().Errorf("failed to get memory total: %v", err)
		return nil, err
	}

	memoryTotalMetric, err := metrics.NodeMemoryTotal.AsFlatValue(memoryTotal.GetInt64Value())
	if err != nil {
		adapter.Logger().Errorf("Error creating NodeMemoryTotal metric: %v", err)
		return nil, err
	}

	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Errorf("Error creating database system info metrics: %v", err)
		return nil, err
	}

	systemInfo := make([]metrics.FlatValue, 0, 4+len(databaseInfo))
	systemInfo = append(systemInfo,
		version,
		maxConnectionsMetric,
		cpuCountMetric,
		memoryTotalMetric,
	)
	systemInfo = append(systemInfo, databaseInfo...)

	adapter.Logger().Debugf("SystemMetrics: %v", systemInfo)

	return systemInfo, nil
}
