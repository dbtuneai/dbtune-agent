package aiven

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
)

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Info("Collecting Aiven system info")

	// Get service information from Aiven API
	service, err := adapter.Client.ServiceGet(
		ctx,
		adapter.Config.ProjectName,
		adapter.Config.ServiceName,
		[2]string{"include_secrets", "false"},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get service info: %w", err)
	}

	numCPUs := *service.NodeCpuCount
	nodeMemoryMb := *service.NodeMemoryMb
	totalMemoryBytes := int64(math.Round(nodeMemoryMb * 1024.0 * 1024.0))

	// Store hardware information in state
	adapter.State.Hardware = Hardware{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          numCPUs,
		LastChecked:      time.Now(),
	}

	// Update the last known PGStatMonitorEnable state
	pgStatMonitorEnable, ok := service.UserConfig["pg_stat_monitor_enable"]
	if ok {
		adapter.State.LastKnownPGStatMonitorEnable = pgStatMonitorEnable.(bool)
	}

	// Get PostgreSQL version and max connections from database
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	// Create metrics
	totalMemory, err := metrics.NodeMemoryTotal.AsFlatValue(totalMemoryBytes)
	if err != nil {
		adapter.Logger().Errorf("Error creating total memory metric: %v", err)
		return nil, err
	}

	noCPUsMetric, err := metrics.NodeCPUCount.AsFlatValue(numCPUs)
	if err != nil {
		adapter.Logger().Errorf("Error creating number of CPUs metric: %v", err)
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

	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Errorf("Error creating database system info metrics: %v", err)
		return nil, err
	}

	systemInfo := make([]metrics.FlatValue, 0, 4+len(databaseInfo))
	systemInfo = append(systemInfo,
		version,
		totalMemory,
		maxConnectionsMetric,
		noCPUsMetric,
	)
	systemInfo = append(systemInfo, databaseInfo...)

	return systemInfo, nil
}
