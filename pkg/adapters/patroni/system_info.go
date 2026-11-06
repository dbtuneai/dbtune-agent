package patroni

import (
	"context"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

func (adapter *PatroniAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	logger := adapter.Logger()

	logger.Println("Collecting system info for Patroni cluster")

	if err := adapter.checkFailoverBeforeOperation(ctx, "GetSystemInfo"); err != nil {
		return nil, err
	}

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(ctx, err, "in GetSystemInfo (PGVersion)"); failoverErr != nil {
			return nil, failoverErr
		}
		return nil, err
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(ctx, err, "in GetSystemInfo (MaxConnections)"); failoverErr != nil {
			return nil, failoverErr
		}
		return nil, err
	}

	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	hostInfo, err := host.Info()
	if err != nil {
		return nil, err
	}
	// gopsutil can return an empty Platform/PlatformVersion on some Windows
	// builds; the backend rejects a blank string for either field.
	if hostInfo.Platform == "" {
		hostInfo.Platform = "unknown"
	}
	if hostInfo.PlatformVersion == "" {
		hostInfo.PlatformVersion = "unknown"
	}

	noCPUs, err := cpu.Counts(true)
	if err != nil {
		return nil, err
	}

	// Convert into metrics
	totalMemory, err := metrics.NodeMemoryTotal.AsFlatValue(memoryInfo.Total)
	if err != nil {
		return nil, err
	}
	version, _ := metrics.PGVersion.AsFlatValue(pgVersion.String())
	hostOS, _ := metrics.NodeOSInfo.AsFlatValue(hostInfo.OS)
	platform, _ := metrics.NodeOSPlatform.AsFlatValue(hostInfo.Platform)
	platformVersion, _ := metrics.NodeOSPlatformVer.AsFlatValue(hostInfo.PlatformVersion)
	maxConnectionsMetric, _ := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	noCPUsMetric, _ := metrics.NodeCPUCount.AsFlatValue(noCPUs)

	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		if failoverErr := adapter.handlePossibleFailoverError(ctx, err, "in GetSystemInfo (DatabaseSystemInfo)"); failoverErr != nil {
			return nil, failoverErr
		}
		return nil, err
	}

	systemInfo := make([]metrics.FlatValue, 0, 7+len(databaseInfo))
	systemInfo = append(systemInfo,
		version,
		totalMemory,
		hostOS,
		platformVersion,
		platform,
		maxConnectionsMetric,
		noCPUsMetric,
	)
	systemInfo = append(systemInfo, databaseInfo...)

	return systemInfo, nil
}
