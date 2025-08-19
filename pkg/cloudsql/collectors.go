package cloudsql

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/sirupsen/logrus"
)

func CloudSQLHardwareInfo(logger *logrus.Logger, config Config, cloudMonitoringClient *CloudMonitoringClient) func(ctx context.Context, metric_state *agent.MetricsState) error {
	return func(ctx context.Context, metric_state *agent.MetricsState) error {
		// CPU
		cpuUtil, err := GetCPUUtilization(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get CPU utilization: %v", err)
		}

		// note that GCP will give us this a decimal, whereas we expect a percentage
		cpuUtilMetric, _ := metrics.NodeCPUUsage.AsFlatValue(cpuUtil.GetDoubleValue() * 100.)
		metric_state.AddMetric(cpuUtilMetric)

		// Disk: IOPS
		diskIOPSWrite, err := GetDiskIOPSWrite(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get Disk IOPS Write: %v", err)
		}

		diskIOPSRead, err := GetDiskIOPSRead(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get Disk IOPS Read: %v", err)
		}

		diskWriteCount := metric_state.Cache.IOCountersStat.WriteCount + uint64(diskIOPSWrite.GetInt64Value())
		diskReadCount := metric_state.Cache.IOCountersStat.ReadCount + uint64(diskIOPSRead.GetInt64Value())
		diskTotalCount := diskWriteCount + diskReadCount

		metric_state.Cache.IOCountersStat = agent.IOCounterStat{WriteCount: diskWriteCount, ReadCount: diskReadCount}

		diskIOPSWriteMetric, _ := metrics.NodeDiskIOPSWritePerSecond.AsFlatValue(float64(diskIOPSWrite.GetInt64Value() / 60.))
		metric_state.AddMetric(diskIOPSWriteMetric)
		diskIOWriteCountMetric, _ := metrics.NodeDiskIOWriteCount.AsFlatValue(diskWriteCount)
		metric_state.AddMetric(diskIOWriteCountMetric)

		diskIOPSReadMetric, _ := metrics.NodeDiskIOPSReadPerSecond.AsFlatValue(float64(diskIOPSRead.GetInt64Value() / 60.))
		metric_state.AddMetric(diskIOPSReadMetric)
		diskIOReadCountMetric, _ := metrics.NodeDiskIOReadCount.AsFlatValue(diskReadCount)
		metric_state.AddMetric(diskIOReadCountMetric)

		diskIOPSTotalMetic, _ := metrics.NodeDiskIOPSTotalPerSecond.AsFlatValue(float64((diskIOPSRead.GetInt64Value() + diskIOPSWrite.GetInt64Value()) / 60.))
		metric_state.AddMetric(diskIOPSTotalMetic)
		diskIOTotalCountMetric, _ := metrics.NodeDiskIOTotalCount.AsFlatValue(diskTotalCount)
		metric_state.AddMetric(diskIOTotalCountMetric)

		// Disk: Size/utilisation
		diskSize, err := GetDiskSize(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get Disk Size: %v", err)
		}

		diskSizeMetric, _ := metrics.NodeDiskSize.AsFlatValue(diskSize.GetInt64Value())
		metric_state.AddMetric(diskSizeMetric)

		diskUsedPercentage, err := GetDiskUsedPercentage(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get Disk Used Percentage: %v", err)
		}

		diskUsedPercentageMetric, _ := metrics.NodeDiskUsedPercentage.AsFlatValue(diskUsedPercentage.GetDoubleValue() * 100.)
		metric_state.AddMetric(diskUsedPercentageMetric)

		// Network
		networkReceiveCount, err := GetNetworkReceiveCount(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get network recevived count: %v", err)
		}

		networkReceiveCountMetric, _ := metrics.NodeNetworkReceiveCount.AsFlatValue(networkReceiveCount.GetInt64Value())
		metric_state.AddMetric(networkReceiveCountMetric)

		networkSentCount, err := GetNetworkSentCount(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get network sent count: %v", err)
		}

		networkSentCountMetric, _ := metrics.NodeNetworkSendCount.AsFlatValue(networkSentCount.GetInt64Value())
		metric_state.AddMetric(networkSentCountMetric)

		// Memory
		components, err := GetMemoryMetrics(cloudMonitoringClient, config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get memory information: %v", err)
		}
		memoryUsedMetric, err := metrics.NodeMemoryUsed.AsFlatValue(components.Used)
		metric_state.AddMetric(memoryUsedMetric)
		memoryUsedPercentageMetric, err := metrics.NodeMemoryUsedPercentage.AsFlatValue(components.UsedPercentage)
		metric_state.AddMetric(memoryUsedPercentageMetric)

		memoryFreeableMetric, err := metrics.NodeMemoryFreeable.AsFlatValue(components.Freeable)
		metric_state.AddMetric(memoryFreeableMetric)
		memoryAvailablePercentageMetric, err := metrics.NodeMemoryAvailablePercentage.AsFlatValue(components.AvailablePercentage)
		metric_state.AddMetric(memoryAvailablePercentageMetric)

		return nil
	}
}
