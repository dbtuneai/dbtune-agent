package collectors

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/rdsutil"
	"github.com/sirupsen/logrus"
)

// RDSHardwareInfo collects hardware metrics from RDS instance using the Performance Insights API or the CloudWatch API
func RDSHardwareInfo(
	databaseIdentifier string,
	rdsDBInfo *rdsutil.RDSDBInfo,
	clients *rdsutil.AWSClients,
	logger *logrus.Logger,
) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// PI -> granular memory used -> node_memory_used
		if rdsDBInfo.PerformanceInsightsEnabled() {
			resourceID, err := rdsDBInfo.ResourceID()
			// ewww, GO ugly as sh*t
			if err == nil {
				memoryUsage, err := rdsutil.GetMemoryUsageFromPI(clients, resourceID, logger)
				if err == nil {
					memoryMetric, err := utils.NewMetric("node_memory_used", memoryUsage, utils.Bytes)
					if err == nil {
						state.AddMetric(memoryMetric)
					} else {
						logger.Errorf("failed to create memory metric: %v", err)
					}
				} else {
					logger.Errorf("failed to get memory usage from Performance Insights: %v", err)
				}
			}
		}

		// CloudWatch -> freeable memory -> node_memory_freeable
		freeableMemory, err := rdsutil.GetFreeableMemoryFromCW(databaseIdentifier, clients)
		if err != nil {
			logger.Errorf("failed to get freeable memory from CloudWatch: %v", err)
		} else {
			freeableMemoryMetric, _ := utils.NewMetric("node_memory_freeable", freeableMemory, utils.Bytes)
			state.AddMetric(freeableMemoryMetric)
		}

		// CPU Utilization
		cpuUtil, err := rdsutil.GetCPUUtilization(databaseIdentifier, clients)
		if err != nil {
			logger.Errorf("failed to get CPU utilization: %v", err)
		} else {
			cpuUtilMetric, _ := utils.NewMetric("node_cpu_usage", cpuUtil, utils.Percentage)
			state.AddMetric(cpuUtilMetric)
		}

		return nil
	}
}
