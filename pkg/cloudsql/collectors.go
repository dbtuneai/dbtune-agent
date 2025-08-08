package cloudsql

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/sirupsen/logrus"
)

func CloudSQLHardwareInfo(logger *logrus.Logger, config Config) func(ctx context.Context, metric_state *agent.MetricsState) error {
	return func(ctx context.Context, metric_state *agent.MetricsState) error {

		cpuUtil, err := GetCPUUtilization(config.ProjectID, config.DatabaseName)
		if err != nil {
			logger.Errorf("failed to get CPU utilization: %v", err)
		}

		// note that GCP will give us this a decimal, whereas we expect a percentage
		cpuUtilMetric, _ := metrics.NodeCPUUsage.AsFlatValue(cpuUtil * 100)

		metric_state.AddMetric(cpuUtilMetric)

		return nil
	}
}
