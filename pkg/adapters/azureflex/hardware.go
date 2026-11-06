package azureflex

import (
	"context"

	"github.com/dbtuneai/agent/pkg/metrics"
)

func (adapter *AzureFlexAdapter) Collectors() []metrics.Collector {
	return []metrics.Collector{
		{
			Key:       "cpu_utilization",
			Collector: AsCollector(CPUUtilization(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeCPUUsage),
		},
		{
			Key:       "memory_used",
			Collector: AsCollector(MemoryPercent(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeMemoryUsedPercentage),
		},
	}
}

func AsCollector(metricGetter func() (float64, error), metric metrics.MetricDef) func(ctx context.Context, metric_state *metrics.State) error {
	return func(_ context.Context, metric_state *metrics.State) error {
		metricResult, err := metricGetter()
		if err != nil {
			return err
		}
		metricValue, err := metric.AsFlatValue(metricResult)
		if err != nil {
			return err
		}
		metric_state.AddMetric(metricValue)
		return nil
	}
}

func CPUUtilization(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
	return func() (float64, error) {
		return GetMostRecentAzureMetric(subscriptionId, resourceGroupName, serverName, "cpu_percent", DefaultGetMostRecentAzureMetricOpts())
	}
}

func MemoryPercent(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
	return func() (float64, error) {
		return GetMostRecentAzureMetric(subscriptionId, resourceGroupName, serverName, "memory_percent", DefaultGetMostRecentAzureMetricOpts())
	}
}
