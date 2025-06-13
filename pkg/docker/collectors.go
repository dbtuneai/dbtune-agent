package docker

import (
	"context"
	"encoding/json"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerHardwareInfo collects hardware metrics from a Docker container using the Docker API
func DockerHardwareInfo(client *client.Client, containerName string) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {

		// Get container stats
		stats, err := client.ContainerStats(ctx, containerName, false)
		if err != nil {
			return err
		}
		defer stats.Body.Close()

		// Parse stats and create metrics
		var statsJSON container.StatsResponse

		// Decode JSON stats
		decoder := json.NewDecoder(stats.Body)
		if err := decoder.Decode(&statsJSON); err != nil {
			return err
		}

		// Calculate CPU percentage using the utility function
		cpuPercent := CalculateDockerCPUPercent(
			statsJSON.PreCPUStats.CPUUsage.TotalUsage,
			statsJSON.PreCPUStats.SystemUsage,
			&statsJSON,
		)

		// Add metrics
		cpuMetric, _ := metrics.NodeCPUUsage.AsFlatValue(cpuPercent)
		state.AddMetric(cpuMetric)

		memoryUsed := CalculateDockerMemoryUsed(statsJSON.MemoryStats)
		memUsedMetric, _ := metrics.NodeMemoryUsed.AsFlatValue(memoryUsed)
		state.AddMetric(memUsedMetric)

		memLimitMetric, _ := metrics.NodeMemoryTotal.AsFlatValue(statsJSON.MemoryStats.Limit)
		state.AddMetric(memLimitMetric)

		return nil
	}
}
