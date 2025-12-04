package dockerswarm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// DockerHardwareInfo collects hardware metrics from a Docker Swarm service using the Docker API
func DockerHardwareInfo(client *client.Client, serviceName string) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {

		// Get service info for resource limits
		service, _, err := client.ServiceInspectWithRaw(ctx, serviceName, types.ServiceInspectOptions{})
		if err != nil {
			return fmt.Errorf("failed to inspect service %s: %w", serviceName, err)
		}

		// Get a running task container ID
		filter := filters.NewArgs()
		filter.Add("service", serviceName)
		filter.Add("desired-state", "running")

		tasks, err := client.TaskList(ctx, types.TaskListOptions{
			Filters: filter,
		})
		if err != nil {
			return fmt.Errorf("failed to list tasks for service %s: %w", serviceName, err)
		}

		var containerID string
		for _, task := range tasks {
			if task.Status.State == swarm.TaskStateRunning && task.Status.ContainerStatus != nil {
				containerID = task.Status.ContainerStatus.ContainerID
				break
			}
		}

		if containerID == "" {
			return fmt.Errorf("no running tasks found for service %s", serviceName)
		}

		// Get container stats from the running task
		stats, err := client.ContainerStats(ctx, containerID, false)
		if err != nil {
			return fmt.Errorf("failed to get stats for task container %s: %w", containerID, err)
		}
		defer stats.Body.Close()

		// Get container info for CPU limits
		containerInfo, err := client.ContainerInspect(ctx, containerID)
		if err != nil {
			return fmt.Errorf("failed to inspect task container %s: %w", containerID, err)
		}

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
		cpuMetric, err := metrics.NodeCPUUsage.AsFlatValue(cpuPercent)
		if err != nil {
			return err
		}
		state.AddMetric(cpuMetric)

		// Add CPU count metric - check service resources first, then container limits
		var cpuCount float64
		if service.Spec.TaskTemplate.Resources != nil && service.Spec.TaskTemplate.Resources.Limits != nil && service.Spec.TaskTemplate.Resources.Limits.NanoCPUs > 0 {
			// Use service-level CPU limits (preferred for swarm)
			cpuCount = float64(service.Spec.TaskTemplate.Resources.Limits.NanoCPUs) / 1e9
		} else if containerInfo.HostConfig.NanoCPUs > 0 {
			// Convert from nano CPUs to actual CPU count
			cpuCount = float64(containerInfo.HostConfig.NanoCPUs) / 1e9
		} else if containerInfo.HostConfig.CPUQuota > 0 && containerInfo.HostConfig.CPUPeriod > 0 {
			// Convert from quota/period to CPU count
			cpuCount = float64(containerInfo.HostConfig.CPUQuota) / float64(containerInfo.HostConfig.CPUPeriod)
		} else {
			// If no limits set, use the number of CPUs available to the container
			cpuCount = float64(len(statsJSON.CPUStats.CPUUsage.PercpuUsage))
		}

		cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(int64(cpuCount))
		if err != nil {
			return err
		}
		state.AddMetric(cpuCountMetric)

		// Validate memory stats before calculating
		if statsJSON.MemoryStats.Usage == 0 {
			return fmt.Errorf("invalid memory stats: usage is 0")
		}

		memoryUsed := CalculateDockerMemoryUsed(statsJSON.MemoryStats)
		memUsedMetric, err := metrics.NodeMemoryUsed.AsFlatValue(int64(memoryUsed))
		if err != nil {
			return err
		}
		state.AddMetric(memUsedMetric)

		memLimitMetric, err := metrics.NodeMemoryTotal.AsFlatValue(int64(statsJSON.MemoryStats.Limit))
		if err != nil {
			return err
		}
		state.AddMetric(memLimitMetric)

		return nil
	}
}
