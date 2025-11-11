package dockerswarm

import "github.com/docker/docker/api/types/container"

// CalculateDockerCPUPercent calculates the CPU usage percentage for a Docker container
// Implementation based on: https://github.com/docker/cli/blob/master/cli/command/container/stats_helpers.go
func CalculateDockerCPUPercent(previousCPU, previousSystem uint64, v *container.StatsResponse) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(v.CPUStats.CPUUsage.TotalUsage) - float64(previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(v.CPUStats.SystemUsage) - float64(previousSystem)
		onlineCPUs  = float64(v.CPUStats.OnlineCPUs)
	)

	if onlineCPUs == 0.0 {
		onlineCPUs = float64(len(v.CPUStats.CPUUsage.PercpuUsage))
	}

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * onlineCPUs * 100.0
	}

	return cpuPercent
}

// CalculateDockerMemoryUsed is mirroring the official way to calculate:
// https://github.com/docker/cli/blob/master/cli/command/container/stats_helpers.go#L239
func CalculateDockerMemoryUsed(mem container.MemoryStats) float64 {
	// cgroup v1
	if v, isCgroup1 := mem.Stats["total_inactive_file"]; isCgroup1 && v < mem.Usage {
		return float64(mem.Usage - v)
	}
	// cgroup v2
	if v := mem.Stats["inactive_file"]; v < mem.Usage {
		return float64(mem.Usage - v)
	}
	return float64(mem.Usage)
}
