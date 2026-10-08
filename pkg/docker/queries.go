package docker

import "github.com/moby/moby/api/types/container"

// CalculateDockerCPUPercent calculates the CPU usage percentage for a Docker container
// Implementation based on: https://github.com/docker/cli/blob/master/cli/command/container/stats_helpers.go
func CalculateDockerCPUPercent(previousCPU, previousSystem uint64, v *container.StatsResponse) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(v.CPUStats.CPUUsage.TotalUsage) - float64(previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(v.CPUStats.SystemUsage) - float64(previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * 100.0
	}

	return cpuPercent
}

// CalculateDockerCPUCount returns the CPUs the container may use: its limit if
// it has one, else the CPUs online. PercpuUsage is only a fallback because
// cgroup v2 leaves it empty.
func CalculateDockerCPUCount(hc *container.HostConfig, cpu container.CPUStats) float64 {
	switch {
	case hc.NanoCPUs > 0:
		return float64(hc.NanoCPUs) / 1e9
	case hc.CPUQuota > 0 && hc.CPUPeriod > 0:
		return float64(hc.CPUQuota) / float64(hc.CPUPeriod)
	case cpu.OnlineCPUs > 0:
		return float64(cpu.OnlineCPUs)
	default:
		return float64(len(cpu.CPUUsage.PercpuUsage))
	}
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
