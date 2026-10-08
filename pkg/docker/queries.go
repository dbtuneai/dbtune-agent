package docker

import (
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
)

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

// CalculateDockerCPUCount returns the number of CPUs the container can use.
// An explicit --cpus or quota/period limit wins; otherwise it falls back to the
// CPUs the container can see, honouring --cpuset-cpus. PercpuUsage is empty on
// cgroup v2, so OnlineCPUs is preferred to count available CPUs.
func CalculateDockerCPUCount(hostConfig *container.HostConfig, cpuStats container.CPUStats) float64 {
	available := float64(cpuStats.OnlineCPUs)
	if available == 0 {
		available = float64(len(cpuStats.CPUUsage.PercpuUsage))
	}
	if hostConfig != nil {
		if n := countCpuset(hostConfig.CpusetCpus); n > 0 && (available == 0 || float64(n) < available) {
			available = float64(n)
		}
	}

	var limit float64
	switch {
	case hostConfig == nil:
	case hostConfig.NanoCPUs > 0:
		limit = float64(hostConfig.NanoCPUs) / 1e9
	case hostConfig.CPUQuota > 0 && hostConfig.CPUPeriod > 0:
		limit = float64(hostConfig.CPUQuota) / float64(hostConfig.CPUPeriod)
	}

	if limit > 0 && (available == 0 || limit < available) {
		return limit
	}
	return available
}

// countCpuset counts the CPUs in a cpuset list such as "0-3,8,10-11".
// It returns 0 for an empty or malformed list.
func countCpuset(cpuset string) int {
	cpuset = strings.TrimSpace(cpuset)
	if cpuset == "" {
		return 0
	}
	count := 0
	for _, part := range strings.Split(cpuset, ",") {
		lo, hi, isRange := strings.Cut(strings.TrimSpace(part), "-")
		start, err := strconv.Atoi(lo)
		if err != nil {
			return 0
		}
		end := start
		if isRange {
			if end, err = strconv.Atoi(hi); err != nil || end < start {
				return 0
			}
		}
		count += end - start + 1
	}
	return count
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
