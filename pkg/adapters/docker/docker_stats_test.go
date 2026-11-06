package docker

import (
	"testing"

	"github.com/moby/moby/api/types/container"
)

func TestCalculateDockerCPUCount(t *testing.T) {
	tests := []struct {
		name       string
		hostConfig *container.HostConfig
		cpuStats   container.CPUStats
		want       float64
	}{
		{
			name:       "no limit on cgroup v2 uses online CPUs",
			hostConfig: &container.HostConfig{},
			cpuStats:   container.CPUStats{OnlineCPUs: 18},
			want:       18,
		},
		{
			name:       "no limit on cgroup v1 falls back to per-CPU usage",
			hostConfig: &container.HostConfig{},
			cpuStats:   container.CPUStats{CPUUsage: container.CPUUsage{PercpuUsage: make([]uint64, 8)}},
			want:       8,
		},
		{
			name:       "nano CPUs limit",
			hostConfig: &container.HostConfig{Resources: container.Resources{NanoCPUs: 4e9}},
			cpuStats:   container.CPUStats{OnlineCPUs: 18},
			want:       4,
		},
		{
			name:       "quota and period limit",
			hostConfig: &container.HostConfig{Resources: container.Resources{CPUQuota: 250000, CPUPeriod: 100000}},
			cpuStats:   container.CPUStats{OnlineCPUs: 18},
			want:       2.5,
		},
		{
			name:       "cpuset restricts available CPUs",
			hostConfig: &container.HostConfig{Resources: container.Resources{CpusetCpus: "0-3,8"}},
			cpuStats:   container.CPUStats{OnlineCPUs: 18},
			want:       5,
		},
		{
			name:       "limit above available CPUs is capped",
			hostConfig: &container.HostConfig{Resources: container.Resources{NanoCPUs: 32e9}},
			cpuStats:   container.CPUStats{OnlineCPUs: 18},
			want:       18,
		},
		{
			name:     "nil host config",
			cpuStats: container.CPUStats{OnlineCPUs: 6},
			want:     6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateDockerCPUCount(tt.hostConfig, tt.cpuStats); got != tt.want {
				t.Errorf("CalculateDockerCPUCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCountCpuset(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"0", 1},
		{"0-3", 4},
		{"0-3,8,10-11", 7},
		{" 0, 2 ", 2},
		{"3-1", 0},
		{"a-b", 0},
	}
	for _, tt := range tests {
		if got := countCpuset(tt.in); got != tt.want {
			t.Errorf("countCpuset(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
