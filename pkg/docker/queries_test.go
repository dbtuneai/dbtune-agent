package docker

import (
	"testing"

	"github.com/moby/moby/api/types/container"
)

func TestCalculateDockerCPUCount(t *testing.T) {
	tests := []struct {
		name string
		hc   container.HostConfig
		cpu  container.CPUStats
		want float64
	}{
		{"nano cpus limit", container.HostConfig{Resources: container.Resources{NanoCPUs: 2_500_000_000}}, container.CPUStats{OnlineCPUs: 14}, 2.5},
		{"quota limit", container.HostConfig{Resources: container.Resources{CPUQuota: 400_000, CPUPeriod: 100_000}}, container.CPUStats{OnlineCPUs: 14}, 4},
		{"no limit, cgroup v2 (empty percpu)", container.HostConfig{}, container.CPUStats{OnlineCPUs: 14}, 14},
		{"no limit, no online cpus (cgroup v1)", container.HostConfig{}, container.CPUStats{CPUUsage: container.CPUUsage{PercpuUsage: make([]uint64, 8)}}, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateDockerCPUCount(&tt.hc, tt.cpu); got != tt.want {
				t.Errorf("CalculateDockerCPUCount() = %v, want %v", got, tt.want)
			}
		})
	}
}
