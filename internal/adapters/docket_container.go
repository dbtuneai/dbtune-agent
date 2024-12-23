package adapters

import (
	"fmt"
)

// DockerContainerAdapter works with the container name and by
// communicating with the docker Unix socket to get stats like memory usage,
// number of CPUs available and memory limit
type DockerContainerAdapter struct {
	DefaultPostgreSQLAdapter
	ContainerName string
}

func CreateDockerContainerAdapter(url string, agentID string, instanceID string) *DockerContainerAdapter {

	fmt.Println("Custom logic is here")

	dockerAdapter := &DockerContainerAdapter{
		DefaultPostgreSQLAdapter: *CreateDefaultPostgreSQLAdapter(url, agentID, instanceID),
		ContainerName:            "my-container",
	}

	return dockerAdapter
}
