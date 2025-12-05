// Package kubernetes provides a Kubernetes client with auto-detection:
// 1. Try in-cluster config first (for when agent runs as a pod)
// 2. Fall back to kubeconfig file (configPath or default ~/.kube/config)
// Also provides light wrappers around Kubernetes resources for simpler usage.
package kubernetes

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtuneai/agent/pkg/metrics"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/kubernetes"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsv1 "k8s.io/metrics/pkg/client/clientset/versioned"
)

type Client struct {
	Namespace     string
	Config        *rest.Config
	Clientset     *kubernetes.Clientset
	MetricsClient *metricsv1.Clientset
}

// CreateClient creates a Kubernetes client with auto-detection:
// 1. Try in-cluster config first (for when agent runs as a pod)
// 2. Fall back to kubeconfig file (configPath or default ~/.kube/config)
// Client also keeps track of namespace
func CreateClient(configPath string, namespace string) (Client, error) {
	var config *rest.Config
	var err error

	// Try in-cluster config first
	config, err = rest.InClusterConfig()
	if err == nil {
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return Client{}, fmt.Errorf("failed to create in-cluster K8s client: %w", err)
		}
		metricsClient, err := metricsv1.NewForConfig(config)
		if err != nil {
			return Client{}, fmt.Errorf("failed to create in-cluster metrics client: %w", err)
		}
		return Client{
			Namespace:     namespace,
			Config:        config,
			Clientset:     clientset,
			MetricsClient: metricsClient,
		}, nil
	}

	// Fall back to kubeconfig
	if configPath == "" {
		// Use default kubeconfig path
		home, err := os.UserHomeDir()
		if err != nil {
			return Client{}, fmt.Errorf("failed to get home directory: %w", err)
		}
		configPath = filepath.Join(home, ".kube", "config")
	}

	config, err = clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		return Client{}, fmt.Errorf("failed to load kubeconfig from %s: %w", configPath, err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return Client{}, fmt.Errorf("failed to create K8s client from kubeconfig: %w", err)
	}

	metricsClient, err := metricsv1.NewForConfig(config)
	if err != nil {
		return Client{}, fmt.Errorf("failed to create metrics client from kubeconfig: %w", err)
	}

	return Client{
		Namespace:     namespace,
		Config:        config,
		Clientset:     clientset,
		MetricsClient: metricsClient,
	}, nil
}

// FindCNPGPrimaryPod finds the primary pod in a CNPG cluster by label selector.
// CNPG labels pods with cnpg.io/cluster=<cluster-name> and role=primary.
func (c *Client) FindCNPGPrimaryPod(ctx context.Context, clusterName string) (string, error) {
	labelSelector := fmt.Sprintf("cnpg.io/cluster=%s,role=primary", clusterName)

	pods, err := c.Clientset.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list pods with selector %s: %w", labelSelector, err)
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no primary pod found for CNPG cluster %s", clusterName)
	}

	// Return the first (and should be only) primary pod
	return pods.Items[0].Name, nil
}

// CNPGClusterStatus contains key status fields from a CNPG Cluster resource
type CNPGClusterStatus struct {
	// Current primary pod name (e.g., "postgres-1")
	CurrentPrimary string
	// Target primary pod name during failover/switchover (e.g., "postgres-2")
	TargetPrimary string
	// Cluster phase (e.g., "Cluster in healthy state", "Failing over", "Switchover in progress")
	Phase string
	// Number of ready instances
	ReadyInstances int64
	// Total number of instances
	Instances int64
}

// GetCNPGClusterStatus retrieves the status of a CNPG cluster from the Cluster CRD.
// This is more reliable than checking pod labels because it shows the cluster's
// actual state including failover/switchover in progress.
func (c *Client) GetCNPGClusterStatus(ctx context.Context, clusterName string) (*CNPGClusterStatus, error) {
	// Create dynamic client for reading CRDs
	dynamicClient, err := dynamic.NewForConfig(c.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    "postgresql.cnpg.io",
		Version:  "v1",
		Resource: "clusters",
	}

	// Get the cluster resource
	cluster, err := dynamicClient.Resource(gvr).
		Namespace(c.Namespace).
		Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get CNPG cluster %s: %w", clusterName, err)
	}

	// Extract status fields
	status, found, err := unstructured.NestedMap(cluster.Object, "status")
	if err != nil || !found {
		return nil, fmt.Errorf("cluster status not found")
	}

	clusterStatus := &CNPGClusterStatus{}

	// Extract current primary
	if currentPrimary, found, _ := unstructured.NestedString(status, "currentPrimary"); found {
		clusterStatus.CurrentPrimary = currentPrimary
	}

	// Extract target primary (set during failover/switchover)
	if targetPrimary, found, _ := unstructured.NestedString(status, "targetPrimary"); found {
		clusterStatus.TargetPrimary = targetPrimary
	}

	// Extract phase
	if phase, found, _ := unstructured.NestedString(status, "phase"); found {
		clusterStatus.Phase = phase
	}

	// Extract instance counts
	if readyInstances, found, _ := unstructured.NestedInt64(status, "readyInstances"); found {
		clusterStatus.ReadyInstances = readyInstances
	}

	if instances, found, _ := unstructured.NestedInt64(status, "instances"); found {
		clusterStatus.Instances = instances
	}

	return clusterStatus, nil
}

type PodClient struct {
	Namespace     string
	PodName       string
	Clientset     *kubernetes.Clientset
	MetricsClient *metricsv1.Clientset
}

func (c *Client) PodClient(podName string) PodClient {
	return PodClient{
		Namespace: c.Namespace,
		Clientset: c.Clientset,
		PodName:   podName,
	}
}

type ContainerClient struct {
	Namespace     string
	PodName       string
	ContainerName string
	Clientset     *kubernetes.Clientset
	MetricsClient *metricsv1.Clientset
}

func (c *Client) ContainerClient(podName string, containerName string) ContainerClient {
	return ContainerClient{
		Namespace:     c.Namespace,
		PodName:       podName,
		ContainerName: containerName,
		Clientset:     c.Clientset,
		MetricsClient: c.MetricsClient,
	}
}

func (cc *ContainerClient) CPULimitCoresMilli(ctx context.Context) (int64, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	container := findContainer(pod.Spec.Containers, cc.ContainerName)
	if container == nil {
		return 0, fmt.Errorf("pod %s has no containers", cc.PodName)
	}

	cpuLimit := container.Resources.Limits.Cpu()
	if cpuLimit != nil && cpuLimit.Value() > 0 {
		return cpuLimit.MilliValue(), nil
	}

	// Fallback to requests if limits are not set
	cpuLimit = container.Resources.Requests.Cpu()
	if cpuLimit != nil && cpuLimit.Value() > 0 {
		return cpuLimit.MilliValue(), nil
	}
	// Final fallback: query cAdvisor for actual cgroup CPU limit
	cgroupLimit, err := cc.cpuCgroupLimitMillicores(ctx)
	if err == nil && cgroupLimit > 0 {
		return cgroupLimit, nil
	}
	return 0, fmt.Errorf("pod %s has no CPU limit, request, or cgroup limit: %w", cc.PodName, err)
}

func (cc *ContainerClient) MemoryLimitBytes(ctx context.Context) (int64, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	container := findContainer(pod.Spec.Containers, cc.ContainerName)
	if container == nil {
		return 0, fmt.Errorf("pod %s has no containers", cc.PodName)
	}

	memoryLimit := container.Resources.Limits.Memory()
	if memoryLimit != nil && memoryLimit.Value() > 0 {
		return memoryLimit.Value(), nil
	}

	// Fallback to requests if limits are not set
	memoryLimit = container.Resources.Requests.Memory()
	if memoryLimit != nil && memoryLimit.Value() > 0 {
		return memoryLimit.Value(), nil
	}

	// Final fallback: query cAdvisor for actual cgroup memory limit
	cgroupLimit, err := cc.memoryCgroupLimitBytes(ctx)
	if err != nil {
		return 0, fmt.Errorf("pod %s has no memory limit, request, or cgroup limit: %w", cc.PodName, err)
	}
	return cgroupLimit, nil
}

// cpuCgroupLimitMillicores queries cAdvisor for the container's cgroup CPU limit.
// This is used as a fallback when Kubernetes resource limits/requests are not set.
// Returns CPU limit in millicores (1 core = 1000 millicores).
// If the container has no limit (quota missing or < 0), returns the node's CPU capacity.
func (cc *ContainerClient) cpuCgroupLimitMillicores(ctx context.Context) (int64, error) {
	metrics, err := cc.getCAdvisorMetrics(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get cAdvisor metrics: %w", err)
	}

	lines := string(metrics)

	// Parse line by line looking for container_spec_cpu_quota and container_spec_cpu_period
	// CPU limit = (quota / period) * 1000 millicores
	var quota, period float64
	quotaFound, periodFound := false, false

	for line := range strings.SplitSeq(lines, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if line contains our container's labels
		if !strings.Contains(line, fmt.Sprintf(`container="%s"`, cc.ContainerName)) ||
			!strings.Contains(line, fmt.Sprintf(`pod="%s"`, cc.PodName)) ||
			!strings.Contains(line, fmt.Sprintf(`namespace="%s"`, cc.Namespace)) {
			continue
		}

		if strings.HasPrefix(line, "container_spec_cpu_quota{") {
			if val := extractMetricValue(line); val >= 0 {
				quota = val
				quotaFound = true
			}
		} else if strings.HasPrefix(line, "container_spec_cpu_period{") {
			if val := extractMetricValue(line); val > 0 {
				period = val
				periodFound = true
			}
		}

		if quotaFound && periodFound {
			break
		}
	}

	// If quota metric doesn't exist or is negative, container has no CPU limit - use node capacity
	if !quotaFound || quota < 0 {
		return cc.nodeCPUCapacityMillicores(ctx)
	}

	if !periodFound {
		return 0, fmt.Errorf("CPU period metric not found for container %s in pod %s", cc.ContainerName, cc.PodName)
	}

	// Calculate millicores: (quota / period) * 1000
	cores := quota / period
	millicores := int64(cores * 1000)

	return millicores, nil
}

// memoryCgroupLimitBytes queries cAdvisor for the container's cgroup memory limit.
// This is used as a fallback when Kubernetes resource limits/requests are not set.
// If the container has no limit (value is 0 or very large), returns the node's total memory.
func (cc *ContainerClient) memoryCgroupLimitBytes(ctx context.Context) (int64, error) {
	metrics, err := cc.getCAdvisorMetrics(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get cAdvisor metrics: %w", err)
	}

	lines := string(metrics)

	// Parse line by line looking for container_spec_memory_limit_bytes
	for line := range strings.SplitSeq(lines, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if this is the memory limit metric for our container
		if !strings.HasPrefix(line, "container_spec_memory_limit_bytes{") {
			continue
		}

		// Check if line contains our container's labels
		if !strings.Contains(line, fmt.Sprintf(`container="%s"`, cc.ContainerName)) ||
			!strings.Contains(line, fmt.Sprintf(`pod="%s"`, cc.PodName)) ||
			!strings.Contains(line, fmt.Sprintf(`namespace="%s"`, cc.Namespace)) {
			continue
		}

		// Extract the value
		if val := extractMetricValue(line); val >= 0 {
			limit := int64(val)
			// If limit is 0 or very large (>1PB), container is unlimited - use node capacity
			if limit == 0 || limit > 1<<50 {
				return cc.nodeMemoryCapacityBytes(ctx)
			}
			return limit, nil
		}
	}

	return 0, fmt.Errorf("container_spec_memory_limit_bytes metric not found for container %s in pod %s", cc.ContainerName, cc.PodName)
}

// MemoryUsageBytes returns the current memory usage in bytes.
func (cc *ContainerClient) MemoryUsageBytes(ctx context.Context) (int64, error) {
	podMetrics, err := podMetrics(ctx, cc.MetricsClient, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	container := findContainerMetrics(podMetrics.Containers, cc.ContainerName)
	if container == nil {
		return 0, fmt.Errorf("pod %s has no containers", cc.PodName)
	}

	memoryUsage := container.Usage.Memory().Value()
	if memoryUsage == 0 {
		return 0, fmt.Errorf("pod %s has no memory usage", cc.PodName)
	}

	return memoryUsage, nil
}

// CPUUsageMillicores returns the current instantaneous CPU usage in millicores from metrics-server.
// This is NOT cumulative - it's a snapshot of current usage at the time of the call.
// 1 core = 1000 millicores.
func (cc *ContainerClient) CPUUsageMillicores(ctx context.Context) (int64, error) {
	podMetrics, err := podMetrics(ctx, cc.MetricsClient, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	container := findContainerMetrics(podMetrics.Containers, cc.ContainerName)
	if container == nil {
		return 0, fmt.Errorf("pod %s has no containers", cc.PodName)
	}

	cpuUsage := container.Usage.Cpu()
	if cpuUsage != nil && cpuUsage.MilliValue() > 0 {
		return cpuUsage.MilliValue(), nil
	}

	return 0, fmt.Errorf("pod %s has no CPU usage", cc.PodName)
}

func findContainer(containers []v1.Container, name string) *v1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func findContainerMetrics(metrics []metricsv1beta1.ContainerMetrics, name string) *metricsv1beta1.ContainerMetrics {
	for i := range metrics {
		if metrics[i].Name == name {
			return &metrics[i]
		}
	}
	return nil
}

func getPod(ctx context.Context, clientset *kubernetes.Clientset, namespace string, podName string) (*v1.Pod, error) {
	return clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
}

func podMetrics(ctx context.Context, metricsClient *metricsv1.Clientset, namespace string, podName string) (*metricsv1beta1.PodMetrics, error) {
	return metricsClient.MetricsV1beta1().PodMetricses(namespace).Get(ctx, podName, metav1.GetOptions{})
}

// ContainerIOStats contains cumulative I/O statistics for a container from cAdvisor metrics.
type ContainerIOStats struct {
	// Cumulative count of read operations completed
	ReadsTotal uint64
	// Cumulative count of write operations completed
	WritesTotal uint64
	// Cumulative bytes read
	ReadBytesTotal uint64
	// Cumulative bytes written
	WriteBytesTotal uint64
	// Timestamp from cAdvisor in milliseconds (used to detect stale metrics)
	Timestamp int64
}

// IOStats returns cumulative I/O statistics for the container.
// These are cumulative counters - to get rates, take the difference between two samples.
func (cc *ContainerClient) IOStats(ctx context.Context) (*ContainerIOStats, error) {
	metrics, err := cc.getCAdvisorMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return parseContainerIOMetrics(metrics, cc.ContainerName, cc.PodName, cc.Namespace)
}

// NodeOSInfo contains operating system information from the node.
type NodeOSInfo struct {
	OSImage          string // e.g., "Ubuntu 22.04.3 LTS"
	KernelVersion    string // e.g., "5.15.0-1048-gke"
	OperatingSystem  string // e.g., "linux"
	Architecture     string // e.g., "amd64"
	ContainerRuntime string // e.g., "containerd://1.6.24"
	KubeletVersion   string // e.g., "v1.27.3-gke.100"
}

// NodeOSInfo returns operating system information from the node where the pod is running.
func (cc *ContainerClient) NodeOSInfo(ctx context.Context) (*NodeOSInfo, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return nil, fmt.Errorf("pod %s is not scheduled to a node", cc.PodName)
	}

	node, err := cc.Clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	return &NodeOSInfo{
		OSImage:          node.Status.NodeInfo.OSImage,
		KernelVersion:    node.Status.NodeInfo.KernelVersion,
		OperatingSystem:  node.Status.NodeInfo.OperatingSystem,
		Architecture:     node.Status.NodeInfo.Architecture,
		ContainerRuntime: node.Status.NodeInfo.ContainerRuntimeVersion,
		KubeletVersion:   node.Status.NodeInfo.KubeletVersion,
	}, nil
}

// nodeMemoryCapacityBytes returns the total allocatable memory on the node where the pod is running.
// This is used as a fallback when a container has no memory limit set.
func (cc *ContainerClient) nodeMemoryCapacityBytes(ctx context.Context) (int64, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return 0, fmt.Errorf("pod %s is not scheduled to a node", cc.PodName)
	}

	node, err := cc.Clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	// Use allocatable memory (what can be used by pods) rather than capacity (total physical memory)
	allocatableMemory := node.Status.Allocatable.Memory()
	if allocatableMemory == nil || allocatableMemory.Value() == 0 {
		return 0, fmt.Errorf("node %s has no allocatable memory", nodeName)
	}

	return allocatableMemory.Value(), nil
}

// nodeCPUCapacityMillicores returns the total allocatable CPU on the node where the pod is running.
// This is used as a fallback when a container has no CPU limit set.
func (cc *ContainerClient) nodeCPUCapacityMillicores(ctx context.Context) (int64, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return 0, fmt.Errorf("pod %s is not scheduled to a node", cc.PodName)
	}

	node, err := cc.Clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	// Use allocatable CPU (what can be used by pods) rather than capacity (total physical CPU)
	allocatableCPU := node.Status.Allocatable.Cpu()
	if allocatableCPU == nil || allocatableCPU.MilliValue() == 0 {
		return 0, fmt.Errorf("node %s has no allocatable CPU", nodeName)
	}

	return allocatableCPU.MilliValue(), nil
}

// NetworkStats contains cumulative network statistics for the container.
type NetworkStats struct {
	// Cumulative bytes received
	ReceiveBytesTotal uint64
	// Cumulative bytes transmitted
	TransmitBytesTotal uint64
	// Cumulative packets received
	ReceivePacketsTotal uint64
	// Cumulative packets transmitted
	TransmitPacketsTotal uint64
}

// NetworkStats returns cumulative network statistics for the container from cAdvisor.
func (cc *ContainerClient) NetworkStats(ctx context.Context) (*NetworkStats, error) {
	metrics, err := cc.getCAdvisorMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return parseContainerNetworkMetrics(metrics, cc.ContainerName, cc.PodName, cc.Namespace)
}

// DiskInfo contains filesystem information for the container's volumes.
type DiskInfo struct {
	// Total disk space in bytes
	TotalBytes uint64
	// Used disk space in bytes
	UsedBytes uint64
	// Usage percentage (0-100)
	UsedPercent float64
	// Filesystem type (e.g., "ext4", "xfs", "overlay")
	FilesystemType string
	// Storage class name (e.g., "standard", "fast-ssd")
	StorageClassName string
}

// DiskInfo returns disk usage information for the container from cAdvisor.
func (cc *ContainerClient) DiskInfo(ctx context.Context) (*DiskInfo, error) {
	metrics, err := cc.getCAdvisorMetrics(ctx)
	if err != nil {
		return nil, err
	}

	info, err := parseContainerDiskMetrics(metrics, cc.ContainerName, cc.PodName, cc.Namespace)
	if err != nil {
		return nil, err
	}

	// Try to get storage class from PVC
	storageClass, err := cc.getStorageClassName(ctx)
	if err == nil {
		info.StorageClassName = storageClass
	}

	return info, nil
}

// getStorageClassName returns the storage class name for the container's PVC.
func (cc *ContainerClient) getStorageClassName(ctx context.Context) (string, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return "", fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	// Find the container in the pod spec
	container := findContainer(pod.Spec.Containers, cc.ContainerName)
	if container == nil {
		return "", fmt.Errorf("container %s not found in pod %s", cc.ContainerName, cc.PodName)
	}

	// Look for volume mounts in the container
	for _, volumeMount := range container.VolumeMounts {
		// Find the corresponding volume in the pod spec
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == volumeMount.Name && volume.PersistentVolumeClaim != nil {
				// Get the PVC
				pvc, err := cc.Clientset.CoreV1().PersistentVolumeClaims(cc.Namespace).Get(
					ctx,
					volume.PersistentVolumeClaim.ClaimName,
					metav1.GetOptions{},
				)
				if err != nil {
					continue // Try next volume
				}

				// Return the storage class name
				if pvc.Spec.StorageClassName != nil {
					return *pvc.Spec.StorageClassName, nil
				}
			}
		}
	}

	return "", fmt.Errorf("no PVC found for container %s in pod %s", cc.ContainerName, cc.PodName)
}

// LoadAverage returns the system load average from the node.
func (cc *ContainerClient) LoadAverage(ctx context.Context) (float64, error) {
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return 0, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return 0, fmt.Errorf("pod %s is not scheduled to a node", cc.PodName)
	}

	// Query the node's metrics endpoint for load average
	req := cc.Clientset.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(nodeName).
		SubResource("proxy").
		Suffix("metrics/cadvisor")

	result := req.Do(ctx)
	body, err := result.Raw()
	if err != nil {
		return 0, fmt.Errorf("failed to get node metrics: %w", err)
	}

	// Parse load average from machine_load_average metric
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "machine_load_average") {
			value := extractMetricValue(line)
			if value >= 0 {

				return value, nil
			}
		}
	}

	return 0, fmt.Errorf("load average metric not found")
}

// getCAdvisorMetrics fetches cAdvisor metrics from the kubelet endpoint.
func (cc *ContainerClient) getCAdvisorMetrics(ctx context.Context) ([]byte, error) {
	// Get the node name for the pod
	pod, err := getPod(ctx, cc.Clientset, cc.Namespace, cc.PodName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s: %w", cc.PodName, err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return nil, fmt.Errorf("pod %s is not scheduled to a node", cc.PodName)
	}

	// Query the kubelet cAdvisor metrics API via the API server proxy
	req := cc.Clientset.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(nodeName).
		SubResource("proxy").
		Suffix("metrics/cadvisor")

	data, err := req.DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cAdvisor metrics from node %s: %w", nodeName, err)
	}

	return data, nil
}

// GetContainerSystemInfo gathers static system information from Kubernetes container
// This should be called once at startup or infrequently, as the values don't change often
func (cc *ContainerClient) GetContainerSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	var systemInfo []metrics.FlatValue

	// Get CPU limit (static)
	cpuLimitMillicores, err := cc.CPULimitCoresMilli(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU limit: %w", err)
	}
	cpuCores := float64(cpuLimitMillicores) / 1000.0
	cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(int64(cpuCores))
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU count metric: %w", err)
	}
	systemInfo = append(systemInfo, cpuCountMetric)

	// Get memory limit (static)
	memoryLimitBytes, err := cc.MemoryLimitBytes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get memory limit: %w", err)
	}
	memTotalMetric, err := metrics.NodeMemoryTotal.AsFlatValue(memoryLimitBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory total metric: %w", err)
	}
	systemInfo = append(systemInfo, memTotalMetric)

	// Get disk info (total size is static)
	diskInfo, err := cc.DiskInfo(ctx)
	if err == nil {
		diskSizeMetric, err := metrics.NodeDiskSize.AsFlatValue(int64(diskInfo.TotalBytes))
		if err == nil {
			systemInfo = append(systemInfo, diskSizeMetric)
		}
	}

	// Get OS info (static)
	osInfo, err := cc.NodeOSInfo(ctx)
	if err == nil {
		// Add OS info string metric (combined OS image)
		if osInfoMetric, err := metrics.NodeOSInfo.AsFlatValue(osInfo.OSImage); err == nil {
			systemInfo = append(systemInfo, osInfoMetric)
		}

		// Add platform metrics
		if platformMetric, err := metrics.NodeOSPlatform.AsFlatValue(osInfo.OperatingSystem); err == nil {
			systemInfo = append(systemInfo, platformMetric)
		}

		if platformVerMetric, err := metrics.NodeOSPlatformVer.AsFlatValue(osInfo.KernelVersion); err == nil {
			systemInfo = append(systemInfo, platformVerMetric)
		}
	}

	return systemInfo, nil
}

// parseContainerIOMetrics parses cAdvisor Prometheus metrics to extract container I/O stats.
// Accumulates metrics across all devices for the container.
func parseContainerIOMetrics(metricsData []byte, containerName, podName, namespace string) (*ContainerIOStats, error) {
	lines := string(metricsData)
	stats := &ContainerIOStats{}
	found := false

	// Parse line by line looking for metrics matching our container
	for line := range strings.SplitSeq(lines, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if line contains our container's labels
		if !strings.Contains(line, fmt.Sprintf(`container="%s"`, containerName)) ||
			!strings.Contains(line, fmt.Sprintf(`pod="%s"`, podName)) ||
			!strings.Contains(line, fmt.Sprintf(`namespace="%s"`, namespace)) {
			continue
		}

		// Extract metric name and value, accumulating across all devices
		if strings.HasPrefix(line, "container_fs_reads_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.ReadsTotal += uint64(val)
				// Capture timestamp from first matching metric
				if stats.Timestamp == 0 {
					stats.Timestamp = extractMetricTimestamp(line)
				}
				found = true
			}
		} else if strings.HasPrefix(line, "container_fs_writes_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.WritesTotal += uint64(val)
				// Capture timestamp from first matching metric
				if stats.Timestamp == 0 {
					stats.Timestamp = extractMetricTimestamp(line)
				}
				found = true
			}
		} else if strings.HasPrefix(line, "container_blkio_device_usage_total{") && strings.Contains(line, `operation="Read"`) {
			if val := extractMetricValue(line); val >= 0 {
				stats.ReadBytesTotal += uint64(val)
				found = true
			}
		} else if strings.HasPrefix(line, "container_blkio_device_usage_total{") && strings.Contains(line, `operation="Write"`) {
			if val := extractMetricValue(line); val >= 0 {
				stats.WriteBytesTotal += uint64(val)
				found = true
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("no I/O metrics found for container %s in pod %s", containerName, podName)
	}

	return stats, nil
}

// parseContainerNetworkMetrics parses network statistics from cAdvisor metrics.
func parseContainerNetworkMetrics(metricsData []byte, containerName, podName, namespace string) (*NetworkStats, error) {
	lines := string(metricsData)
	stats := &NetworkStats{}
	found := false

	// Parse line by line looking for metrics matching our container
	for line := range strings.SplitSeq(lines, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if line contains our container's labels
		if !strings.Contains(line, fmt.Sprintf(`container="%s"`, containerName)) ||
			!strings.Contains(line, fmt.Sprintf(`pod="%s"`, podName)) ||
			!strings.Contains(line, fmt.Sprintf(`namespace="%s"`, namespace)) {
			continue
		}

		// Extract network metrics, accumulating across all interfaces
		if strings.HasPrefix(line, "container_network_receive_bytes_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.ReceiveBytesTotal += uint64(val)
				found = true
			}
		} else if strings.HasPrefix(line, "container_network_transmit_bytes_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.TransmitBytesTotal += uint64(val)
				found = true
			}
		} else if strings.HasPrefix(line, "container_network_receive_packets_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.ReceivePacketsTotal += uint64(val)
				found = true
			}
		} else if strings.HasPrefix(line, "container_network_transmit_packets_total{") {
			if val := extractMetricValue(line); val >= 0 {
				stats.TransmitPacketsTotal += uint64(val)
				found = true
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("no network metrics found for container %s in pod %s", containerName, podName)
	}

	return stats, nil
}

// parseContainerDiskMetrics parses disk usage statistics from cAdvisor metrics.
func parseContainerDiskMetrics(metricsData []byte, containerName, podName, namespace string) (*DiskInfo, error) {
	lines := string(metricsData)
	info := &DiskInfo{}
	found := false

	// Parse line by line looking for metrics matching our container
	for line := range strings.SplitSeq(lines, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if line contains our container's labels
		if !strings.Contains(line, fmt.Sprintf(`container="%s"`, containerName)) ||
			!strings.Contains(line, fmt.Sprintf(`pod="%s"`, podName)) ||
			!strings.Contains(line, fmt.Sprintf(`namespace="%s"`, namespace)) {
			continue
		}

		// Extract filesystem metrics
		if strings.HasPrefix(line, "container_fs_limit_bytes{") {
			if val := extractMetricValue(line); val >= 0 {
				info.TotalBytes = uint64(val)
				found = true
				// Extract filesystem type from the fstype label
				if info.FilesystemType == "" {
					info.FilesystemType = extractLabel(line, "fstype")
				}
			}
		} else if strings.HasPrefix(line, "container_fs_usage_bytes{") {
			if val := extractMetricValue(line); val >= 0 {
				info.UsedBytes = uint64(val)
				found = true
				// Extract filesystem type from the fstype label
				if info.FilesystemType == "" {
					info.FilesystemType = extractLabel(line, "fstype")
				}
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("no disk metrics found for container %s in pod %s", containerName, podName)
	}

	// Calculate usage percentage
	if info.TotalBytes > 0 {
		info.UsedPercent = (float64(info.UsedBytes) / float64(info.TotalBytes)) * 100.0
	}

	return info, nil
}

// extractLabel extracts a label value from a Prometheus metric line.
// Example: extractLabel('container_fs_usage_bytes{fstype="ext4",pod="test"}', "fstype") returns "ext4"
func extractLabel(line string, labelName string) string {
	// Find the label in the format: labelName="value"
	searchStr := labelName + `="`
	startIdx := strings.Index(line, searchStr)
	if startIdx == -1 {
		return ""
	}

	// Move past the 'labelName="' part
	startIdx += len(searchStr)

	// Find the closing quote
	endIdx := strings.Index(line[startIdx:], `"`)
	if endIdx == -1 {
		return ""
	}

	return line[startIdx : startIdx+endIdx]
}

// extractMetricValue extracts the numeric value from a Prometheus metric line.
// Expected formats:
//   - metric_name{labels} value
//   - metric_name{labels} value timestamp
//
// The value is always the second field (parts[1])
func extractMetricValue(line string) float64 {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return -1
	}

	// Value is always the second field (first field after the metric name/labels)
	var value float64
	if _, err := fmt.Sscanf(parts[1], "%f", &value); err != nil {
		return -1
	}

	return value
}

// extractMetricTimestamp extracts the timestamp from a Prometheus metric line if present.
// Returns the timestamp in milliseconds, or 0 if not found.
func extractMetricTimestamp(line string) int64 {
	parts := strings.Fields(line)
	if len(parts) < 3 {
		return 0 // No timestamp present
	}

	// Timestamp is the third field (parts[2]) if present
	var timestamp int64
	if _, err := fmt.Sscanf(parts[2], "%d", &timestamp); err != nil {
		return 0
	}

	return timestamp
}

// IsPodReady checks if a pod has the Ready condition set to True
func (pc *PodClient) IsPodReady(ctx context.Context) (bool, error) {
	pod, err := getPod(ctx, pc.Clientset, pc.Namespace, pc.PodName)
	if err != nil {
		return false, fmt.Errorf("failed to get pod %s: %w", pc.PodName, err)
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady {
			return condition.Status == v1.ConditionTrue, nil
		}
	}

	return false, nil
}

