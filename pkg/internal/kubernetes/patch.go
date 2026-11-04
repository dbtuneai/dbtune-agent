package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// CRDPatchRequest contains all parameters needed to patch a CRD resource with PostgreSQL parameters.
type CRDPatchRequest struct {
	// Context for the operation
	Ctx context.Context

	// Kubernetes client with configuration
	Client Client

	// GroupVersionResource identifying the CRD (e.g., CNPG Cluster, Percona XtraDB Cluster)
	GVR schema.GroupVersionResource

	// Name of the specific resource to patch
	ResourceName string

	// JSON path where parameters should be set (e.g., "spec.postgresql.parameters")
	ParametersPath string

	// Map of parameter names to values (all values must be strings)
	Parameters map[string]string

	// Maximum number of retry attempts for concurrent operations
	MaxRetries int

	// Delay between retry attempts
	RetryDelay time.Duration

	// Logger for operation messages
	Logger *log.Logger
}

// PatchCRDParameters applies PostgreSQL configuration parameters to a Kubernetes CRD resource.
// This is a generic function that can work with different operators (CNPG, Percona, etc.)
// by specifying the appropriate GVR and parameters path.
//
// Example for CNPG:
//
//	req := kubernetes.CRDPatchRequest{
//	    Ctx: ctx,
//	    Client: k8sClient,
//	    GVR: schema.GroupVersionResource{
//	        Group:    "postgresql.cnpg.io",
//	        Version:  "v1",
//	        Resource: "clusters",
//	    },
//	    ResourceName:   "my-cluster",
//	    ParametersPath: "spec.postgresql.parameters",
//	    Parameters:     params,
//	    MaxRetries:     5,
//	    RetryDelay:     5 * time.Second,
//	    Logger:         logger,
//	}
//	err := kubernetes.PatchCRDParameters(req)
//
// Example for Percona:
//
//	req := kubernetes.CRDPatchRequest{
//	    Ctx: ctx,
//	    Client: k8sClient,
//	    GVR: schema.GroupVersionResource{
//	        Group:    "pxc.percona.com",
//	        Version:  "v1",
//	        Resource: "perconaxtradbclusters",
//	    },
//	    ResourceName:   "my-cluster",
//	    ParametersPath: "spec.pxc.configuration",
//	    Parameters:     params,
//	    MaxRetries:     5,
//	    RetryDelay:     5 * time.Second,
//	    Logger:         logger,
//	}
//	err := kubernetes.PatchCRDParameters(req)
func PatchCRDParameters(req CRDPatchRequest) error {
	if len(req.Parameters) == 0 {
		req.Logger.Info("No parameters to apply")
		return nil
	}

	// Create dynamic client for patching CRDs
	dynamicClient, err := dynamic.NewForConfig(req.Client.Config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Build the patch payload by parsing the parameters path
	// e.g., "spec.postgresql.parameters" becomes nested map structure
	patch, err := buildNestedPatch(req.ParametersPath, req.Parameters)
	if err != nil {
		return fmt.Errorf("failed to build patch: %w", err)
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	// Apply the patch with retry logic for concurrent operations
	req.Logger.Infof("Applying patch to %s/%s %s in namespace %s",
		req.GVR.Resource, req.GVR.Version, req.ResourceName, req.Client.Namespace)
	req.Logger.Debugf("Patch payload: %s", string(patchBytes))

	for attempt := 0; attempt < req.MaxRetries; attempt++ {
		_, err = dynamicClient.Resource(req.GVR).
			Namespace(req.Client.Namespace).
			Patch(req.Ctx, req.ResourceName, types.MergePatchType, patchBytes, metav1.PatchOptions{})

		if err == nil {
			req.Logger.Info("Successfully applied configuration patch")
			return nil
		}

		// Check if error is due to concurrent operation
		if strings.Contains(err.Error(), "operation") && strings.Contains(err.Error(), "in progress") {
			req.Logger.Warnf("Resource operation in progress (attempt %d/%d), retrying in %v...",
				attempt+1, req.MaxRetries, req.RetryDelay)
			time.Sleep(req.RetryDelay)
			continue
		}

		// For other errors, fail immediately
		return fmt.Errorf("failed to patch resource: %w", err)
	}

	return fmt.Errorf("failed to patch resource after %d retries: %w", req.MaxRetries, err)
}

// TriggerCNPGRollingRestart triggers a rolling restart of a CNPG cluster by setting
// the restart annotation. This is the recommended way to apply restart-required
// PostgreSQL parameter changes.
func TriggerCNPGRollingRestart(ctx context.Context, client Client, clusterName string, logger *log.Logger) error {
	dynamicClient, err := dynamic.NewForConfig(client.Config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    "postgresql.cnpg.io",
		Version:  "v1",
		Resource: "clusters",
	}

	// Create annotation patch with current timestamp
	timestamp := time.Now().UTC().Format(time.RFC3339)
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				"kubectl.kubernetes.io/restartedAt": timestamp,
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal restart annotation patch: %w", err)
	}

	logger.Infof("Triggering rolling restart for CNPG cluster %s", clusterName)
	logger.Debugf("Restart annotation patch: %s", string(patchBytes))

	_, err = dynamicClient.Resource(gvr).
		Namespace(client.Namespace).
		Patch(ctx, clusterName, types.MergePatchType, patchBytes, metav1.PatchOptions{})

	if err != nil {
		return fmt.Errorf("failed to trigger rolling restart: %w", err)
	}

	logger.Info("Rolling restart triggered successfully")
	return nil
}

// WaitForCNPGClusterHealthy waits for a CNPG cluster to reach healthy state after rolling restart.
// It monitors the cluster's status.phase field and waits until it becomes "Cluster in healthy state".
// This is more reliable than watching individual pods because CNPG manages the restart sequence.
func WaitForCNPGClusterHealthy(ctx context.Context, client Client, clusterName string, maxWait, pollInterval time.Duration, logger *log.Logger) error {
	dynamicClient, err := dynamic.NewForConfig(client.Config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    "postgresql.cnpg.io",
		Version:  "v1",
		Resource: "clusters",
	}

	deadline := time.Now().Add(maxWait)
	lastPhase := ""
	restartDetected := false

	logger.Infof("Waiting for CNPG cluster %s to complete rolling restart (timeout: %v)...", clusterName, maxWait)

	for time.Now().Before(deadline) {
		// Get the cluster resource
		cluster, err := dynamicClient.Resource(gvr).
			Namespace(client.Namespace).
			Get(ctx, clusterName, metav1.GetOptions{})
		if err != nil {
			logger.Warnf("Failed to get cluster status: %v", err)
			time.Sleep(pollInterval)
			continue
		}

		// Extract status.phase from the unstructured object
		status, found, err := unstructured.NestedMap(cluster.Object, "status")
		if err != nil || !found {
			logger.Debug("Cluster status not found, waiting...")
			time.Sleep(pollInterval)
			continue
		}

		phase, _, _ := unstructured.NestedString(status, "phase")
		readyInstances, _, _ := unstructured.NestedInt64(status, "readyInstances")
		instances, _, _ := unstructured.NestedInt64(status, "instances")

		// Log phase changes
		if phase != lastPhase {
			logger.Infof("Cluster phase: %s (ready: %d/%d)", phase, readyInstances, instances)
			lastPhase = phase
		}

		// Detect when restart starts (cluster becomes unhealthy)
		if phase != "Cluster in healthy state" && phase != "" {
			restartDetected = true
		}

		// Check if cluster is healthy and all instances are ready
		if phase == "Cluster in healthy state" && readyInstances == instances && instances > 0 {
			if restartDetected {
				logger.Info("CNPG cluster rolling restart completed successfully")
				return nil
			}
			// If we haven't seen restart yet, keep waiting
			logger.Debug("Cluster healthy but restart not detected yet, waiting...")
		}

		time.Sleep(pollInterval)
	}

	return fmt.Errorf("CNPG cluster did not become healthy within %v timeout", maxWait)
}

// buildNestedPatch constructs a nested map structure from a dot-separated path.
// For example: "spec.postgresql.parameters" with {"key": "value"}
// becomes: {"spec": {"postgresql": {"parameters": {"key": "value"}}}}
func buildNestedPatch(path string, parameters map[string]string) (map[string]interface{}, error) {
	if path == "" {
		return nil, fmt.Errorf("parameters path cannot be empty")
	}

	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid parameters path: %s", path)
	}

	// Build the nested structure from the innermost level outward
	result := make(map[string]interface{})
	current := result

	// Navigate/create the nested structure
	for i := 0; i < len(parts)-1; i++ {
		nested := make(map[string]interface{})
		current[parts[i]] = nested
		current = nested
	}

	// Set the parameters at the leaf level
	current[parts[len(parts)-1]] = parameters

	return result, nil
}
