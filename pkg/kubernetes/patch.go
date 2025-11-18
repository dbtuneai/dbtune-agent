package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
