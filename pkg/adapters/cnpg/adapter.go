package cnpg

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/catalog"
	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/internal/kubernetes"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

const (
	// CNPG Kubernetes API constants
	CNPGAPIGroup     = "postgresql.cnpg.io"
	CNPGAPIVersion   = "v1"
	CNPGResourceName = "clusters"

	// CNPG parameters path in the Cluster CRD spec
	CNPGParametersPath = "spec.postgresql.parameters"

	// Configuration application limits
	ConfigApplyMaxRetries = 5
	ConfigApplyRetryDelay = 5 * time.Second

	// Rolling restart wait times - increased for production scenarios where
	// replicas may take longer to catch up with WAL during heavy workloads
	MaxWaitTimeClusterHealthy  = 60 * time.Minute
	PollIntervalClusterHealthy = 5 * time.Second
)

type CNPGAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	GuardrailSettings config.Guardrails
	Config            Config
	PGDriver          *pgxpool.Pool
	PGVersion         pg.Version
	K8sClient         kubernetes.Client
	State             *State
}

func CreateCNPGAdapter() (*CNPGAdapter, error) {
	// Load CNPG cfg
	cfg, err := ConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to load CNPG config: %w", err)
	}

	// Load guardrail settings
	guardrailSettings, err := config.GuardrailsFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load guardrail settings: %w", err)
	}

	// Create Kubernetes client
	client, err := kubernetes.CreateClient(cfg.KubeconfigPath, cfg.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Resolve pod name - either use explicit config or auto-discover primary
	podName := cfg.PodName
	if podName == "" {
		if cfg.ClusterName == "" {
			return nil, fmt.Errorf("either pod_name or cluster_name must be specified in CNPG config")
		}
		// Auto-discover primary pod from cluster
		ctx := context.Background()
		discoveredPod, err := client.FindCNPGPrimaryPod(ctx, cfg.ClusterName)
		if err != nil {
			return nil, fmt.Errorf("failed to auto-discover primary pod: %w", err)
		}
		podName = discoveredPod
		log.Infof("Auto-discovered primary pod: %s", podName)
	}
	// Update config with resolved pod name
	cfg.PodName = podName

	// Create PostgreSQL connection pool
	pgConfig, err := config.PostgresFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load PostgreSQL config: %w", err)
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Get PostgreSQL version
	pgVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	commonAgent.DBPool = dbpool
	adapter := &CNPGAdapter{
		CommonAgent:       *commonAgent,
		Config:            cfg,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         pgVersion,
		K8sClient:         client,
		State:             &State{LastGuardrailCheck: time.Now()},
	}

	// Initialize operations context for cancellation support
	adapter.State.CreateOperationsContext()

	collectors, err := catalog.StandardCollectors(dbpool, pgVersion, commonAgent.Logger())
	if err != nil {
		return nil, err
	}
	adapter.SetCatalogCollectors(collectors)

	// Initialize collectors
	// Use cluster name (not pod name) so metrics always come from current primary
	adapter.InitCollectors(Collectors(
		client,
		cfg.ClusterName,
		cfg.ContainerName,
	))

	return adapter, nil
}

// getClusterName returns the CNPG cluster name from config
// This is reliable even during failover when pods may be deleted/recreated
func (adapter *CNPGAdapter) getClusterName(_ context.Context) (string, error) {
	if adapter.Config.ClusterName == "" {
		return "", fmt.Errorf("cluster name not configured")
	}
	return adapter.Config.ClusterName, nil
}
