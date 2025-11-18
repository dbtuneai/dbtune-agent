package cnpg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/kubernetes"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// CNPG Kubernetes API constants
	CNPGAPIGroup     = "postgresql.cnpg.io"
	CNPGAPIVersion   = "v1"
	CNPGResourceName = "clusters"

	// CNPG label for cluster identification
	CNPGClusterLabel = "cnpg.io/cluster"

	// CNPG parameters path in the Cluster CRD spec
	CNPGParametersPath = "spec.postgresql.parameters"

	// Configuration application limits
	ConfigApplyMaxRetries           = 5
	ConfigApplyRetryDelay           = 5 * time.Second
	MaxWaitTimePodReadyTransitions  = 20 * time.Minute
	PollIntervalPodReadyTransitions = 2 * time.Second
)

type CNPGAdapter struct {
	agent.CommonAgent
	GuardrailSettings guardrails.Config
	Config            Config
	PGDriver          *pgxpool.Pool
	PGVersion         string
	K8sClient         kubernetes.Client
	State             *State
}

func CreateCNPGAdapter() (*CNPGAdapter, error) {
	// Load CNPG config
	config, err := ConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to load CNPG config: %w", err)
	}

	// Load guardrail settings
	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load guardrail settings: %w", err)
	}

	// Create Kubernetes client
	client, err := kubernetes.CreateClient(config.KubeconfigPath, config.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Create PostgreSQL connection pool
	pgConfig, err := pg.ConfigFromViper(nil)
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

	adapter := &CNPGAdapter{
		CommonAgent:       *agent.CreateCommonAgent(),
		Config:            config,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         pgVersion,
		K8sClient:         client,
		State:             &State{LastGuardrailCheck: time.Now()},
	}

	// Initialize collectors
	adapter.InitCollectors(Collectors(
		dbpool,
		client,
		config.PodName,
		config.ContainerName,
		pgVersion,
		adapter.Logger(),
	))

	return adapter, nil
}

func (adapter *CNPGAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	// https://cloudnative-pg.io/documentation/1.27/postgresql_conf/#changing-configuration
	ctx := context.Background()
	logger := adapter.Logger()

	// Get cluster name from pod's CNPG cluster label
	clusterName, err := adapter.getClusterName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster name: %w", err)
	}
	logger.Infof("Applying configuration to CNPG cluster: %s", clusterName)

	// Parse and validate all knobs upfront
	parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
	if err != nil {
		return fmt.Errorf("failed to parse knob configurations: %w", err)
	}

	if len(parsedKnobs) == 0 {
		logger.Info("No configuration changes to apply")
		return nil
	}

	// Build parameters map for the patch
	parametersMap := make(map[string]string)
	for _, knob := range parsedKnobs {
		// CNPG requires all parameter values to be strings
		parametersMap[knob.Name] = knob.SettingValue
		logger.Infof("Will set %s = %s", knob.Name, knob.SettingValue)
	}

	// Apply the patch using generic Kubernetes patching function
	err = kubernetes.PatchCRDParameters(kubernetes.CRDPatchRequest{
		Ctx:    ctx,
		Client: adapter.K8sClient,
		GVR: schema.GroupVersionResource{
			Group:    CNPGAPIGroup,
			Version:  CNPGAPIVersion,
			Resource: CNPGResourceName,
		},
		ResourceName:   clusterName,
		ParametersPath: CNPGParametersPath,
		Parameters:     parametersMap,
		MaxRetries:     ConfigApplyMaxRetries,
		RetryDelay:     ConfigApplyRetryDelay,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("failed to apply configuration patch: %w", err)
	}

	// Wait for pod to transition through not ready and back to ready
	// This ensures the configuration has been applied and the pod has reloaded
	logger.Info("Waiting for pod ready state transitions...")
	podClient := adapter.K8sClient.PodClient(adapter.Config.PodName)
	err = podClient.WaitForPodReadyTransitions(
		ctx,
		MaxWaitTimePodReadyTransitions,
		PollIntervalPodReadyTransitions,
		logger,
	)
	if err != nil {
		return fmt.Errorf("pod did not complete ready state transitions: %w", err)
	}

	logger.Info("Pod has completed ready state transitions - configuration applied")

	// Update state to track when we last applied config
	adapter.State.LastAppliedConfig = time.Now()

	return nil
}

// getClusterName retrieves the CNPG cluster name from the pod's labels
func (adapter *CNPGAdapter) getClusterName(ctx context.Context) (string, error) {
	pod, err := adapter.K8sClient.Clientset.CoreV1().Pods(adapter.Config.Namespace).
		Get(ctx, adapter.Config.PodName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get pod %s: %w", adapter.Config.PodName, err)
	}

	clusterName, ok := pod.Labels[CNPGClusterLabel]
	if !ok {
		return "", fmt.Errorf("pod %s does not have %s label", adapter.Config.PodName, CNPGClusterLabel)
	}

	return clusterName, nil
}

func (adapter *CNPGAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.PGDriver, context.Background(), adapter.Logger())
}

func (adapter *CNPGAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	ctx := context.Background()
	var flatValues []metrics.FlatValue

	// Get PostgreSQL version
	pgVersionMetric, err := metrics.PGVersion.AsFlatValue(adapter.PGVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG version metric: %w", err)
	}
	flatValues = append(flatValues, pgVersionMetric)

	// Get max_connections from PostgreSQL
	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, fmt.Errorf("failed to get max_connections: %w", err)
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to create max_connections metric: %w", err)
	}
	flatValues = append(flatValues, maxConnectionsMetric)

	containerClient := adapter.K8sClient.ContainerClient(adapter.Config.PodName, adapter.Config.ContainerName)
	systemInfo, err := containerClient.GetContainerSystemInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}
	flatValues = append(flatValues, systemInfo...)

	return flatValues, nil
}

func (adapter *CNPGAdapter) Guardrails() *guardrails.Signal {
	// Check if enough time has passed since the last guardrail check
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Debugf("Checking Guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	// Get pod resources to determine memory limit
	ctx := context.Background()

	containerClient := adapter.K8sClient.ContainerClient(adapter.Config.PodName, adapter.Config.ContainerName)

	memoryBytesTotal, err := containerClient.MemoryLimitBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory limit for guardrail: %v", err)
		return nil
	}
	if memoryBytesTotal == 0 {
		adapter.Logger().Errorf("Memory limit bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	memoryBytesUsage, err := containerClient.MemoryUsageBytes(ctx)
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory usage for guardrail: %v", err)
		return nil
	}

	if memoryBytesUsage == 0 {
		adapter.Logger().Errorf("Memory usage bytes is 0, something is likely wrong with monitoring")
		return nil
	}

	if memoryBytesUsage > memoryBytesTotal {
		adapter.Logger().Errorf(
			"Memory usage of %d exceeds memory limit of %d, likely something is wrong with monitoring",
			memoryBytesUsage,
			memoryBytesTotal,
		)
		// We still continue gaurdrails for safety purposes.
	}

	usedPercentage := float64(memoryBytesUsage) / float64(memoryBytesTotal) * 100

	// Check if memory usage exceeds the threshold
	if usedPercentage > adapter.GuardrailSettings.MemoryThreshold {
		adapter.Logger().Warnf(
			"Memory usage: %.2f%% is over threshold %.2f%%, triggering critical guardrail",
			usedPercentage,
			adapter.GuardrailSettings.MemoryThreshold,
		)
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func Collectors(pool *pgxpool.Pool, kubeClient kubernetes.Client, podName string, containerName string, PGVersion string, logger *log.Logger) []agent.MetricCollector {
	collectors := []agent.MetricCollector{
		// TODO: Re-enable pg_role collector once backend supports it
		// {
		// 	Key:        "postgresql_role",
		// 	MetricType: "string",
		// 	Collector:  pg.PostgreSQLRole(pool),
		// },
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  pg.PGStatStatements(pool),
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  pg.TransactionsPerSecond(pool),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  pg.ActiveConnections(pool),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  pg.DatabaseSize(pool),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  pg.Autovacuum(pool),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  pg.UptimeMinutes(pool),
		},
		{
			Key:        "pg_database",
			MetricType: "int",
			Collector:  pg.PGStatDatabase(pool),
		},
		{
			Key:        "pg_user_tables",
			MetricType: "int",
			Collector:  pg.PGStatUserTables(pool),
		},
		{
			Key:        "pg_bgwriter",
			MetricType: "int",
			Collector:  pg.PGStatBGwriter(pool),
		},
		{
			Key:        "pg_wal",
			MetricType: "int",
			Collector:  pg.PGStatWAL(pool),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  pg.WaitEvents(pool),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  kubernetes.ContainerMetricsCollector(kubeClient, podName, containerName),
		},
	}

	// Add pg_checkpointer for PostgreSQL 17+
	majorVersion := strings.Split(PGVersion, ".")
	if len(majorVersion) > 0 {
		intMajorVersion, err := strconv.Atoi(majorVersion[0])
		if err != nil {
			logger.Warn("Failed to parse PostgreSQL major version, skipping pg_checkpointer collector", "version", PGVersion, "error", err)
		} else if intMajorVersion >= 17 {
			collectors = append(collectors, agent.MetricCollector{
				Key:        "pg_checkpointer",
				MetricType: "int",
				Collector:  pg.PGStatCheckpointer(pool),
			})
		}
	}

	return collectors
}
