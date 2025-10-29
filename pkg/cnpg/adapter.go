package cnpg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/kubernetes"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
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
	// TODO: Implement configuration application for CNPG
	return nil
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
