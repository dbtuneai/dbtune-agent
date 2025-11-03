package azureflex

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v5"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AzureFlexAdapter struct {
	agent.CommonAgent
	AzureFlexConfig Config
	PGDriver        *pgxpool.Pool
	GuardrailConfig guardrails.Config
}

func (adapter *AzureFlexAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return err
	}

	clientFactory, err := armpostgresqlflexibleservers.NewClientFactory(adapter.AzureFlexConfig.SubscriptionID, cred, nil)
	if err != nil {
		return err
	}

	paramsClient := clientFactory.NewConfigurationsClient()

	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return err
		}
		// we have to apply the parameters one by one, in principle each of these
		// updates could fail, if one does we bail out early and let the backend
		// realise that the wrong config has been applied and go back to the baseline
		err = ApplyParameter(ctx, paramsClient, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, knobConfig)
		if err != nil {
			return err
		}
	}

	// once all parameters have been applied, check if we need to restart the server
	if proposedConfig.KnobApplication == "restart" {
		// Restart the service
		adapter.Logger().Warn("Restarting service")
		restartResp, err := clientFactory.NewServersClient().BeginRestart(ctx, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, nil)
		if err != nil {
			return err
		}

		// wait for restart to complete
		for !(restartResp.Done()) {
			// if the network is flakey, Poll will just sit for ages, this timeout
			// stops that from happening. Since it is a polling loop it will retry
			// anyway. Obviously it could be bad if it retries indefinitely
			// We are basically assuming that at some point we will get a response
			timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			_, err := restartResp.Poll(timeoutCtx)
			if err != nil {
				fmt.Printf("Error: %v", err)
			}
		}

		_, err = restartResp.Result(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func ApplyParameter(ctx context.Context, paramsClient *armpostgresqlflexibleservers.ConfigurationsClient, resourceGroupName string, serverName string, config agent.PGConfigRow) error {
	value, err := config.GetSettingValue()
	if err != nil {
		return err
	}
	update := armpostgresqlflexibleservers.Configuration{
		Properties: &armpostgresqlflexibleservers.ConfigurationProperties{
			Source: to.Ptr("user-override"),
			Value:  &value,
		},
	}
	updateResp, err := paramsClient.BeginPut(ctx, resourceGroupName, serverName, config.Name, update, nil)
	if err != nil {
		return err
	}

	for !(updateResp.Done()) {
		// is the network is flakey, Poll will just sit for ages, this timeout
		// stops that from happening. Since it is a polling loop it will retry
		// anyway. Obviously it could be bad if it retries indefinitely
		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, err := updateResp.Poll(timeoutCtx)
		if err != nil {
			fmt.Printf("Error: %v", err)
		}

	}

	_, err = updateResp.Result(ctx)
	if err != nil {
		return err
	}
	// end of the apply cycle
	return nil
}

func (adapter *AzureFlexAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	ctx := context.Background()

	config, err := pg.GetActiveConfig(adapter.PGDriver, ctx, adapter.Logger())
	if err != nil {
		return nil, err
	}

	return config, err
}

func (adapter *AzureFlexAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	clientFactory, err := armpostgresqlflexibleservers.NewClientFactory(adapter.AzureFlexConfig.SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	serverInfo, err := clientFactory.NewServersClient().Get(context.Background(), adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, nil)
	if err != nil {
		return nil, err
	}

	// TODO: nil pointer checks!
	sku := *serverInfo.SKU.Name
	majorVersion := *serverInfo.Server.Properties.Version
	minorVersion := *serverInfo.Server.Properties.MinorVersion

	// TODO: there are probably loads of ways this horror show could break
	skuSegments := strings.Split(sku, "_")
	machineSeries := skuSegments[1][0:1]
	digitFinder, err := regexp.Compile("[0-9]+")
	nCores, err := strconv.Atoi(digitFinder.FindString(skuSegments[1]))

	var memGb int
	switch machineSeries {
	case "B":
		return nil, fmt.Errorf("B Series Machine are hard...")
	case "D":
		memGb = 4 * nCores
	case "E":
		switch nCores {
		case 96:
			memGb = 672
		case 64:
			memGb = 432
		default:
			memGb = 8 * nCores
		}
	}

	version, err := metrics.PGVersion.AsFlatValue(string(majorVersion) + "." + minorVersion)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert version: %v", err)
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(1718)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert connections: %v", err)
	}
	cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(nCores)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert cpu count: %v", err)
	}
	memoryTotalMetric, err := metrics.NodeMemoryTotal.AsFlatValue(memGb * 1024 * 1024 * 1024)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert memory total: %v", err)
	}

	systemInfo := []metrics.FlatValue{
		version,
		maxConnectionsMetric,
		cpuCountMetric,
		memoryTotalMetric,
	}
	return systemInfo, nil
}

func (adapter *AzureFlexAdapter) Guardrails() *guardrails.Signal {
	memoryUsagePercent, err := MemoryPercent(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName)()
	if err != nil {
		adapter.Logger().Errorf("Failed to get memory metric for guardrail: %v", err)
		return nil
	}

	adapter.Logger().Debugf("Memory usage: %f%%", memoryUsagePercent)

	// If memory usage is greater than 90% (default), trigger critical guardrail
	if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
		return &guardrails.Signal{
			Level: guardrails.Critical,
			Type:  guardrails.Memory,
		}
	}

	return nil
}

func CreateAzureFlexAdapter() (*AzureFlexAdapter, error) {
	ctx := context.Background()
	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	config, err := ConfigFromViper()
	if err != nil {
		return nil, err
	}

	pgPool, err := pgxpool.New(ctx, pgConfig.ConnectionURL)
	if err != nil {
		return nil, err
	}

	guardrailConfig, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	common := agent.CreateCommonAgent()
	adpt := AzureFlexAdapter{
		CommonAgent:     *common,
		AzureFlexConfig: config,
		PGDriver:        pgPool,
		GuardrailConfig: guardrailConfig,
	}

	adpt.InitCollectors(adpt.Collectors())
	return &adpt, nil
}

func (adapter *AzureFlexAdapter) Collectors() []agent.MetricCollector {
	pool := adapter.PGDriver
	collectors := []agent.MetricCollector{
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
			Key:        "cpu_utilization",
			MetricType: "float",
			Collector:  AsCollector(CPUUtilization(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeCPUUsage),
		},
		{
			Key:        "memory_used",
			MetricType: "float",
			Collector:  AsCollector(MemoryPercent(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeMemoryUsedPercentage),
		},
	}
	// majorVersion := strings.Split(adapter.PGVersion, ".")
	// intMajorVersion, err := strconv.Atoi(majorVersion[0])
	// if err != nil {
	// 	adapter.Logger().Warnf("Could not parse major version from version string %s: %v", adapter.PGVersion, err)
	// 	return collectors
	// }
	// if intMajorVersion >= 17 {
	// 	collectors = append(collectors, agent.MetricCollector{
	// 		Key:        "pg_checkpointer",
	// 		MetricType: "int",
	// 		Collector:  pg.PGStatCheckpointer(pool),
	// 	})
	// }
	return collectors
}

func AsCollector(metricGetter func() (float64, error), metric metrics.MetricDef) func(ctx context.Context, metric_state *agent.MetricsState) error {
	return func(ctx context.Context, metric_state *agent.MetricsState) error {
		metricResult, err := metricGetter()
		if err != nil {
			return err
		}
		metricValue, err := metric.AsFlatValue(metricResult)
		if err != nil {
			return err
		}
		metric_state.AddMetric(metricValue)
		return nil
	}
}

func CPUUtilization(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
	return func() (float64, error) {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return 0.0, err
		}

		clientFactory, err := armmonitor.NewClientFactory(subscriptionId, cred, nil)
		if err != nil {
			return 0.0, err
		}

		client := clientFactory.NewMetricsClient()

		ctx := context.TODO()
		resourceURI := fmt.Sprintf(
			"/subscriptions/%s/resourcegroups/%s/providers/Microsoft.DBforPostgreSQL/flexibleServers/%s",
			subscriptionId,
			resourceGroupName,
			serverName,
		)
		opts := armmonitor.MetricsClientListOptions{
			Metricnames:     to.Ptr("cpu_percent"),
			Metricnamespace: to.Ptr("Microsoft.DBforPostgreSQL/flexibleServers"),
			Aggregation:     to.Ptr("average"),
			Interval:        to.Ptr("PT1M"),
			Timespan:        to.Ptr(fmt.Sprintf("%s/%s", time.Now().UTC().Add(-2*time.Minute).Format("2006-01-02T15:04:05.999Z"), time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))),
		}
		resp, err := client.List(ctx, resourceURI, &opts)
		if err != nil {
			return 0.0, err
		}
		if len(resp.Value) == 0 {
			return 0.0, fmt.Errorf("Metric response for CPU: Value was length 0")
		}
		if len(resp.Value[0].Timeseries) == 0 {
			return 0.0, fmt.Errorf("Metric response for CPU: Timeseries was length 0")
		}
		dataLen := len(resp.Value[0].Timeseries[0].Data)
		if dataLen == 0 {
			return 0.0, fmt.Errorf("Metric response for CPU: Data was length 0")
		}
		result := resp.Value[0].Timeseries[0].Data[dataLen-1].Average
		if result == nil {
			return 0.0, fmt.Errorf("Metric response for CPU: Average was nil")
		}
		return *result, nil
	}
}

func MemoryPercent(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
	return func() (float64, error) {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return 0.0, err
		}

		clientFactory, err := armmonitor.NewClientFactory(subscriptionId, cred, nil)
		if err != nil {
			return 0.0, err
		}

		client := clientFactory.NewMetricsClient()

		ctx := context.TODO()
		resourceURI := fmt.Sprintf(
			"/subscriptions/%s/resourcegroups/%s/providers/Microsoft.DBforPostgreSQL/flexibleServers/%s",
			subscriptionId,
			resourceGroupName,
			serverName,
		)
		opts := armmonitor.MetricsClientListOptions{
			Metricnames:     to.Ptr("memory_percent"),
			Metricnamespace: to.Ptr("Microsoft.DBforPostgreSQL/flexibleServers"),
			Aggregation:     to.Ptr("average"),
			Interval:        to.Ptr("PT1M"),
			Timespan:        to.Ptr(fmt.Sprintf("%s/%s", time.Now().UTC().Add(-2*time.Minute).Format("2006-01-02T15:04:05.999Z"), time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))),
		}
		resp, err := client.List(ctx, resourceURI, &opts)
		if err != nil {
			return 0.0, err
		}
		if len(resp.Value) == 0 {
			return 0.0, fmt.Errorf("Metric response for Memory: Value was length 0")
		}
		if len(resp.Value[0].Timeseries) == 0 {
			return 0.0, fmt.Errorf("Metric response for Memory: Timeseries was length 0")
		}
		dataLen := len(resp.Value[0].Timeseries[0].Data)
		if dataLen == 0 {
			return 0.0, fmt.Errorf("Metric response for Memory: Data was length 0")
		}
		result := resp.Value[0].Timeseries[0].Data[dataLen-1].Average
		if result == nil {
			return 0.0, fmt.Errorf("Metric response for Memory: Average was nil")
		}
		return *result, nil
	}
}
