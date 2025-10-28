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
}

func (adapter *AzureFlexAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
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
		err = ApplyParameter(context.Background(), paramsClient, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, knobConfig)
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
		fmt.Printf("Error: %v", err)
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
	// TODO: the is another way we could get this, via the API - that returns _more_ parameters
	// so should probably workout what is different and what to use
	// cred, err := azidentity.NewDefaultAzureCredential(nil)
	// if err != nil {
	// 	return nil, err
	// }

	// clientFactory, err := armpostgresqlflexibleservers.NewClientFactory("a574f78e-5a47-4071-a37c-cc0199fd8e10", cred, nil)
	// if err != nil {
	// 	return nil, err
	// }

	// paramsClient := clientFactory.NewConfigurationsClient()

	// paramPager := paramsClient.NewListByServerPager("ms-flex-testing", "david-test", nil)

	// for paramPager.More() {
	// 	page, err := paramPager.NextPage(ctx)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	fmt.Printf("Config page: %v", len(page.ConfigurationListResult.Value))
	// 	for _, config := range(page.ConfigurationListResult.Value) {

	// 	}
	// }

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
	machineType := skuSegments[0]
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

	fmt.Printf("%s", sku)
	fmt.Printf("%s", machineType)
	fmt.Printf("%s", machineSeries)

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

func (*AzureFlexAdapter) Guardrails() *guardrails.Signal {
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

	common := agent.CreateCommonAgent()
	adpt := AzureFlexAdapter{
		CommonAgent:     *common,
		AzureFlexConfig: config,
		PGDriver:        pgPool,
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
		{
			Key:        "read_iops",
			MetricType: "int",
			Collector:  AsCollector(DiskReadIOPS(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeDiskIOPSReadPerSecond),
		},
		{
			Key:        "write_iops",
			MetricType: "int",
			Collector:  AsCollector(DiskWriteIOPS(adapter.AzureFlexConfig.SubscriptionID, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName), metrics.NodeDiskIOPSWritePerSecond),
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
		fmt.Println("NO PROBLEMS!!!")
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
		fmt.Println("NO PROBLEMS!!!")
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

func DiskReadIOPS(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
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
			Metricnames:     to.Ptr("read_iops"),
			Metricnamespace: to.Ptr("Microsoft.DBforPostgreSQL/flexibleServers"),
			// Aggregation:     to.Ptr("average"),
			Interval: to.Ptr("PT1M"),
			Timespan: to.Ptr(fmt.Sprintf("%s/%s", time.Now().UTC().Add(-10*time.Minute).Format("2006-01-02T15:04:05.999Z"), time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))),
		}
		resp, err := client.List(ctx, resourceURI, &opts)
		if err != nil {
			return 0.0, err
		}
		fmt.Println("NO PROBLEMS!!!")
		if len(resp.Value) == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Value was length 0")
		}
		if len(resp.Value[0].Timeseries) == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Timeseries was length 0")
		}
		dataLen := len(resp.Value[0].Timeseries[0].Data)
		if dataLen == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Data was length 0")
		}
		for i, dataPoint := range resp.Value[0].Timeseries[0].Data {
			fmt.Printf("Read IOPS: %d: %+v\n", i, *dataPoint)
		}
		result := resp.Value[0].Timeseries[0].Data[dataLen-1].Average
		if result == nil {
			return 0.0, fmt.Errorf("Metric response for IOPS: Average was nil")
		}
		return *result, nil
	}
}

func DiskWriteIOPS(subscriptionId string, resourceGroupName string, serverName string) func() (float64, error) {
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
			Metricnames:     to.Ptr("write_iops"),
			Metricnamespace: to.Ptr("Microsoft.DBforPostgreSQL/flexibleServers"),
			// Aggregation:     to.Ptr("average"),
			Interval: to.Ptr("PT1M"),
			Timespan: to.Ptr(fmt.Sprintf("%s/%s", time.Now().UTC().Add(-5*time.Minute).Format("2006-01-02T15:04:05.999Z"), time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))),
		}
		resp, err := client.List(ctx, resourceURI, &opts)
		if err != nil {
			return 0.0, err
		}
		fmt.Println("NO PROBLEMS!!!")
		if len(resp.Value) == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Value was length 0")
		}
		if len(resp.Value[0].Timeseries) == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Timeseries was length 0")
		}
		dataLen := len(resp.Value[0].Timeseries[0].Data)
		if dataLen == 0 {
			return 0.0, fmt.Errorf("Metric response for IOPS: Data was length 0")
		}
		for i, dataPoint := range resp.Value[0].Timeseries[0].Data {
			fmt.Printf("Write IOPS: %d: %+v\n", i, *dataPoint)
		}
		result := resp.Value[0].Timeseries[0].Data[dataLen-1].Average
		if result == nil {
			return 0.0, fmt.Errorf("Metric response for IOPS: Average was nil")
		}
		return *result, nil
	}
}
