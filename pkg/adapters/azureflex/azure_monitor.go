package azureflex

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
)

type GetMostRecentAzureMetricOpts struct {
	Aggregation string
	Interval    string
}

func DefaultGetMostRecentAzureMetricOpts() GetMostRecentAzureMetricOpts {
	return GetMostRecentAzureMetricOpts{
		Aggregation: "average",
		Interval:    "PT1M",
	}
}

func GetMostRecentAzureMetric(subscriptionId string, resourceGroupName string, serverName string, metricName string, opts GetMostRecentAzureMetricOpts) (float64, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return 0.0, err
	}

	clientFactory, err := armmonitor.NewClientFactory(subscriptionId, cred, nil)
	if err != nil {
		return 0.0, err
	}

	client := clientFactory.NewMetricsClient()

	ctx := context.Background()
	resourceURI := fmt.Sprintf(
		"/subscriptions/%s/resourcegroups/%s/providers/Microsoft.DBforPostgreSQL/flexibleServers/%s",
		subscriptionId,
		resourceGroupName,
		serverName,
	)
	listOpts := armmonitor.MetricsClientListOptions{
		Metricnames:     &metricName,
		Metricnamespace: to.Ptr("Microsoft.DBforPostgreSQL/flexibleServers"),
		Aggregation:     &opts.Aggregation,
		Interval:        &opts.Interval,
		Timespan:        to.Ptr(fmt.Sprintf("%s/%s", time.Now().UTC().Add(-2*time.Minute).Format("2006-01-02T15:04:05.999Z"), time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))),
	}
	resp, err := client.List(ctx, resourceURI, &listOpts)
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
