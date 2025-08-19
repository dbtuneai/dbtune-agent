package cloudsql

import (
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type QueryLabel struct {
	Name  string
	Value string
}

func GetCPUCount(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/cpu/reserved_cores", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetCPUUtilization(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/cpu/utilization", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetDiskIOPSRead(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/disk/read_ops_count", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetDiskIOPSWrite(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/disk/write_ops_count", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetDiskSize(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/disk/quota", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetDiskUsedPercentage(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/disk/utilization", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetNetworkReceiveCount(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/network/received_bytes_count", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetNetworkSentCount(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/network/sent_bytes_count", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetMemoryTotal(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/memory/quota", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetMemoryUsed(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/memory/total_usage", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func GetMemoryUsedPercentage(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/memory/utilization", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{})
}

func getMemoryComponents(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string, component string) (monitoringpb.TypedValue, error) {
	return getLatestMetricValue(cloudMonitoringClient, projectID, "cloudsql.googleapis.com/database/memory/components", []QueryLabel{{Name: "database_id", Value: fmt.Sprintf("%s:%s", projectID, databaseName)}}, []QueryLabel{{Name: "component", Value: component}})
}

type MemoryMetrics struct {
	Total               int64
	Used                int64
	Freeable            int64
	UsedPercentage      float64
	AvailablePercentage float64
}

func GetMemoryMetrics(cloudMonitoringClient *CloudMonitoringClient, projectID string, databaseName string) (MemoryMetrics, error) {
	totalMemory, err := GetMemoryTotal(cloudMonitoringClient, projectID, databaseName)
	if err != nil {
		return MemoryMetrics{}, err
	}
	usedPercentage, err := getMemoryComponents(cloudMonitoringClient, projectID, databaseName, "Usage")
	if err != nil {
		return MemoryMetrics{}, err
	}
	cachePercentage, err := getMemoryComponents(cloudMonitoringClient, projectID, databaseName, "Cache")
	if err != nil {
		return MemoryMetrics{}, err
	}
	freePercentage, err := getMemoryComponents(cloudMonitoringClient, projectID, databaseName, "Free")
	if err != nil {
		return MemoryMetrics{}, err
	}

	return MemoryMetrics{
		Total:               totalMemory.GetInt64Value(),
		Used:                int64(float64(totalMemory.GetInt64Value()) * usedPercentage.GetDoubleValue() / 100.),
		UsedPercentage:      usedPercentage.GetDoubleValue(),
		Freeable:            int64(float64(totalMemory.GetInt64Value()) * (freePercentage.GetDoubleValue() + cachePercentage.GetDoubleValue()) / 100.),
		AvailablePercentage: freePercentage.GetDoubleValue() + cachePercentage.GetDoubleValue(),
	}, nil
}

func getLatestMetricValue(cloudMonitoringClient *CloudMonitoringClient, projectID string, metricName string, resourceLabels []QueryLabel, metricLabels []QueryLabel) (monitoringpb.TypedValue, error) {
	endTime := time.Now().UTC()
	// get the last _complete_ minute of data
	startTime := endTime.Add(-10 * time.Minute)

	filter := fmt.Sprintf(`metric.type = "%s"`, metricName)

	for _, label := range resourceLabels {
		filter = fmt.Sprintf(`%s AND resource.label.%s = "%s"`, filter, label.Name, label.Value)
	}

	for _, label := range metricLabels {
		filter = fmt.Sprintf(`%s AND metric.labels.%s = "%s"`, filter, label.Name, label.Value)
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", projectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			StartTime: &timestamppb.Timestamp{Seconds: startTime.Unix()},
			EndTime:   &timestamppb.Timestamp{Seconds: endTime.Unix()},
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	}

	// TODO: better contexts
	it := cloudMonitoringClient.client.ListTimeSeries(cloudMonitoringClient.ctx, req)

	// we expect there to be exactly one result
	resp, err := it.Next()
	if err == iterator.Done {
		return monitoringpb.TypedValue{}, fmt.Errorf("Oh no, we didn't get any results!")
	}
	// TODO: what other errors can this actually return?
	if err != nil {
		log.Fatalf("Could not read time series value: %v", err)
	}

	return *resp.GetPoints()[0].GetValue(), nil
}
