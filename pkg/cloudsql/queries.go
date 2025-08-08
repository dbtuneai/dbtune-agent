package cloudsql

import (
	"context"
	"fmt"
	"log"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func GetCPUUtilization(projectID string, databaseName string) (float64, error) {
	return getLatestMetricValue(projectID, databaseName, "cloudsql.googleapis.com/database/cpu/utilization")
}

func getLatestMetricValue(projectID string, databaseName string, metricName string) (float64, error) {
	ctx := context.Background()
	databaseID := fmt.Sprintf("%s:%s", projectID, databaseName)

	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	endTime := time.Now().UTC()
	// get the last _complete_ minute of data
	startTime := endTime.Add(-10 * time.Minute)

	filter := fmt.Sprintf(`metric.type = "%s" AND resource.label.database_id = "%s"`, metricName, databaseID)

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", projectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			StartTime: &timestamppb.Timestamp{Seconds: startTime.Unix()},
			EndTime:   &timestamppb.Timestamp{Seconds: endTime.Unix()},
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	}

	it := client.ListTimeSeries(ctx, req)

	// we expect there to be exactly one result
	resp, err := it.Next()
	if err == iterator.Done {
		return 0, fmt.Errorf("Oh no, we didn't get any results!")
	}
	// what other errors can this actually return?
	if err != nil {
		log.Fatalf("Could not read time series value: %v", err)
	}

	return resp.GetPoints()[0].GetValue().GetDoubleValue(), nil
}
