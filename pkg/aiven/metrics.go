package aiven

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	aiven "github.com/aiven/go-client-codegen"
	"github.com/aiven/go-client-codegen/handler/service"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

type FetchMetricJSONData struct {
	Data struct {
		Cols []Schema `json:"cols" validate:"required"`
		Rows [][]any  `json:"rows" validate:"required"` // 1st element is Schema, rest are values with length of Cols
	} `json:"data" validate:"required"`
	MaxTimestamp int64 `json:"max_timestamp" validate:"required"` // seconds since epoch
	TotalSeries  int64 `json:"total_series"`                      // How many rows there are I guess?
	Hints        struct {
		Title string `json:"title"`
	} `json:"hints"` // Additional metadata like title
}

type FetchMetricsJSONScheme map[string]FetchMetricJSONData

// NOTE: The label is what's important, as we want to log the metrics of the master node.
//
//	[{'label': 'time', 'type': 'date'}, {'label': 'pg-1f3a86df-14 (standby)', 'type': 'number'}, {'label': 'pg-1f3a86df-15 (master)', 'type': 'number'}]
type Schema struct {
	Label string `json:"label" validate:"required"`
	Type  string `json:"type"` // "number" or "date", could be others
}

type MetricKnownName string

const (
	CPU_USAGE_KEY      MetricKnownName = "cpu_usage"
	DISK_USAGE_KEY     MetricKnownName = "disk_usage"
	DISK_IO_READ_KEY   MetricKnownName = "diskio_read"
	DISK_IO_WRITES_KEY MetricKnownName = "diskio_writes"
	LOAD_AVERAGE_KEY   MetricKnownName = "load_average"
	MEM_AVAILABLE_KEY  MetricKnownName = "mem_available"
	MEM_USAGE_KEY      MetricKnownName = "mem_usage"
	NET_RECEIVE_KEY    MetricKnownName = "net_receive"
	NET_SEND_KEY       MetricKnownName = "net_send"
)

// I'm not confident this will always be the only set of names, nor am I confident
// they will always exist.
// ['cpu_usage', 'disk_usage', 'diskio_read', 'diskio_writes', 'load_average', 'mem_available', 'mem_usage', 'net_receive', 'net_send']
// TODO(eddiebergman): I wasn't sure on the naming, as the units are missing from some
// of these metrics, and I tried to match pre-existing ones where possible.
var MetricsKnown = map[MetricKnownName]metrics.MetricDef{
	CPU_USAGE_KEY:      metrics.NodeCPUUsage,
	DISK_USAGE_KEY:     metrics.NodeDiskUsedPercentage,
	DISK_IO_READ_KEY:   metrics.NodeDiskIOPSReadPerSecond,
	DISK_IO_WRITES_KEY: metrics.NodeDiskIOPSWritePerSecond,
	LOAD_AVERAGE_KEY:   metrics.NodeLoadAverage,
	MEM_AVAILABLE_KEY:  metrics.NodeMemoryAvailablePercentage,
	MEM_USAGE_KEY:      metrics.NodeMemoryUsedPercentage,
	NET_RECEIVE_KEY:    metrics.NodeNetworkReceivePerSecond,
	NET_SEND_KEY:       metrics.NodeNetworkSendPerSecond,
}

func asKnownMetric(name string) (MetricKnownName, error) {
	if _, ok := MetricsKnown[MetricKnownName(name)]; !ok {
		return "", fmt.Errorf("unknown metric: %s", name)
	}
	return MetricKnownName(name), nil
}

// FetchedHardwareMetrics is a struct that contains the latest values for each metric
// and the maximum known timestamp. In theory the `MaximumKnownTimestamp` is the same for
// all metrics, but we store it separately for each metric as this is not gauranteed.
type ParsedMetric struct {
	Name      MetricKnownName
	Value     any
	MetricDef metrics.MetricDef
	Timestamp time.Time
}

func (p ParsedMetric) AsFlatValue() (metrics.FlatValue, error) {
	return p.MetricDef.AsFlatValue(p.Value)
}

func parseMetricForMasterNode(name MetricKnownName, data FetchMetricJSONData) (ParsedMetric, error) {
	if _, ok := MetricsKnown[name]; !ok {
		return ParsedMetric{}, fmt.Errorf("unexpected metric: %s", name)
	}

	rows := data.Data.Rows
	if len(rows) == 0 {
		return ParsedMetric{}, fmt.Errorf("no rows found for metric %s", name)
	} else if len(rows) == 1 {
		return ParsedMetric{}, fmt.Errorf("only one row found, the schema is probably wrong for metric %s", name)
	}

	// Find which index to use for pulling out data
	masterNodeIndex := -1
	for i, col := range data.Data.Cols {
		if strings.Contains(col.Label, "(master)") {
			masterNodeIndex = i
			break
		}
	}

	if masterNodeIndex == -1 {
		return ParsedMetric{}, fmt.Errorf("no master node found in schema %v for metric %s", data.Data.Cols, name)
	}

	latestData := rows[len(rows)-1]
	if len(latestData) == 0 {
		return ParsedMetric{}, fmt.Errorf("no data found in last row for metric %s", name)
	}
	latestValue := latestData[masterNodeIndex]
	if latestValue == nil {
		return ParsedMetric{}, fmt.Errorf("value for metric %s was nil", name)
	}

	return ParsedMetric{
		Name:      name,
		MetricDef: MetricsKnown[name],
		Value:     latestValue,
		Timestamp: time.Unix(data.MaxTimestamp, 0),
	}, nil
}

type MaybeParsedMetric struct {
	ParsedMetric
	Error error
}

type FetchedMetricsOut map[MetricKnownName]MaybeParsedMetric

type FetchedMetricsIn struct {
	ProjectName string
	ServiceName string
	Client      *aiven.Client
	Logger      *log.Logger
	Period      service.PeriodType
	Metrics     []MetricKnownName
}

// Fetches all metrics from Aiven and returns them as a map of metric name to ParsedMetric
// For any metrics that error, we log the error and return nil for that metric
func GetFetchedMetrics(
	ctx context.Context,
	in FetchedMetricsIn,
) (FetchedMetricsOut, error) {
	client := *in.Client
	metrics, err := client.ServiceMetricsFetch(
		ctx,
		in.ProjectName,
		in.ServiceName,
		&service.ServiceMetricsFetchIn{
			Period: service.PeriodTypeHour,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %v", err)
	}

	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metrics: %v", err)
	}

	// in.Logger.Infof("Raw metrics JSON: %s", string(metricsJSON))

	var metricData FetchMetricsJSONScheme
	if err := json.Unmarshal(metricsJSON, &metricData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metrics: %v", err)
	}

	// Create a map to store the latest values for each metric
	latestValues := make(map[MetricKnownName]MaybeParsedMetric)

	// Map the values to the struct fields
	for metricName, data := range metricData {
		// See if we know about this metric
		knownMetric, err := asKnownMetric(metricName)
		if err != nil {
			in.Logger.Infof("Unknown metric: %s, skipping", metricName)
			continue
		}

		// Validate the individual metric data structure
		if err := utils.ValidateStruct(&data); err != nil {
			in.Logger.Errorf("Validation failed for metric %s: %v", metricName, err)
			continue
		}

		parsedMetric, err := parseMetricForMasterNode(knownMetric, data)
		latestValues[knownMetric] = MaybeParsedMetric{
			ParsedMetric: parsedMetric,
			Error:        err,
		}
	}

	return latestValues, nil
}
