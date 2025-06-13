package metrics

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
)

type MetricType string

const (
	Int        MetricType = "int"
	Float      MetricType = "float"
	String     MetricType = "string"
	Bytes      MetricType = "bytes"
	Boolean    MetricType = "boolean"
	Time       MetricType = "time"
	Percentage MetricType = "percentage"
	PgssDelta  MetricType = "pgss_delta"
)

// FlatValue is a struct that represents
// a flat metric value.
type FlatValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Type  MetricType  `json:"type"`
}

type MetricData struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// FormattedMetrics matches the expected payload of DBtune backend
type FormattedMetrics struct {
	Metrics   map[string]MetricData `json:"metrics"`
	Timestamp string                `json:"timestamp"`
}

type FormattedSystemInfo struct {
	SystemInfo map[string]MetricData `json:"system_info"`
	Timestamp  string                `json:"timestamp"`
}

// validatePgssDeltaItems validates that the input is an array of CachedPGStatStatement items
func validatePgssDeltaItems(value interface{}) error {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return fmt.Errorf("value must be an array or slice")
	}

	// Check each item in the array
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i).Interface()

		// Try to convert to CachedPGStatStatement
		if _, ok := item.(utils.CachedPGStatStatement); !ok {
			return fmt.Errorf("item at index %d is not a valid CachedPGStatStatement", i)
		}
	}

	return nil
}

// NewMetric creates a new Metric object based on the provided key, value, and type.
func NewMetric(key string, value interface{}, typeStr MetricType) (FlatValue, error) {

	// Determine the type based on the provided type string
	switch typeStr {
	case "int", "bytes", "time":
		v := reflect.ValueOf(value)
		if !(v.Kind() >= reflect.Int && v.Kind() <= reflect.Uint64) {
			return FlatValue{}, fmt.Errorf("value is not of type int")
		}
		// If value is uint64, try to safely cast to int64
		if v.Kind() == reflect.Uint64 {
			u := v.Interface().(uint64)
			intVal, err := TryUint64ToInt64(u)
			if err != nil {
				return FlatValue{}, err
			}
			value = intVal
		}
	case "float", "percentage":
		if _, ok := value.(float64); !ok {
			return FlatValue{}, fmt.Errorf("value is not of type float")
		}
	case "string":
		if _, ok := value.(string); !ok {
			return FlatValue{}, fmt.Errorf("value is not of type string")
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return FlatValue{}, fmt.Errorf("value is not of type boolean")
		}
	case "pgss_delta":
		// For pgss_delta, we expect an array/slice
		v := reflect.ValueOf(value)
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return FlatValue{}, fmt.Errorf("value is not an array or slice for pgss_delta type")
		}

		// Validate array elements using the validator
		if err := validatePgssDeltaItems(value); err != nil {
			return FlatValue{}, err
		}
	default:
		return FlatValue{}, fmt.Errorf("unknown type: %s", typeStr)
	}

	return FlatValue{
		Key:   key,
		Value: value,
		Type:  typeStr,
	}, nil
}

// TODO: write util tests for this
// FormatMetrics converts the MetricsState object into a FormattedMetrics object
// to be used as a metrics payload
func FormatMetrics(metrics []FlatValue) FormattedMetrics {
	metricsMap := make(map[string]MetricData)

	for _, metric := range metrics {
		metricsMap[metric.Key] = MetricData{
			Type:  string(metric.Type), // Assuming MetricType is a string type, adjust if necessary
			Value: metric.Value,
		}
	}

	return FormattedMetrics{
		Metrics:   metricsMap,
		Timestamp: time.Now().Format(time.RFC3339Nano), // Current timestamp in RFC3339 format
	}
}

func FormatSystemInfo(metrics []FlatValue) FormattedSystemInfo {
	metricsMap := make(map[string]MetricData)

	for _, metric := range metrics {
		metricsMap[metric.Key] = MetricData{
			Type:  string(metric.Type), // Assuming MetricType is a string type, adjust if necessary
			Value: metric.Value,
		}
	}

	return FormattedSystemInfo{
		SystemInfo: metricsMap,
		Timestamp:  time.Now().Format(time.RFC3339Nano), // Current timestamp in RFC3339 format
	}
}

func TryUint64ToInt64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("value is too large to convert to int64")
	}
	return int64(value), nil
}

type MetricDef struct {
	Key  string
	Type MetricType
}

func (m MetricDef) AsFlatValue(value any) (FlatValue, error) {
	return NewMetric(m.Key, value, m.Type)
}

var (
	// CPU
	NodeCPUCount = MetricDef{Key: "node_cpu_count", Type: Int}
	NodeCPUUsage = MetricDef{Key: "node_cpu_usage", Type: Float}

	// Memory
	NodeMemoryTotal               = MetricDef{Key: "node_memory_total", Type: Bytes}
	NodeMemoryUsed                = MetricDef{Key: "node_memory_used", Type: Bytes}
	NodeMemoryUsedPercentage      = MetricDef{Key: "node_memory_used_percentage", Type: Percentage}
	NodeMemoryFreeable            = MetricDef{Key: "node_memory_freeable", Type: Bytes}
	NodeMemoryAvailablePercentage = MetricDef{Key: "node_memory_available_percentage", Type: Percentage}

	// Load
	NodeLoadAverage = MetricDef{Key: "node_load_average", Type: Float}
	// Load - NOTE: These are per second
	NodeDiskIOPSReadPerSecond  = MetricDef{Key: "node_disk_iops_read", Type: Float}
	NodeDiskIOPSWritePerSecond = MetricDef{Key: "node_disk_iops_write", Type: Float}
	NodeDiskIOPSTotalPerSecond = MetricDef{Key: "node_disk_iops_total", Type: Float}
	// Load - NOTE: These are total count, as used in adapter pgprem
	NodeDiskIOReadCount  = MetricDef{Key: "node_disk_io_ops_read", Type: Int}
	NodeDiskIOWriteCount = MetricDef{Key: "node_disk_io_ops_write", Type: Int}
	NodeDiskIOTotalCount = MetricDef{Key: "node_disk_io_ops_total", Type: Int}

	// Disk
	NodeDiskSize           = MetricDef{Key: "node_disk_size", Type: Bytes}
	NodeDiskUsedPercentage = MetricDef{Key: "node_disk_used_percentage", Type: Percentage}

	// Network
	NodeNetworkReceiveCount     = MetricDef{Key: "node_net_receive_count", Type: Int}
	NodeNetworkSendCount        = MetricDef{Key: "node_net_send_count", Type: Int}
	NodeNetworkReceivePerSecond = MetricDef{Key: "node_net_receive_per_second", Type: Float}
	NodeNetworkSendPerSecond    = MetricDef{Key: "node_net_send_per_second", Type: Float}

	// OS Info
	NodeOSInfo        = MetricDef{Key: "node_os_info", Type: String}
	NodeStorageType   = MetricDef{Key: "node_storage_type", Type: String}
	NodeOSPlatform    = MetricDef{Key: "system_info_platform", Type: String}
	NodeOSPlatformVer = MetricDef{Key: "system_info_platform_version", Type: String}

	// PG
	PGVersion                  = MetricDef{Key: "pg_version", Type: String}
	PGMaxConnections           = MetricDef{Key: "pg_max_connections", Type: Int}
	PGStatStatementsDelta      = MetricDef{Key: "pg_stat_statements_delta", Type: PgssDelta}
	PGStatStatementsDeltaCount = MetricDef{Key: "pg_stat_statements_delta_count", Type: Int}
	PGActiveConnections        = MetricDef{Key: "pg_active_connections", Type: Int}
	PGInstanceSize             = MetricDef{Key: "pg_instance_size", Type: Bytes}
	PGAutovacuumCount          = MetricDef{Key: "pg_autovacuum_count", Type: Int}
	PGCacheHitRatio            = MetricDef{Key: "pg_cache_hit_ratio", Type: Percentage}

	// Performance
	PerfAverageQueryRuntime   = MetricDef{Key: "perf_average_query_runtime", Type: Float}
	PerfTransactionsPerSecond = MetricDef{Key: "perf_transactions_per_second", Type: Float}

	// Misc
	ServerUptimeMinutes = MetricDef{Key: "server_uptime", Type: Float}
)

// One created for each type of wait event, each is Int
type PGWaitEvent struct {
	Name string
}

const PGWaitEventPrefix = "pg_wait_event_"

func (e PGWaitEvent) AsFlatValue(value int) (FlatValue, error) {
	metricName := fmt.Sprintf("%s%s", PGWaitEventPrefix, strings.ToLower(e.Name))
	return MetricDef{Key: metricName, Type: Int}.AsFlatValue(value)
}
