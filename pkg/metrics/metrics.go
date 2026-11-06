package metrics

import (
	"fmt"
	"math"
	"reflect"
)

type MetricType string

const (
	Int        MetricType = "int"
	Float      MetricType = "float"
	String     MetricType = "string"
	Bytes      MetricType = "bytes"
	Percentage MetricType = "percentage"
)

// FlatValue is a struct that represents
// a flat metric value.
type FlatValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Type  MetricType  `json:"type"`
}

// NewMetric creates a new Metric object based on the provided key, value, and type.
func NewMetric(key string, value interface{}, typeStr MetricType) (FlatValue, error) {
	// Determine the type based on the provided type string
	switch typeStr {
	case "int", "bytes":
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
	default:
		return FlatValue{}, fmt.Errorf("unknown type: %s", typeStr)
	}

	return FlatValue{
		Key:   key,
		Value: value,
		Type:  typeStr,
	}, nil
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
	NodeOSPlatform    = MetricDef{Key: "system_info_platform", Type: String}
	NodeOSPlatformVer = MetricDef{Key: "system_info_platform_version", Type: String}

	// AWS RDS / Aurora
	AWSRDSParameterGroup        = MetricDef{Key: "aws_rds_parameter_group", Type: String}
	AWSRDSClusterParameterGroup = MetricDef{Key: "aws_rds_cluster_parameter_group", Type: String}

	// PG system info. Postgres statistics are sent by the catalog collectors
	// in pkg/pg/queries, not as metrics.
	PGVersion         = MetricDef{Key: "pg_version", Type: String}
	PGMaxConnections  = MetricDef{Key: "pg_max_connections", Type: Int}
	PGCurrentDatabase = MetricDef{Key: "pg_current_database", Type: String}
	PGDatabaseCount   = MetricDef{Key: "pg_database_count", Type: Int}
)
