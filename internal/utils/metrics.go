package utils

import (
	"fmt"
	"reflect"
	"time"
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

// NewMetric creates a new Metric object based on the provided key, value, and type.
func NewMetric(key string, value interface{}, typeStr MetricType) (FlatValue, error) {

	// Determine the type based on the provided type string
	switch typeStr {
	case "int", "bytes", "time":
		v := reflect.ValueOf(value)
		if !(v.Kind() >= reflect.Int && v.Kind() <= reflect.Uint64) {
			return FlatValue{}, fmt.Errorf("value is not of type int")
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
