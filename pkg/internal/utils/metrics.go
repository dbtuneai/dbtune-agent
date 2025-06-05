package utils

import (
	"fmt"
	"math"
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
		if _, ok := item.(CachedPGStatStatement); !ok {
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
