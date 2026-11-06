package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"time"

	"github.com/dbtuneai/agent/pkg/metrics"
)

type AgentPayload struct {
	AgentVersion   string `json:"agent_version"`
	AgentStartTime string `json:"agent_start_time"`
	AgentID        string `json:"agent_identifier"`
	AllowRestart   bool   `json:"allow_restart"`
}

type ErrorPayload struct {
	ErrorMessage string `json:"error_message"`
	ErrorType    string `json:"error_type"`
	Timestamp    string `json:"timestamp"`
}

type MetricData struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// FormattedMetrics matches the expected payload of DBtune backend
type FormattedMetrics struct {
	Version   string                 `json:"version"`
	Metrics   map[string]interface{} `json:"metrics"`
	Timestamp string                 `json:"timestamp"`
}

type FormattedSystemInfo struct {
	SystemInfo map[string]MetricData `json:"system_info"`
	Timestamp  string                `json:"timestamp"`
	Hash       string                `json:"hash"`
}

// FormatMetrics converts the MetricsState object into a FormattedMetrics object
// to be used as a metrics payload
func FormatMetrics(values []metrics.FlatValue) FormattedMetrics {
	metricsMap := make(map[string]interface{})

	for _, metric := range values {
		value := metric.Value
		if f, ok := value.(float64); ok {
			value = truncateFloat(f)
		}
		metricsMap[metric.Key] = value
	}

	return FormattedMetrics{
		Version:   "2.0",
		Metrics:   metricsMap,
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

func FormatSystemInfo(values []metrics.FlatValue) FormattedSystemInfo {
	metricsMap := make(map[string]MetricData)

	for _, metric := range values {
		value := metric.Value
		if f, ok := value.(float64); ok {
			value = truncateFloat(f)
		}
		metricsMap[metric.Key] = MetricData{
			Type:  string(metric.Type), // Assuming MetricType is a string type, adjust if necessary
			Value: value,
		}
	}

	hash, _ := HashJSON(metricsMap)

	return FormattedSystemInfo{
		SystemInfo: metricsMap,
		Timestamp:  time.Now().Format(time.RFC3339),
		Hash:       hash,
	}
}

func truncateFloat(value float64) float64 {
	return math.Round(value*1000) / 1000
}

// HashJSON computes a SHA256 hash of the JSON-serialized value.
// Go's encoding/json sorts map keys lexicographically, so the output
// is deterministic for maps with string keys and deterministic values.
func HashJSON(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}
