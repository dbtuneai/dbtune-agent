package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"net/http"
	"time"
)
import log "github.com/sirupsen/logrus"

type ConfigArraySchema []interface{}

type PGConfigRow struct {
	Name    string      `json:"name"`
	Setting interface{} `json:"setting"`
	Unit    interface{} `json:"unit"`
	Vartype string      `json:"vartype"`
	Context string      `json:"context"`
}

type ProposedConfigResponse struct {
	Config          []PGConfigRow `json:"config"`
	KnobsOverrides  []string      `json:"knobs_overrides"`
	KnobApplication string        `json:"knob_application"`
}

type AgentLooper interface {
	// SendHeartbeat sends a heartbeat to the DBtune server
	SendHeartbeat() error

	// GetMetrics returns the metrics for the agent
	// The metrics should have a format of:
	// {
	//   "no_cpu": { "type": "int", "value": 4 },
	//   "available_memory": { "type": "bytes", "value": 1024 },
	// }
	GetMetrics() ([]FlatValue, error)
	SendMetrics([]FlatValue) error

	// GetSystemInfo returns the system info of the PostgresSQL server
	// Example of system info:
	// {
	//   "no_cpu": { "type": "int", "value": 4 },
	//   "total_memory": { "type": "bytes", "value": 1024 },
	// }
	GetSystemInfo() ([]FlatValue, error)
	SendSystemInfo([]FlatValue) error

	GetActiveConfig() (ConfigArraySchema, error)
	SendActiveConfig(ConfigArraySchema) error
	GetProposedConfig() (*ProposedConfigResponse, error)

	// ApplyConfig applies the configuration to the PostgresSQL server
	// The configuration is applied with the appropriate method, either with a
	// restart or a reload operation
	ApplyConfig(knobs *ProposedConfigResponse) error

	// Guardrails is responsible for triggering `revert` callbacks in case
	// of failures. An example failure could be memory above a certain threshold (90%)
	// or really low tps.
	Guardrails() bool

	// Revert calls the back-end to start a revert process for a non-performing configuration
	Revert() error
}

type AgentPayload struct {
	AgentVersion   string `json:"agent_version"`
	AgentStartTime string `json:"agent_start_time"`
}

type Caches struct {
	// QueryRuntimeList is a list of all the queries in pg_stat_statements
	// The list is used to calculate the runtime of the queries
	// Example data:
	// {
	// 	example_query_id: {
	// 		"query_id": example_query_id,
	// 		"time": 1000
	// 	},
	// }
	QueryRuntimeList map[string]CachedPGStatStatement

	// XactCommit is the number of transactions committed
	// This is used to calculate the TPS between two heartbeats
	XactCommit int64
}

type MetricCollector struct {
	Key        string
	MetricType string
	Collector  func(state *MetricsState) error
}

type MetricsState struct {
	Collectors []MetricCollector
	// Caching layer, for metrics that are derived based on multiple heartbeats
	Cache Caches
	// Every round of metric collections this array will be filled with the metrics
	// that are collected, and then emptied
	Metrics []FlatValue
}

// The list below be used to remove
// default collectors with help utils

type MetricKey string

const (
	QueryRuntime      MetricKey = "query_runtime"
	ActiveConnections MetricKey = "active_connections"
)

func (state *MetricsState) RemoveKey(key MetricKey) error {
	// Remove the key from the collectors
	// Check if the state is nil
	if state.Collectors == nil {
		return errors.New("state is nil")
	}

	// Iterate over the state to find the key
	for i, mc := range state.Collectors {
		if mc.Key == string(key) {
			// Remove the MetricCollector by creating a new slice
			state.Collectors = append((state.Collectors)[:i], (state.Collectors)[i+1:]...)
			return nil // Successfully removed
		}
	}

	return nil
}

type CommonAgent struct {
	ServerURLs
	APIClient *retryablehttp.Client
	Logger    *log.Logger
	// Time the agent started
	StartTime    string
	MetricsState MetricsState
}

func CreateCommonAgent(baseURL, dbID, apiKey string) *CommonAgent {
	logger := log.New()
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetLevel(log.DebugLevel)

	client := retryablehttp.NewClient()
	// 50 retries, as the cap is 30seconds for the back-off wait time
	client.RetryMax = 50
	client.Logger = logger

	// Intercept the request to add the API token
	client.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
		req.Header.Add("DBTUNE-API-KEY", apiKey)
	}

	serverUrl, err := CreateServerURLs(baseURL, apiKey, dbID)
	if err != nil {
		logger.Fatalf("Error creating server URLs: %s", err)
	}

	return &CommonAgent{
		ServerURLs: serverUrl,
		APIClient:  client,
		Logger:     logger,
		StartTime:  time.Now().UTC().Format(time.RFC3339),
	}
}

func (a *CommonAgent) SendHeartbeat() error {
	a.Logger.Infof("Sending heartbeat to %s", a.ServerURLs.PostHeartbeat())

	payload := AgentPayload{
		AgentVersion:   "1.0.0",
		AgentStartTime: a.StartTime,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		a.Logger.Infof("Error marshaling JSON: %s", err)
		fmt.Println("Error marshaling JSON:", err)
		panic(err)
	}

	resp, err := a.APIClient.Post(a.ServerURLs.PostHeartbeat(), "application/json", bytes.NewBuffer(jsonData))

	if resp.StatusCode != 204 {
		a.Logger.Infof("Failed to send heartbeat to %s", a.ServerURLs.PostHeartbeat())
	}

	if err != nil {
		panic(err)
	}

	return nil
}

func (a *CommonAgent) Guardrails() bool {
	return false
}

func (a *CommonAgent) Revert() error {
	return nil
}
