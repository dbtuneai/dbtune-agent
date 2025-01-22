package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

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
	// The current implementation of GetMetrics is following a concurrent collection
	// approach, where the collectors are executed in parallel and the errors are
	// collected in a channel. The channel is then closed and the results are
	// returned. Uses the errgroup package to delegate the concurrent execution.
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

	// Guardrails is responsible for triggering a signal to the DBtune server
	// that something is heading towards a failure.
	// An example failure could be memory above a certain threshold (90%)
	// or a rate of disk growth that is more than usual and not acceptable.
	// Returns nil if no guardrail is triggered, otherwise returns the type of guardrail
	Guardrails() *GuardrailType
	// SendGuardrailSignal sends a signal to the DBtune server that something is heading towards a failure.
	// The signal will be send maximum once every 15 seconds.
	SendGuardrailSignal(level GuardrailType) error

	// GetLogger returns the logger for the agent
	Logger() *logrus.Logger
}

type AgentPayload struct {
	AgentVersion   string `json:"agent_version"`
	AgentStartTime string `json:"agent_start_time"`
}

type GuardrailType string

const (
	// Critical is a guardrail that is critical to the operation of the database
	// and should be reverted immediately. This also means that the DBtune server
	// will revert to the baseline configuration to stabilise the system before recommending
	// a new configuration.
	Critical GuardrailType = "critical"
	// NonCritical is a guardrail that is not critical
	// to the operation of the database, but a new configuration
	// is recommended to be applied.
	NonCritical GuardrailType = "non_critical"
)

type GuardrailSignal struct {
	GuardrailType GuardrailType `json:"level"`
}

type IOCounterStat struct {
	ReadCount  uint64
	WriteCount uint64
}

// Caches is a struct that holds the caches for the agent
// that is updated between each metric collection beat.
// Currently, this is fixed for all adapters.
// TODO: Make this dynamic for each adapter, this could use
// GJSON and SJSON to update the cache as a string, but the locking then would be a problem in reading and writing without custom methods on state.
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

	IOCountersStat IOCounterStat

	// Hardware specific cache for guardrails
	// {
	// 	"total_memory": 1024,
	// 	"disk_size": 1024,
	// }
	HardwareCache map[string]interface{}
}

type MetricCollector struct {
	Key        string
	MetricType string
	Collector  func(ctx context.Context, state *MetricsState) error
}

type MetricsState struct {
	Collectors []MetricCollector
	// Caching layer, for metrics that are derived based on multiple heartbeats
	Cache Caches
	// Every round of metric collections this array will be filled with the metrics
	// that are collected, and then emptied
	Metrics []FlatValue
	Mutex   *sync.Mutex
}

// AddMetric appends a metric in a thread-safe way
func (state *MetricsState) AddMetric(metric FlatValue) {
	state.Mutex.Lock()
	defer state.Mutex.Unlock()
	state.Metrics = append(state.Metrics, metric)
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
	logger    *log.Logger
	APIClient *retryablehttp.Client
	// Time the agent started
	StartTime    string
	MetricsState MetricsState
}

func CreateCommonAgent() *CommonAgent {
	logger := log.New()
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	if viper.GetBool("debug") {
		logger.SetLevel(log.DebugLevel)
	} else {
		logger.SetLevel(log.InfoLevel)
	}

	serverUrl, err := CreateServerURLs()
	if err != nil {
		logger.Fatalf("Error creating server URLs: %s", err)
	}

	client := retryablehttp.NewClient()
	// 30 retries, as the cap is 30seconds for the back-off wait time
	client.RetryMax = 30
	client.Logger = &LeveledLogrus{Logger: logger}

	// Intercept the request to add the API token
	client.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
		key := req.Header.Get("DBTUNE-API-KEY")
		if key == "" {
			req.Header.Add("DBTUNE-API-KEY", serverUrl.ApiKey)
		}
	}

	return &CommonAgent{
		ServerURLs: serverUrl,
		APIClient:  client,
		logger:     logger,
		StartTime:  time.Now().UTC().Format(time.RFC3339),
	}
}

func (a *CommonAgent) Logger() *logrus.Logger {
	return a.logger
}

// SendHeartbeat sends a heartbeat to the DBtune server
// to indicate that the agent is running.
// This method does not need to be overridden by any adapter
func (a *CommonAgent) SendHeartbeat() error {
	a.Logger().Infof("Sending heartbeat to %s", a.ServerURLs.PostHeartbeat())

	payload := AgentPayload{
		AgentVersion:   "1.0.0",
		AgentStartTime: a.StartTime,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		a.Logger().Infof("Error marshaling JSON: %s", err)
		fmt.Println("Error marshaling JSON:", err)
		panic(err)
	}

	resp, err := a.APIClient.Post(a.ServerURLs.PostHeartbeat(), "application/json", bytes.NewBuffer(jsonData))

	if resp.StatusCode != 204 {
		a.Logger().Infof("Failed to send heartbeat to %s", a.ServerURLs.PostHeartbeat())
	}

	if err != nil {
		panic(err)
	}

	return nil
}

// GetMetrics will have a default implementation to handle gracefully
// error and send partial metrics rather than failing.
// It is discouraged for every adapter overriding this one.
func (a *CommonAgent) GetMetrics() ([]FlatValue, error) {
	a.Logger().Println("Staring metric collection")

	// Cleanup metrics from the previous heartbeat
	a.MetricsState.Metrics = []FlatValue{}

	// Create error group with timeout context
	// Default timeout is 20 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Channel to collect results from goroutines
	// resultsChan := make(chan []utils.FlatValue, len(a.MetricsState.Collectors))
	errorsChan := make(chan error, len(a.MetricsState.Collectors))

	// Launch collectors in parallel
	for _, collector := range a.MetricsState.Collectors {
		c := collector // Create local copy for goroutine
		g.Go(func() error {
			// Individual collector timeout
			collectorCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			done := make(chan error, 1)

			go func() {
				err := c.Collector(collectorCtx, &a.MetricsState)
				if err != nil {
					a.Logger().Error("Error in collector", c.Key, err)
					done <- fmt.Errorf("collector %s failed: %w", c.Key, err)
				}
				done <- err
			}()

			select {
			case err := <-done:
				// If the error is not nil and the error channel is not closed,
				// return the error but send it to the error channel also
				if err != nil {
					errorsChan <- err
				}
				return err
			case <-collectorCtx.Done():
				return fmt.Errorf("collector %s timed out", c.Key)
			}
		})
	}

	// Wait for all collectors or timeout
	if err := g.Wait(); err != nil {
		a.Logger().Errorln("Some collectors failed:", err)
	}

	// Collect errors before closing the channel
	var errors []error
	close(errorsChan) // Close channel after all collectors are done
	for err := range errorsChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		a.Logger().Errorln("Collector errors:")
		for _, err := range errors {
			a.Logger().Errorln(err)
		}
	}

	a.Logger().Debug("Metrics collected", a.MetricsState.Metrics)

	return a.MetricsState.Metrics, nil
}

func (a *CommonAgent) SendMetrics(metrics []FlatValue) error {
	a.Logger().Println("Sending metrics to server")

	formattedMetrics := FormatMetrics(metrics)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	a.Logger().Debug("Metrics body payload")
	a.Logger().Debug(string(jsonData))

	resp, err := a.APIClient.Post(a.ServerURLs.PostMetrics(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Debug("Failed to send metrics. Response body: ", string(body))
		return errors.New(fmt.Sprintf("Failed to send metrics, code: %d", resp.StatusCode))
	}

	return nil
}

func (a *CommonAgent) SendSystemInfo(systemInfo []FlatValue) error {
	a.Logger().Println("Sending system info to server")

	formattedMetrics := FormatSystemInfo(systemInfo)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	a.Logger().Debug("System info body payload")
	a.Logger().Debug(string(jsonData))

	req, _ := retryablehttp.NewRequest("PUT", a.ServerURLs.PostSystemInfo(), bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.APIClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Error("Failed to send system info. Response body: ", string(body))
		return errors.New(fmt.Sprintf("Failed to send syste info, code: %d", resp.StatusCode))
	}

	return nil
}

func (a *CommonAgent) SendActiveConfig(config ConfigArraySchema) error {
	a.Logger().Println("Sending active configuration to server")

	type Payload struct {
		Config ConfigArraySchema `json:"config"`
	}

	jsonData, err := json.Marshal(Payload{Config: config})
	if err != nil {
		return err
	}

	a.Logger().Debug("Configuration info body payload")
	a.Logger().Debug(string(jsonData))

	resp, err := a.APIClient.Post(a.ServerURLs.PostActiveConfig(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Error("Failed to send configuration info. Response body: ", string(body))
		return fmt.Errorf("Failed to send config info, code: %d", resp.StatusCode)
	}

	return nil
}

func (a *CommonAgent) GetProposedConfig() (*ProposedConfigResponse, error) {
	a.Logger().Println("Fetching proposed configurations")

	resp, err := a.APIClient.Get(a.ServerURLs.GetKnobRecommendations())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var proposedConfig []ProposedConfigResponse

	if err := json.Unmarshal(body, &proposedConfig); err != nil {
		return nil, err
	}

	if len(proposedConfig) > 0 {
		return &proposedConfig[0], nil
	}

	return nil, nil

}

func (a *CommonAgent) Guardrails() *GuardrailType {
	return nil
}

// SendGuardrailSignal sends a guardrail signal to the DBtune server
// that something is heading towards a failure.
func (a *CommonAgent) SendGuardrailSignal(level GuardrailType) error {
	a.Logger().Warnf("🚨 Seding Guardrail, level: %s", level)

	payload := GuardrailSignal{
		GuardrailType: level,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := a.APIClient.Post(a.ServerURLs.PostGuardrailSignal(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Error("Failed to send guardrail signal. Response body: ", string(body))
		return fmt.Errorf("failed to send guardrail signal, code: %d", resp.StatusCode)
	}

	return nil
}
