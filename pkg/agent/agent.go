package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dbtuneai/agent/pkg/dbtune"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/version"

	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/viper"
)

type ConfigArraySchema []any

// TODO: extract PostgreSQL specific types + methods to utils/separate place
type PGConfigRow struct {
	Name    string      `json:"name"`
	Setting interface{} `json:"setting"`
	Unit    interface{} `json:"unit"`
	Vartype string      `json:"vartype"`
	Context string      `json:"context"`
}

// GetSettingValue returns the setting value in its appropriate type and format
// This is needed for cases like Aurora RDS when modifying parameters
func (p PGConfigRow) GetSettingValue() (string, error) {
	switch p.Vartype {
	case "integer":
		// Handle both string and number JSON representations
		var val int64
		switch v := p.Setting.(type) {
		case int:
			val = int64(v)
		case int64:
			val = v
		case float64:
			val = int64(v)
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return "", fmt.Errorf("failed to parse integer setting: %v", err)
			}
			val = parsed
		default:
			return "", fmt.Errorf("unexpected type for integer setting: %T", p.Setting)
		}
		return fmt.Sprintf("%d", val), nil

	case "real":
		// Handle both string and number JSON representations
		var val float64
		switch v := p.Setting.(type) {
		case float64:
			val = v
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", fmt.Errorf("failed to parse real setting: %v", err)
			}
			val = parsed
		default:
			return "", fmt.Errorf("unexpected type for real setting: %T", p.Setting)
		}
		return fmt.Sprintf("%.6g", val), nil

	case "bool":
		return fmt.Sprintf("%v", p.Setting), nil

	case "string", "enum":
		return fmt.Sprintf("%v", p.Setting), nil

	default:
		return fmt.Sprintf("%v", p.Setting), nil
	}
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
	GetMetrics() ([]metrics.FlatValue, error)
	SendMetrics([]metrics.FlatValue) error

	// GetSystemInfo returns the system info of the PostgresSQL server
	// Example of system info:
	// {
	//   "no_cpu": { "type": "int", "value": 4 },
	//   "total_memory": { "type": "bytes", "value": 1024 },
	// }
	// Importantly, you should never return a partial view of the SystemInfo, that is
	// if one step fails, you should abort and not return any metrics, just an error.
	// This is because if only a partial amount of the SystemInfo can be observed, then
	// it means that DBtune will detect this as the system information having been changed
	// and potentially abort an inprogress tuning session.
	GetSystemInfo() ([]metrics.FlatValue, error)
	SendSystemInfo([]metrics.FlatValue) error

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
	// and the metric that is monitored.
	Guardrails() *guardrails.Signal
	// SendGuardrailSignal sends a signal to the DBtune server that something is heading towards a failure.
	// The signal will be send maximum once every 15 seconds.
	SendGuardrailSignal(signal guardrails.Signal) error

	// SendError sends an error report to the DBtune server
	SendError(payload ErrorPayload) error

	// GetLogger returns the logger for the agent
	Logger() *log.Logger
}

type AgentPayload struct {
	AgentVersion   string `json:"agent_version"`
	AgentStartTime string `json:"agent_start_time"`
	AgentID        string `json:"agent_identifier"`
}

type ErrorPayload struct {
	ErrorMessage string `json:"error_message"`
	ErrorType    string `json:"error_type"`
	Timestamp    string `json:"timestamp"`
}

type IOCounterStat struct {
	ReadCount  uint64
	WriteCount uint64
}

type XactStat struct {
	Count     int64
	Timestamp time.Time
}

type BufferStat struct {
	BlksHit   int64
	BlksRead  int64
	Timestamp time.Time
}

type PGDatabase struct {
	BlksHit               int64
	BlksRead              int64
	TuplesReturned        int64
	TuplesFetched         int64
	TuplesInserted        int64
	TuplesUpdated         int64
	TuplesDeleted         int64
	TempFiles             int64
	TempBytes             int64
	Deadlocks             int64
	IdleInTransactionTime float64
	Timestamp             time.Time
}

type PGBGWriter struct {
	BuffersClean    int64
	MaxWrittenClean int64
	BuffersAlloc    int64
	Timestamp       time.Time
}

type PGWAL struct {
	WALRecords     int64
	WALFpi         int64
	WALBytes       int64
	WALBuffersFull int64
	WALWrite       int64
	WALSync        int64
	WALWriteTime   float64
	WALSyncTime    float64
	Timestamp      time.Time
}

type PGCheckPointer struct {
	NumTimed       int64
	NumRequested   int64
	WriteTime      float64
	SyncTime       float64
	BuffersWritten int64
	Timestamp      time.Time
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
	// 		"total_exec_time": 1000,
	// 		"calls": 10,
	// 	},
	// }
	QueryRuntimeList map[string]utils.CachedPGStatStatement

	// XactCommit is the number of transactions committed
	// This is used to calculate the TPS between two heartbeats
	XactCommit XactStat

	IOCountersStat IOCounterStat

	// stores metrics from pg_stat_database
	PGDatabase     PGDatabase
	PGBGWriter     PGBGWriter
	PGWAL          PGWAL
	PGCheckPointer PGCheckPointer

	PGUserTables map[string]utils.PGUserTables

	// Hardware specific cache for guardrails
	// {
	// 	"total_memory": 1024,
	// 	"disk_size": 1024,
	// }
	HardwareCache map[string]interface{}
}

type MetricCollector struct {
	Key       string
	Collector func(ctx context.Context, state *MetricsState) error
}

type MetricsState struct {
	Collectors []MetricCollector
	// Caching layer, for metrics that are derived based on multiple heartbeats
	Cache Caches
	// Every round of metric collections this array will be filled with the metrics
	// that are collected, and then emptied
	Metrics []metrics.FlatValue
	Mutex   *sync.Mutex
}

// AddMetric appends a metric in a thread-safe way
func (state *MetricsState) AddMetric(metric metrics.FlatValue) {
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

type CommonAgent struct {
	dbtune.ServerURLs
	logger    *log.Logger
	APIClient *retryablehttp.Client
	// Unique identifier for this agent instance
	AgentID string
	// Time the agent started
	StartTime    string
	MetricsState MetricsState
	// Timeout configuration
	CollectionTimeout time.Duration // Total timeout for all collectors
	IndividualTimeout time.Duration // Timeout for each individual collector
	// Version information
	Version string
}

func CreateCommonAgent() *CommonAgent {
	return CreateCommonAgentWithVersion(version.GetVersionOnly())
}

func CreateCommonAgentWithVersion(version string) *CommonAgent {
	logger := log.New()
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return "", fmt.Sprintf(" %s:%d", filename, f.Line)
		},
	})
	logger.SetReportCaller(true)

	if viper.GetBool("debug") {
		logger.SetLevel(log.DebugLevel)
	} else {
		logger.SetLevel(log.InfoLevel)
	}

	serverUrl, err := dbtune.CreateServerURLs()
	if err != nil {
		logger.Fatalf("Error creating server URLs: %s", err)
	}

	client := retryablehttp.NewClient()
	// 30 retries, as the cap is 30seconds for the back-off wait time
	client.RetryMax = 30
	client.Logger = &utils.LeveledLogrus{Logger: logger}

	// Intercept the request to add the API token
	client.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
		key := req.Header.Get("DBTUNE-API-KEY")
		if key == "" {
			req.Header.Add("DBTUNE-API-KEY", serverUrl.ApiKey)
		}
	}

	agentID := uuid.New().String()
	logger.Infof("Agent instance ID: %s", agentID)

	return &CommonAgent{
		ServerURLs: serverUrl,
		APIClient:  client,
		logger:     logger,
		AgentID:    agentID,
		StartTime:  time.Now().UTC().Format(time.RFC3339),
		Version:    version,
		MetricsState: MetricsState{
			Collectors: []MetricCollector{},
			Cache:      Caches{},
			Mutex:      &sync.Mutex{},
			Metrics:    []metrics.FlatValue{},
		},
		// Default timeouts
		CollectionTimeout: 20 * time.Second,
		IndividualTimeout: 10 * time.Second,
	}
}

func (a *CommonAgent) Logger() *log.Logger {
	return a.logger
}

// SendHeartbeat sends a heartbeat to the DBtune server
// to indicate that the agent is running.
// This method does not need to be overridden by any adapter
func (a *CommonAgent) SendHeartbeat() error {
	a.Logger().Infof("Sending heartbeat to %s", a.ServerURLs.PostHeartbeat())

	payload := AgentPayload{
		AgentVersion:   a.Version,
		AgentStartTime: a.StartTime,
		AgentID:        a.AgentID,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		a.Logger().Errorf("Error marshaling JSON: %s", err)
		return err
	}

	// Add a timeout context to avoid hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", a.ServerURLs.PostHeartbeat(), bytes.NewBuffer(jsonData))
	if err != nil {
		a.Logger().Errorf("Failed to create heartbeat request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.APIClient.Do(req)
	if err != nil {
		a.Logger().Errorf("Failed to send heartbeat: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send heartbeat to %s, status: %d, body: %s", a.ServerURLs.PostHeartbeat(), resp.StatusCode, string(body))
		return fmt.Errorf("heartbeat failed with status code %d", resp.StatusCode)
	}

	return nil
}

// Should be called after creating the common agent is created to attach the collectors.
// You can also forgo this step if you create the common agent with the collectors already attached.
func (a *CommonAgent) InitCollectors(collectors []MetricCollector) {
	a.MetricsState.Collectors = collectors
}

// GetMetrics will have a default implementation to handle gracefully
// error and send partial metrics rather than failing.
// It is discouraged for every adapter overriding this one.
func (a *CommonAgent) GetMetrics() ([]metrics.FlatValue, error) {
	a.Logger().Println("Staring metric collection")

	// Cleanup metrics from the previous heartbeat
	a.MetricsState.Metrics = []metrics.FlatValue{}

	// Create context with configured timeout
	ctx, cancel := context.WithTimeout(context.Background(), a.CollectionTimeout)
	defer cancel()

	// Use WaitGroup to wait for all collectors
	var wg sync.WaitGroup
	errorsChan := make(chan error, len(a.MetricsState.Collectors))

	// Launch collectors in parallel
	for _, collector := range a.MetricsState.Collectors {
		c := collector // Create local copy for goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Individual collector timeout using configured value
			collectorCtx, cancel := context.WithTimeout(ctx, a.IndividualTimeout)
			defer cancel()

			done := make(chan error, 1)

			go func() {
				a.Logger().Debugf("Starting collector: %s", c.Key)
				// Create copy of the context to be passed to the collector
				newCtx, cancel := context.WithCancel(collectorCtx)
				defer cancel()

				err := c.Collector(newCtx, &a.MetricsState)
				if err != nil {
					a.Logger().Errorf("Error in collector %s: %v", c.Key, err)
					done <- fmt.Errorf("collector %s failed: %w", c.Key, err)
					return
				}
				done <- nil
			}()

			select {
			case err := <-done:
				if err != nil {
					errorsChan <- err
				}
			case <-collectorCtx.Done():
				errorsChan <- fmt.Errorf("collector %s timed out", c.Key)
			}
		}()
	}

	// Wait for all collectors to complete
	wg.Wait()

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

func (a *CommonAgent) SendMetrics(ms []metrics.FlatValue) error {
	a.Logger().Println("Sending metrics to server")

	formattedMetrics := metrics.FormatMetrics(ms)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	resp, err := a.APIClient.Post(a.ServerURLs.PostMetrics(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Warnf("Failed to send metrics. Response body: %s", string(body))
		return fmt.Errorf("failed to send metrics, code: %d", resp.StatusCode)
	}

	return nil
}

func (a *CommonAgent) SendSystemInfo(systemInfo []metrics.FlatValue) error {
	a.Logger().Println("Sending system info to server")

	formattedMetrics := metrics.FormatSystemInfo(systemInfo)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	req, _ := retryablehttp.NewRequest("PUT", a.ServerURLs.PostSystemInfo(), bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.APIClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send system info. Response body: %s", string(body))
		return fmt.Errorf("failed to send system info, code: %d", resp.StatusCode)
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

	resp, err := a.APIClient.Post(a.ServerURLs.PostActiveConfig(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send configuration info. Response body: %s", string(body))
		return fmt.Errorf("failed to send config info, code: %d", resp.StatusCode)
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

// SendGuardrailSignal sends a guardrail signal to the DBtune server
// that something is heading towards a failure.
func (a *CommonAgent) SendGuardrailSignal(signal guardrails.Signal) error {
	a.Logger().Warnf("🚨 Sending Guardrail, level: %s, type: %s", signal.Level, signal.Type)

	jsonData, err := json.Marshal(signal)
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

func (a *CommonAgent) SendError(payload ErrorPayload) error {
	a.Logger().Errorf("🚨 Sending Error Report, type: %s, message: %s", payload.ErrorType, payload.ErrorMessage)

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := a.APIClient.Post(a.ServerURLs.PostError(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send error. Response body: %s", string(body))
		return fmt.Errorf("failed to send error, code: %d", resp.StatusCode)
	}

	return nil
}
