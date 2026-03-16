package agent

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"reflect"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dbtuneai/agent/pkg/dbtune"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/dbtuneai/agent/pkg/version"

	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/viper"
)

// Package-level agent ID, generated once and shared across all CommonAgent instances
var (
	agentIDOnce   sync.Once
	sharedAgentID string
)

// GetAgentID returns the shared agent ID, generating it once on first call
func GetAgentID() string {
	agentIDOnce.Do(func() {
		sharedAgentID = uuid.New().String()
	})
	return sharedAgentID
}

// ConfigArraySchema is the transport type for sending configuration to the API.
type ConfigArraySchema []any

// ProposedConfigResponse represents configuration recommendations from the server.
type ProposedConfigResponse struct {
	Config          []queries.PGConfigRow `json:"config"`
	KnobsOverrides  []string              `json:"knobs_overrides"`
	KnobApplication string                `json:"knob_application"`
}

// Type aliases — canonical definitions live in pkg/pg/queries.
type PGConfigRow = queries.PGConfigRow
type CatalogCollector = queries.CatalogCollector

type AgentLooper interface {
	// SendHeartbeat sends a heartbeat to the DBtune server
	SendHeartbeat(ctx context.Context) error

	// GetMetrics returns the metrics for the agent
	GetMetrics(ctx context.Context) ([]metrics.FlatValue, error)
	SendMetrics(ctx context.Context, ms []metrics.FlatValue) error

	// GetSystemInfo returns the system info of the PostgresSQL server
	GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error)
	SendSystemInfo(ctx context.Context, systemInfo []metrics.FlatValue) error

	GetActiveConfig(ctx context.Context) (ConfigArraySchema, error)
	SendActiveConfig(ctx context.Context, config ConfigArraySchema) error
	GetProposedConfig(ctx context.Context) (*ProposedConfigResponse, error)

	// CatalogCollectors returns the list of catalog collection tasks.
	CatalogCollectors() []CatalogCollector
	// SendCatalogPayload sends an arbitrary catalog payload to the server.
	SendCatalogPayload(ctx context.Context, name string, payload any) error

	// ApplyConfig applies the configuration to the PostgresSQL server
	ApplyConfig(ctx context.Context, knobs *ProposedConfigResponse) error

	// Guardrails is responsible for triggering a signal to the DBtune server
	// that something is heading towards a failure.
	Guardrails(ctx context.Context) *guardrails.Signal
	// SendGuardrailSignal sends a signal to the DBtune server that something is heading towards a failure.
	SendGuardrailSignal(ctx context.Context, signal guardrails.Signal) error

	// SendError sends an error report to the DBtune server
	SendError(ctx context.Context, payload ErrorPayload) error

	// Logger returns the logger for the agent
	Logger() *log.Logger
}

type AgentPayload struct {
	AgentVersion   string `json:"agent_version"`
	AgentStartTime string `json:"agent_start_time"`
	AgentID        string `json:"agent_identifier"`
	AllowRestart   bool   `json:"allow_restart"`
}

type RestartNotAllowedError struct {
	Message string
}

func (e *RestartNotAllowedError) Error() string {
	return e.Message
}

func IsRestartAllowed() bool {
	return viper.GetBool("postgresql.allow_restart")
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
	client.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, _ int) {
		key := req.Header.Get("DBTUNE-API-KEY")
		if key == "" {
			req.Header.Add("DBTUNE-API-KEY", serverUrl.ApiKey)
		}
	}

	agentID := GetAgentID()
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

func (a *CommonAgent) WithLogger(logger *log.Logger) {
	a.logger = logger
}

// SendHeartbeat sends a heartbeat to the DBtune server
// to indicate that the agent is running.
// This method does not need to be overridden by any adapter
func (a *CommonAgent) SendHeartbeat(ctx context.Context) error {
	url := a.ServerURLs.AgentURL("heartbeat")
	a.Logger().Infof("Sending heartbeat to %s", url)

	payload := AgentPayload{
		AgentVersion:   a.Version,
		AgentStartTime: a.StartTime,
		AgentID:        a.AgentID,
		AllowRestart:   IsRestartAllowed(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		a.Logger().Errorf("Error marshaling JSON: %s", err)
		return err
	}

	// Add a timeout context to avoid hanging
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
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
		a.Logger().Errorf("Failed to send heartbeat to %s, status: %d, body: %s", url, resp.StatusCode, string(body))
		return fmt.Errorf("heartbeat failed with status code %d", resp.StatusCode)
	}

	return nil
}

// postJSON creates a context-aware POST request with JSON content type and executes it.
func (a *CommonAgent) postJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.APIClient.Do(req)
}

// putJSON creates a context-aware PUT request with JSON content type and executes it.
func (a *CommonAgent) putJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.APIClient.Do(req)
}

// getRequest creates a context-aware GET request and executes it.
func (a *CommonAgent) getRequest(ctx context.Context, url string) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return a.APIClient.Do(req)
}

// Should be called after creating the common agent is created to attach the collectors.
// You can also forgo this step if you create the common agent with the collectors already attached.
func (a *CommonAgent) InitCollectors(collectors []MetricCollector) {
	a.MetricsState.Collectors = collectors
}

// GetMetrics will have a default implementation to handle gracefully
// error and send partial metrics rather than failing.
// It is discouraged for every adapter overriding this one.
func (a *CommonAgent) GetMetrics(ctx context.Context) ([]metrics.FlatValue, error) {
	a.Logger().Println("Staring metric collection")

	// Cleanup metrics from the previous heartbeat
	a.MetricsState.Metrics = []metrics.FlatValue{}

	// Create context with configured timeout
	ctx, cancel := context.WithTimeout(ctx, a.CollectionTimeout)
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
				a.Logger().Warnf("collector %s did not respect context cancellation — goroutine leaked", c.Key)
			}
		}()
	}

	// Wait for all collectors to complete
	wg.Wait()

	// Collect errors before closing the channel
	var errors []error //nolint:prealloc // size unknown, collecting from channel
	close(errorsChan)  // Close channel after all collectors are done
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

func (a *CommonAgent) SendMetrics(ctx context.Context, ms []metrics.FlatValue) error {
	a.Logger().Println("Sending metrics to server")

	formattedMetrics := metrics.FormatMetrics(ms)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	resp, err := a.postJSON(ctx, a.ServerURLs.AgentURL("metrics"), jsonData)
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

func (a *CommonAgent) SendSystemInfo(ctx context.Context, systemInfo []metrics.FlatValue) error {
	a.Logger().Println("Sending system info to server")

	formattedMetrics := metrics.FormatSystemInfo(systemInfo)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	resp, err := a.putJSON(ctx, a.ServerURLs.AgentURL("system-info"), jsonData)
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

func (a *CommonAgent) SendActiveConfig(ctx context.Context, config ConfigArraySchema) error {
	a.Logger().Println("Sending active configuration to server")

	type Payload struct {
		Config ConfigArraySchema `json:"config"`
		Hash   string            `json:"hash"`
	}

	hash, _ := metrics.HashJSON(config)

	jsonData, err := json.Marshal(Payload{Config: config, Hash: hash})
	if err != nil {
		return err
	}

	a.Logger().Debugf("Active config payload: %s", string(jsonData))

	resp, err := a.postJSON(ctx, a.ServerURLs.AgentURL("configurations"), jsonData)
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

// isNilPayload checks if a payload is nil, handling both interface-nil and typed-pointer-nil.
// This is necessary because Go interfaces are nil only when both type and value are nil.
// A typed nil pointer (e.g., (*PgStatCheckpointerPayload)(nil)) passed as interface{} is NOT == nil.
func isNilPayload(payload any) bool {
	if payload == nil {
		return true
	}
	v := reflect.ValueOf(payload)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// gzipJSON marshals the payload to JSON and gzip-compresses the result.
// It returns the uncompressed JSON size (for logging) and a buffer containing
// the gzipped data.
func gzipJSON(payload any) (jsonSize int, buf *bytes.Buffer, err error) {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	buf = &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	if _, err := gz.Write(jsonData); err != nil {
		return 0, nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := gz.Close(); err != nil {
		return 0, nil, fmt.Errorf("gzip close: %w", err)
	}
	return len(jsonData), buf, nil
}

func (a *CommonAgent) SendCatalogPayload(ctx context.Context, name string, payload any) error {
	a.Logger().Printf("Sending %s to server", name)
	if isNilPayload(payload) {
		return fmt.Errorf("%s payload is nil", name)
	}
	jsonSize, buf, err := gzipJSON(payload)
	if err != nil {
		return err
	}
	a.Logger().Printf("%s payload: %d bytes -> %d bytes gzipped", name, jsonSize, buf.Len())
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	url := a.ServerURLs.AgentURL(name)
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := a.APIClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send %s. Response body: %s", name, string(body))
		return fmt.Errorf("failed to send %s, code: %d", name, resp.StatusCode)
	}
	return nil
}

func (a *CommonAgent) GetProposedConfig(ctx context.Context) (*ProposedConfigResponse, error) {
	a.Logger().Println("Fetching proposed configurations")

	resp, err := a.getRequest(ctx, a.ServerURLs.GetKnobRecommendations())
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
func (a *CommonAgent) SendGuardrailSignal(ctx context.Context, signal guardrails.Signal) error {
	a.Logger().Warnf("🚨 Sending Guardrail, level: %s, type: %s", signal.Level, signal.Type)

	jsonData, err := json.Marshal(signal)
	if err != nil {
		return err
	}

	resp, err := a.postJSON(ctx, a.ServerURLs.AgentURL("guardrails"), jsonData)
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

func (a *CommonAgent) SendError(ctx context.Context, payload ErrorPayload) error {
	a.Logger().Errorf("🚨 Sending Error Report, type: %s, message: %s", payload.ErrorType, payload.ErrorMessage)

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := a.postJSON(ctx, a.ServerURLs.AgentURL("log-entries"), jsonData)
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
