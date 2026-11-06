package agent

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/dbtuneai/agent/pkg/backend"
	"github.com/dbtuneai/agent/pkg/config"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/dbtuneai/agent/pkg/tuning"
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

type AgentLooper interface {
	// SendHeartbeat sends a heartbeat to the DBtune server
	SendHeartbeat(ctx context.Context) error

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
	GetMetrics(ctx context.Context) ([]metrics.FlatValue, error)
	SendMetrics(ctx context.Context, ms []metrics.FlatValue) error

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
	GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error)
	SendSystemInfo(ctx context.Context, systemInfo []metrics.FlatValue) error

	GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error)
	SendActiveConfig(ctx context.Context, config tuning.ConfigArraySchema, observedAt time.Time) error
	GetProposedConfig(ctx context.Context) (*tuning.ProposedConfigResponse, error)

	// ApplyConfig applies the configuration to the PostgresSQL server
	// The configuration is applied with the appropriate method, either with a
	// restart or a reload operation
	ApplyConfig(ctx context.Context, knobs *tuning.ProposedConfigResponse) tuning.ApplyConfigError

	// Guardrails is responsible for triggering a signal to the DBtune server
	// that something is heading towards a failure.
	// An example failure could be memory above a certain threshold (90%)
	// or a rate of disk growth that is more than usual and not acceptable.
	// Returns nil if no guardrail is triggered, otherwise returns the type of guardrail
	// and the metric that is monitored.
	Guardrails(ctx context.Context) *guardrails.Signal
	// SendGuardrailSignal sends a signal to the DBtune server that something is heading towards a failure.
	// The signal will be send maximum once every 15 seconds.
	SendGuardrailSignal(ctx context.Context, signal guardrails.Signal) error

	// SendError sends an error report to the DBtune server
	SendError(ctx context.Context, payload ErrorPayload) error

	// CatalogCollectors returns the list of catalog view collectors for this adapter.
	CatalogCollectors() []queries.CatalogCollector

	// SendCatalogPayload sends pre-marshaled, gzip-compressed JSON to the
	// DBtune server for the named catalog view.
	SendCatalogPayload(ctx context.Context, name string, payload []byte) error

	// GetLogger returns the logger for the agent
	Logger() *log.Logger
}

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
	IOCountersStat IOCounterStat

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

type CommonAgent struct {
	backend.ServerURLs
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
	// DBPool is the pgxpool.Pool used for database connections.
	// Set by adapters so the runner can create a HealthGate.
	DBPool *pgxpool.Pool
	// collectorPanics throttles and reports panics of the metric collectors.
	// Created on first use rather than at construction: adapters copy the
	// CommonAgent they are built from, so a guard bound here would report
	// through the pre-copy instance.
	collectorPanics *PanicGuard
}

// collectorGuard returns the guard for the metric collectors. Not safe for
// concurrent GetMetrics calls on the same agent, which already share
// MetricsState and are serialised by the runner's metrics ticker.
func (a *CommonAgent) collectorGuard() *PanicGuard {
	if a.collectorPanics == nil {
		a.collectorPanics = NewPanicGuard(a)
	}
	return a.collectorPanics
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

	serverUrl, err := backend.CreateServerURLs()
	if err != nil {
		logger.Fatalf("Error creating server URLs: %s", err)
	}

	client := retryablehttp.NewClient()
	// 30 retries, as the cap is 30seconds for the back-off wait time
	client.RetryMax = 30
	client.HTTPClient.Timeout = 4 * time.Second
	// retryablehttp logs every failed attempt at Error level. Those are
	// transient and retried, so silencing its internal logger keeps retry noise
	// off the backend log hook. The real outcome surfaces via the error Do
	// returns once retries are exhausted, logged by the caller (runWithTicker /
	// SendHeartbeat). Do guards all internal logging on a nil logger.
	client.Logger = nil

	// Intercept each attempt to add the API token and trace retries at Debug.
	client.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, attempt int) {
		key := req.Header.Get("DBTUNE-API-KEY")
		if key == "" {
			req.Header.Add("DBTUNE-API-KEY", serverUrl.ApiKey)
		}
		if attempt > 0 {
			logger.Debugf("retrying %s %s (attempt %d)", req.Method, req.URL, attempt+1)
		}
	}

	hook := utils.CreateBackendHook(serverUrl.AgentURL("raw-logs"), serverUrl.ApiKey)
	logger.AddHook(hook)

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

func (a *CommonAgent) Pool() *pgxpool.Pool {
	return a.DBPool
}

// postJSON sends a POST request with JSON body using the provided context.
func (a *CommonAgent) postJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.APIClient.Do(req)
}

// putJSON sends a PUT request with JSON body using the provided context.
func (a *CommonAgent) putJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.APIClient.Do(req)
}

// postJSONGzip sends a POST request with a gzip-compressed JSON body
// using the provided context.
func (a *CommonAgent) postJSONGzip(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	buf, err := gzipCompress(jsonData)
	if err != nil {
		return nil, err
	}
	a.Logger().Debugf("POST %s: %d bytes -> %d bytes gzipped", url, len(jsonData), buf.Len())
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	return a.APIClient.Do(req)
}

// getRequest sends a GET request using the provided context.
func (a *CommonAgent) getRequest(ctx context.Context, url string) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return a.APIClient.Do(req)
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
		AllowRestart:   config.IsRestartAllowed(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		a.Logger().Errorf("Error marshaling JSON: %s", err)
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	resp, err := a.postJSON(ctx, url, jsonData)
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

	guard := a.collectorGuard()

	// Launch collectors in parallel
	for _, collector := range a.MetricsState.Collectors {
		c := collector // Create local copy for goroutine
		wg.Add(1)

		// Guarded per collector rather than at the runner's metrics task: a
		// panicking collector becomes one collector error, so the healthy
		// collectors' samples still ship on the partial-send path, and it gets
		// the same <name>_panic report and throttling as every other task.
		collect := guard.Wrap(c.Key, func(ctx context.Context) error {
			a.Logger().Debugf("Starting collector: %s", c.Key)
			if err := c.Collector(ctx, &a.MetricsState); err != nil {
				return fmt.Errorf("collector %s failed: %w", c.Key, err)
			}
			return nil
		})

		go func() {
			defer wg.Done()

			// Individual collector timeout using configured value
			collectorCtx, cancel := context.WithTimeout(ctx, a.IndividualTimeout)
			defer cancel()

			done := make(chan error, 1)

			go func() {
				// Create copy of the context to be passed to the collector
				newCtx, cancel := context.WithCancel(collectorCtx)
				defer cancel()

				done <- collect(newCtx)
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
	var collectorErrors []error
	close(errorsChan) // Close channel after all collectors are done
	for err := range errorsChan {
		collectorErrors = append(collectorErrors, err)
	}

	a.Logger().Debug("Metrics collected", a.MetricsState.Metrics)

	if len(collectorErrors) > 0 {
		return a.MetricsState.Metrics, errors.Join(collectorErrors...)
	}
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

func (a *CommonAgent) SendActiveConfig(ctx context.Context, config tuning.ConfigArraySchema, observedAt time.Time) error {
	a.Logger().Println("Sending active configuration to server")

	type Payload struct {
		Config     tuning.ConfigArraySchema `json:"config"`
		Hash       string                   `json:"hash"`
		ObservedAt string                   `json:"observed_at"`
	}

	hash, _ := metrics.HashJSON(config)

	jsonData, err := json.Marshal(Payload{
		Config:     config,
		Hash:       hash,
		ObservedAt: observedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}

	a.Logger().Debugf("Active config payload: %s", string(jsonData))

	resp, err := a.postJSONGzip(ctx, a.ServerURLs.AgentURL("configurations"), jsonData)
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

func (a *CommonAgent) GetProposedConfig(ctx context.Context) (*tuning.ProposedConfigResponse, error) {
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

	var proposedConfig []tuning.ProposedConfigResponse

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

// catalogSendTimeout bounds a single SendCatalogPayload POST, including any
// retries performed by the retryable http client.
const catalogSendTimeout = 30 * time.Second

// gzipCompress compresses data using gzip and returns the compressed buffer.
func gzipCompress(data []byte) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	if _, err := gz.Write(data); err != nil {
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf, nil
}

// SendCatalogPayload sends pre-marshaled JSON bytes for a catalog view,
// gzip-compressed, to the DBtune server.
func (a *CommonAgent) SendCatalogPayload(ctx context.Context, name string, payload []byte) error {
	a.Logger().Printf("Sending %s payload (%d bytes)", name, len(payload))

	ctx, cancel := context.WithTimeout(ctx, catalogSendTimeout)
	defer cancel()

	resp, err := a.postJSONGzip(ctx, a.ServerURLs.AgentURL(name), payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("failed to send %s, code: %d, body: %s", name, resp.StatusCode, body)
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		a.Logger().Errorf("Failed to send error. Response body: %s", string(body))
		return fmt.Errorf("failed to send error, code: %d", resp.StatusCode)
	}

	return nil
}
