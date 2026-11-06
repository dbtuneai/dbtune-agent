package agent

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/dbtuneai/agent/pkg/backend"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/dbtuneai/agent/pkg/tuning"
	"github.com/dbtuneai/agent/pkg/version"

	"github.com/spf13/viper"
)

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
	SendError(ctx context.Context, payload backend.ErrorPayload) error

	// CatalogCollectors returns the list of catalog view collectors for this adapter.
	CatalogCollectors() []queries.CatalogCollector

	// SendCatalogPayload sends pre-marshaled, gzip-compressed JSON to the
	// DBtune server for the named catalog view.
	SendCatalogPayload(ctx context.Context, name string, payload []byte) error

	// GetLogger returns the logger for the agent
	Logger() *log.Logger
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
	// Client talks to the DBtune backend. Embedded so its Send* methods,
	// GetProposedConfig and Logger satisfy AgentLooper.
	backend.Client
	MetricsState MetricsState
	// Timeout configuration
	CollectionTimeout time.Duration // Total timeout for all collectors
	IndividualTimeout time.Duration // Timeout for each individual collector
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

	client := backend.NewClient(logger, version)

	return &CommonAgent{
		Client: *client,
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

func (a *CommonAgent) Pool() *pgxpool.Pool {
	return a.DBPool
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
