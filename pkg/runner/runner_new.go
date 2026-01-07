package runner

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/router"
	"github.com/dbtuneai/agent/pkg/sink"
	"github.com/dbtuneai/agent/pkg/source"
	"github.com/sirupsen/logrus"
)

const (
	// DefaultCollectorInterval is the default collection interval for unknown collectors
	DefaultCollectorInterval = 5 * time.Second

	// DefaultHeartbeatInterval is the interval for sending heartbeat events
	DefaultHeartbeatInterval = 15 * time.Second

	// DefaultSystemInfoInterval is the interval for collecting system information
	DefaultSystemInfoInterval = 1 * time.Minute

	// DefaultConfigInterval is the interval for checking config changes
	DefaultConfigInterval = 5 * time.Second

	// DefaultGuardrailsCheckInterval is the interval for checking guardrails
	DefaultGuardrailsCheckInterval = 1 * time.Second

	// DefaultGuardrailsRateLimitInterval is the rate limit for guardrails signals
	DefaultGuardrailsRateLimitInterval = 15 * time.Second
)

// Config holds configuration for the runner
type Config struct {
	// Source intervals
	HeartbeatInterval              time.Duration
	SystemInfoInterval             time.Duration
	ConfigInterval                 time.Duration
	GuardrailsCheckInterval        time.Duration
	GuardrailsRateLimitInterval    time.Duration
	DefaultCollectorInterval       time.Duration
	CollectorIntervals             map[string]time.Duration

	// Router config
	RouterBufferSize    int
	RouterFlushInterval time.Duration
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:              DefaultHeartbeatInterval,
		SystemInfoInterval:             DefaultSystemInfoInterval,
		ConfigInterval:                 DefaultConfigInterval,
		GuardrailsCheckInterval:        DefaultGuardrailsCheckInterval,
		GuardrailsRateLimitInterval:    DefaultGuardrailsRateLimitInterval,
		DefaultCollectorInterval:       DefaultCollectorInterval,
		CollectorIntervals:             collectorIntervals,
		RouterBufferSize:               router.DefaultBufferSize,
		RouterFlushInterval:            router.DefaultFlushInterval,
	}
}

// collectorIntervals defines the collection interval for each collector based on how frequently they change
var collectorIntervals = map[string]time.Duration{
	// Fast-changing metrics (5s)
	"database_average_query_runtime":     5 * time.Second,
	"database_transactions_per_second":   5 * time.Second,
	"pg_active_connections":              5 * time.Second,
	"pg_idle_connections":                5 * time.Second,
	"pg_idle_in_transaction_connections": 5 * time.Second,
	"pg_autovacuum_count":                5 * time.Second,
	"pg_stat_database":                   5 * time.Second,
	"wait_events":                        5 * time.Second,
	"hardware":                           5 * time.Second,

	// Medium-changing metrics (10s)
	"pg_stat_bgwriter":     10 * time.Second,
	"pg_stat_wal":          10 * time.Second,
	"pg_stat_checkpointer": 10 * time.Second,

	// Slow-changing metrics (30-60s)
	"pg_stat_user_tables": 30 * time.Second,
	"database_size":       60 * time.Second,
	"uptime_minutes":      60 * time.Second,
}

// RunnerNew is the new channel-based runner
// It takes a pointer to CommonAgent which all adapters embed
func RunnerNew(commonAgent *agent.CommonAgent, looper agent.AgentLooper) {
	logger := commonAgent.Logger()

	// Create config with defaults
	// TODO: Load from config file/env vars in the future
	config := DefaultConfig()

	// Create all sources
	sources := createSources(commonAgent, looper, logger, config)

	// Create sinks
	sinks := []sink.Sink{
		sink.NewDBTunePlatformSink(
			commonAgent.APIClient,
			commonAgent.ServerURLs,
			logger,
		),
	}

	// Create and run router
	routerConfig := router.Config{
		BufferSize:    config.RouterBufferSize,
		FlushInterval: config.RouterFlushInterval,
	}
	r := router.New(sources, sinks, logger, routerConfig)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the router (this blocks)
	if err := r.Run(ctx); err != nil {
		logger.Errorf("Router error: %v", err)
	}
}

// createSources creates all sources from an adapter
func createSources(commonAgent *agent.CommonAgent, looper agent.AgentLooper, logger *logrus.Logger, config Config) []source.SourceRunner {
	sources := make([]source.SourceRunner, 0)

	// Add heartbeat source
	sources = append(sources, source.NewHeartbeatSource(
		commonAgent.Version,
		commonAgent.StartTime,
		config.HeartbeatInterval,
		logger,
	))

	// Add system info source
	sources = append(sources, source.NewSystemInfoSource(
		looper,
		config.SystemInfoInterval,
		logger,
	))

	// Add config source
	sources = append(sources, source.NewConfigSource(
		looper,
		config.ConfigInterval,
		logger,
	))

	// Add guardrails source
	sources = append(sources, source.NewGuardrailsSource(
		looper,
		config.GuardrailsCheckInterval,
		config.GuardrailsRateLimitInterval,
		logger,
	))

	// Add metric collector sources
	// Each collector becomes its own source with its own interval
	for _, collector := range commonAgent.MetricsState.Collectors {
		interval := getIntervalForCollector(collector.Key, config)
		sources = append(sources, source.NewCollectorSource(
			collector.Key,
			interval,
			collector.Collector,
			&commonAgent.MetricsState,
			logger,
		))
	}

	return sources
}

// getIntervalForCollector returns the appropriate interval for each collector
func getIntervalForCollector(key string, config Config) time.Duration {
	if interval, ok := config.CollectorIntervals[key]; ok {
		return interval
	}
	return config.DefaultCollectorInterval
}
