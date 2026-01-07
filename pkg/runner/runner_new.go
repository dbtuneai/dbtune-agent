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
	DefaultCollectorInterval = 5 * time.Second
	DefaultCollectorTimeout = 30 * time.Second
	DefaultHeartbeatInterval = 15 * time.Second
	DefaultSystemInfoInterval = 1 * time.Minute
	DefaultConfigInterval = 5 * time.Second
	DefaultGuardrailsCheckInterval = 1 * time.Second
	DefaultGuardrailsRateLimitInterval = 15 * time.Second
)

type Config struct {
	HeartbeatInterval              time.Duration
	SystemInfoInterval             time.Duration
	ConfigInterval                 time.Duration
	GuardrailsCheckInterval        time.Duration
	GuardrailsRateLimitInterval    time.Duration
	DefaultCollectorInterval       time.Duration
	CollectorIntervals             map[string]time.Duration

	CollectorTimeout time.Duration

	RouterBufferSize    int
	RouterFlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:              DefaultHeartbeatInterval,
		SystemInfoInterval:             DefaultSystemInfoInterval,
		ConfigInterval:                 DefaultConfigInterval,
		GuardrailsCheckInterval:        DefaultGuardrailsCheckInterval,
		GuardrailsRateLimitInterval:    DefaultGuardrailsRateLimitInterval,
		DefaultCollectorInterval:       DefaultCollectorInterval,
		CollectorIntervals:             collectorIntervals,
		CollectorTimeout:               DefaultCollectorTimeout,
		RouterBufferSize:               router.DefaultBufferSize,
		RouterFlushInterval:            router.DefaultFlushInterval,
	}
}

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
func RunnerNew(commonAgent *agent.CommonAgent, looper agent.AgentLooper) {
	logger := commonAgent.Logger()

	// TODO: Load from config file/env vars in the future
	config := DefaultConfig()
	sources := createSources(commonAgent, looper, logger, config)

	sinks := []sink.Sink{
		sink.NewDBTunePlatformSink(
			commonAgent.APIClient,
			commonAgent.ServerURLs,
			logger,
		),
	}

	routerConfig := router.Config{
		BufferSize:    config.RouterBufferSize,
		FlushInterval: config.RouterFlushInterval,
	}
	r := router.New(sources, sinks, logger, routerConfig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.Run(ctx); err != nil {
		logger.Errorf("Router error: %v", err)
	}
}

// createSources creates all sources from an adapter
func createSources(commonAgent *agent.CommonAgent, looper agent.AgentLooper, logger *logrus.Logger, config Config) []source.SourceRunner {
	sources := make([]source.SourceRunner, 0)

	sources = append(sources, source.NewHeartbeatSource(
		commonAgent.Version,
		commonAgent.StartTime,
		config.HeartbeatInterval,
		logger,
	))

	sources = append(sources, source.NewSystemInfoSource(
		looper,
		config.SystemInfoInterval,
		logger,
	))

	sources = append(sources, source.NewConfigSource(
		looper,
		config.ConfigInterval,
		logger,
	))

	sources = append(sources, source.NewGuardrailsSource(
		looper,
		config.GuardrailsCheckInterval,
		config.GuardrailsRateLimitInterval,
		logger,
	))

	for _, collector := range commonAgent.MetricsState.Collectors {
		interval := getIntervalForCollector(collector.Key, config)
		sources = append(sources, source.NewCollectorSource(
			collector.Key,
			interval,
			config.CollectorTimeout,
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
