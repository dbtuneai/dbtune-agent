package runner

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/router"
	"github.com/dbtuneai/agent/pkg/sink"
	"github.com/dbtuneai/agent/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// RunnerNew is the new channel-based runner
// It takes a pointer to CommonAgent which all adapters embed
func RunnerNew(commonAgent *agent.CommonAgent, looper agent.AgentLooper) {
	logger := commonAgent.Logger()

	// Create all sources
	sources := createSources(commonAgent, looper, logger)

	// Create sinks
	sinks := make([]sink.Sink, 0)

	// Always add the DBTune platform sink
	dbtuneSink := sink.NewDBTunePlatformSink(
		commonAgent.APIClient,
		commonAgent.ServerURLs,
		logger,
	)
	sinks = append(sinks, dbtuneSink)

	// Add file sink if debug mode is enabled
	if viper.GetBool("debug") {
		fileSink, err := sink.NewFileSink("debug.log", logger)
		if err != nil {
			logger.Errorf("Failed to create file sink: %v", err)
		} else {
			logger.Info("Debug mode enabled: writing events to debug.log")
			sinks = append(sinks, fileSink)
		}
	}

	// Create and run router
	config := router.Config{
		BufferSize:    1000,
		FlushInterval: 5 * time.Second,
	}
	r := router.New(sources, sinks, logger, config)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the router (this blocks)
	if err := r.Run(ctx); err != nil {
		logger.Errorf("Router error: %v", err)
	}
}

// createSources creates all sources from an adapter
func createSources(commonAgent *agent.CommonAgent, looper agent.AgentLooper, logger *logrus.Logger) []source.Source {
	sources := make([]source.Source, 0)

	// Add heartbeat source
	sources = append(sources, source.NewHeartbeatSource(
		commonAgent.Version,
		commonAgent.StartTime,
		15*time.Second,
		logger,
	))

	// Add system info source
	sources = append(sources, source.NewSystemInfoSource(
		looper,
		1*time.Minute,
		logger,
	))

	// Add config source
	sources = append(sources, source.NewConfigSource(
		looper,
		5*time.Second,
		logger,
	))

	// Add guardrails source (check every 1s, but rate-limit signals to 15s)
	sources = append(sources, source.NewGuardrailsSource(
		looper,
		1*time.Second,
		15*time.Second,
		logger,
	))

	// Add metric collector sources
	// Each collector becomes its own source with its own interval
	for _, collector := range commonAgent.MetricsState.Collectors {
		interval := getIntervalForCollector(collector.Key)
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
// This can be made configurable later
func getIntervalForCollector(key string) time.Duration {
	// Define intervals per collector based on how frequently they change
	intervals := map[string]time.Duration{
		// Fast-changing metrics (5s)
		"database_average_query_runtime":         5 * time.Second,
		"database_transactions_per_second":       5 * time.Second,
		"pg_active_connections":                  5 * time.Second,
		"pg_idle_connections":                    5 * time.Second,
		"pg_idle_in_transaction_connections":     5 * time.Second,
		"pg_autovacuum_count":                    5 * time.Second,
		"pg_stat_database":                       5 * time.Second,
		"wait_events":                            5 * time.Second,
		"hardware":                               5 * time.Second,

		// Medium-changing metrics (10s)
		"pg_stat_bgwriter":                       10 * time.Second,
		"pg_stat_wal":                            10 * time.Second,
		"pg_stat_checkpointer":                   10 * time.Second,

		// Slow-changing metrics (30-60s)
		"pg_stat_user_tables":                    30 * time.Second,
		"database_size":                          60 * time.Second,
		"uptime_minutes":                         60 * time.Second,
	}

	if interval, ok := intervals[key]; ok {
		return interval
	}

	// Default interval for unknown collectors
	return 5 * time.Second
}
