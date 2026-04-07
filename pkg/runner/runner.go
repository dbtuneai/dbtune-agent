package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// isRecoveryError checks if an error indicates the system is in recovery/failover
func isRecoveryError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "failover detected") ||
		strings.Contains(errMsg, "recovery in progress")
}

func runWithTicker(ctx context.Context, ticker *time.Ticker, name string, logger *logrus.Logger, skipFirst bool, fn func() error) {
	// Run immediately
	if !skipFirst {
		if err := fn(); err != nil {
			logger.Errorf("initial %s error: %v", name, err)
		}
	}
	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(); err != nil {
				logger.Errorf("%s error: %v", name, err)
			}
		}
	}
}

// withHealthGate wraps a function with health gate check and error reporting.
// If the gate is closed (database unreachable), fn is skipped. If fn returns
// an error, the gate inspects it for connection-level failures. Recovery errors
// are suppressed (returned as nil) to avoid noisy logging in runWithTicker.
func withHealthGate(hg *agent.HealthGate, logger *logrus.Logger, fn func() error) error {
	if hg.IsClosed() {
		logger.Debugf("Skipping, health gate closed")
		return nil
	}
	err := fn()
	if err != nil {
		hg.ReportError(err)
		if isRecoveryError(err) {
			logger.Debugf("Skipping during recovery: %v", err)
			return nil
		}
	}
	return err
}

// runFileCollectors starts a goroutine per catalog collector, each writing one
// JSON line per tick to <output_dir>/<collector_name>.ndjson.
//
// File rotation: files are opened once at startup and appended to for the
// lifetime of the process. They grow without bound — rotation (e.g. logrotate
// or a size-based truncation strategy) should be handled externally.
//
// includeQueries / PgStatsCollector mapping: postgresql.include_queries controls
// whether query text is included in pg_stat_statements. It is also passed as
// includeTableData to PgStatsCollector, which uses it to gate column-level
// statistics — the semantics differ and these should be split into separate
// config keys in a future pass.
func runFileCollectors(ctx context.Context, adapter agent.AgentLooper, hg *agent.HealthGate, logger *logrus.Logger) {
	outputDir := fileCollectorOutputDir()
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Errorf("file collectors: cannot create output dir %s: %v", outputDir, err)
		return
	}

	// Query PG major version once; version-gated collectors silently skip if 0.
	pgMajorVersion, err := queries.QueryPGMajorVersion(adapter.Pool(), context.Background())
	if err != nil {
		logger.Warnf("file collectors: failed to detect PG version (%v); version-gated collectors disabled", err)
		pgMajorVersion = 0
	}

	// Read pg_stat_statements params from viper (set by pkg/pg/config.go defaults).
	includeQueries := viper.GetBool("postgresql.include_queries")
	maxQueryTextLength := viper.GetInt("postgresql.maximum_query_text_length")

	prepareCtx := queries.PrepareCtx(adapter.PrepareCtx())
	pool := adapter.Pool()

	allCollectors := []queries.CatalogCollector{
		queries.AutovacuumCountCollector(pool, prepareCtx),
		queries.ConnectionStatsCollector(pool, prepareCtx),
		queries.DatabaseSizeCollector(pool, prepareCtx),
		queries.PgAttributeCollector(pool, prepareCtx),
		queries.PgClassCollector(pool, prepareCtx, queries.PgClassBackfillBatchSize),
		queries.PgIndexCollector(pool, prepareCtx),
		queries.PgLocksCollector(pool, prepareCtx),
		queries.PgPreparedXactsCollector(pool, prepareCtx),
		queries.PgReplicationSlotsCollector(pool, prepareCtx),
		queries.PgStatActivityCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatArchiverCollector(pool, prepareCtx),
		queries.PgStatBgwriterCollector(pool, prepareCtx),
		queries.PgStatCheckpointerCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatDatabaseCollector(pool, prepareCtx),
		queries.PgStatDatabaseConflictsCollector(pool, prepareCtx),
		queries.PgStatIOCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatProgressAnalyzeCollector(pool, prepareCtx),
		queries.PgStatProgressCreateIndexCollector(pool, prepareCtx),
		queries.PgStatProgressVacuumCollector(pool, prepareCtx),
		queries.PgStatRecoveryPrefetchCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatReplicationCollector(pool, prepareCtx),
		queries.PgStatReplicationSlotsCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatSlruCollector(pool, prepareCtx),
		queries.PgStatStatementsCollector(pool, prepareCtx, includeQueries, maxQueryTextLength, queries.PgStatStatementsDiffLimit, pgMajorVersion),
		queries.PgStatSubscriptionCollector(pool, prepareCtx),
		queries.PgStatSubscriptionStatsCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatUserFunctionsCollector(pool, prepareCtx),
		queries.PgStatUserIndexesCollector(pool, prepareCtx, queries.PgStatUserIndexesCategoryLimit),
		queries.PgStatUserTablesCollector(pool, prepareCtx, queries.PgStatUserTablesCategoryLimit),
		queries.PgStatWalCollector(pool, prepareCtx, pgMajorVersion),
		queries.PgStatWalReceiverCollector(pool, prepareCtx),
		queries.PgStatioUserIndexesCollector(pool, prepareCtx, queries.PgStatioUserIndexesBatchSize),
		queries.PgStatioUserTablesCollector(pool, prepareCtx),
		queries.PgStatsCollector(pool, prepareCtx, queries.PgStatsBackfillBatchSize, includeQueries),
		queries.TransactionCommitsCollector(pool, prepareCtx),
		queries.UptimeMinutesCollector(pool, prepareCtx),
		queries.WaitEventsCollector(pool, prepareCtx),
	}

	for _, c := range allCollectors {
		c := c // capture loop variable
		filePath := filepath.Join(outputDir, c.Name+".ndjson")
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Errorf("file collectors: failed to open %s: %v", filePath, err)
			continue
		}
		ticker := time.NewTicker(c.Interval)
		go func() {
			defer ticker.Stop()
			defer f.Close()
			runWithTicker(ctx, ticker, c.Name, logger, false, func() error {
				return withHealthGate(hg, logger, func() error {
					result, err := c.Collect(context.Background())
					if err != nil {
						return err
					}
					if result == nil {
						// nil = unchanged (SkipUnchanged) or version-gated; skip write
						return nil
					}
					_, err = f.Write(append(result.JSON, '\n'))
					return err
				})
			})
		}()
	}
}

// Runner is the main entry point for the agent
// that executes the different tasks
func Runner(adapter agent.AgentLooper) {
	logger := adapter.Logger()

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a HealthGate to short-circuit DB-hitting calls when the database is unreachable.
	hg := agent.NewHealthGate(ctx, adapter.Pool(), pg.IsConnectionError, logger)

	go runFileCollectors(ctx, adapter, hg, logger)

	// Create tickers for different intervals
	metricsTicker := time.NewTicker(5 * time.Second)
	systemMetricsTicker := time.NewTicker(1 * time.Minute)
	configTicker := time.NewTicker(5 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)
	guardrailTicker := time.NewTicker(1 * time.Second)

	// Heartbeat goroutine
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, true, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			data, err := adapter.GetMetrics()
			if err != nil {
				// Send partial data even when some collectors failed.
				if len(data) > 0 {
					if sendErr := adapter.SendMetrics(data); sendErr != nil {
						logger.Errorf("failed to send partial metrics: %v", sendErr)
					}
				}
				return fmt.Errorf("failed to collect metrics: %w", err)
			}
			return adapter.SendMetrics(data)
		})
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, systemMetricsTicker, "system info", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			data, err := adapter.GetSystemInfo()
			if err != nil {
				return fmt.Errorf("failed to collect system information: %w", err)
			}
			return adapter.SendSystemInfo(data)
		})
	})

	// Config management goroutine
	go runWithTicker(ctx, configTicker, "config", logger, false, func() error {
		return withHealthGate(hg, logger, func() error {
			config, err := adapter.GetActiveConfig()
			if err != nil {
				return fmt.Errorf("failed to get active configuration: %w", err)
			}

			// Check for proposed configs BEFORE sending current config
			// This is critical after failover recovery - we need to apply baseline first
			proposedConfig, err := adapter.GetProposedConfig()
			if err != nil {
				return err
			}
			if proposedConfig != nil {
				err := adapter.ApplyConfig(proposedConfig)
				if err != nil {
					errorType := "config_apply_error"
					var restartErr *agent.RestartNotAllowedError
					if errors.As(err, &restartErr) {
						errorType = "restart_not_allowed"
					}
					errorPayload := agent.ErrorPayload{
						ErrorMessage: "Failed to apply configuration: " + err.Error(),
						ErrorType:    errorType,
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					}
					if sendErr := adapter.SendError(errorPayload); sendErr != nil {
						logger.Errorf("failed to send error report: %v", sendErr)
					}
					return err
				}

				// Re-fetch config after applying proposed config
				// This ensures we send the newly applied config
				config, err = adapter.GetActiveConfig()
				if err != nil {
					return err
				}
			}

			return adapter.SendActiveConfig(config)
		})
	})

	// Guardrail check goroutine
	// Time is kept in a pointer to keep a persistent reference
	// May need to refactor this for testing
	var lastCheck *time.Time
	go runWithTicker(ctx, guardrailTicker, "guardrail", logger, false, func() error {
		if lastCheck != nil && time.Since(*lastCheck) < 15*time.Second {
			return nil
		}
		signal := adapter.Guardrails()
		if signal != nil {
			if err := adapter.SendGuardrailSignal(*signal); err != nil {
				now := time.Now()
				lastCheck = &now
				return err
			}
			now := time.Now()
			lastCheck = &now
		}
		return nil
	})

	// Block forever
	select {}
}
