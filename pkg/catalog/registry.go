package catalog

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/capabilities"
	"github.com/dbtuneai/agent/pkg/catalog/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// noopPrepareCtx is a PrepareCtx that passes through unchanged.
func noopPrepareCtx(ctx context.Context) (context.Context, error) { return ctx, nil }

// BuildCollectors creates the full set of catalog collectors, applying
// per-collector config from CollectorsConfig (enabled filter, interval overrides).
// A nil pool is allowed for construction-only callers (e.g. tests); the pointer
// is only dereferenced when a collector's Collect is invoked.
//
// The capabilities collector runs the Postgres checker, then extraCheckers.
func BuildCollectors(
	pool *pgxpool.Pool,
	pgMajorVersion int,
	cfg CollectorsConfig,
	prepareCtx PrepareCtx,
	logger *logrus.Logger,
	extraCheckers ...capabilities.CapabilityChecker,
) ([]Collector, error) {
	if prepareCtx == nil {
		prepareCtx = noopPrepareCtx
	}
	if logger == nil {
		logger = logrus.StandardLogger()
	}

	type entry struct {
		collector Collector
		base      collectorconfig.BaseConfig
	}

	simpleBase := func(name string) collectorconfig.BaseConfig {
		if b, ok := cfg.Simple[name]; ok {
			return b
		}
		return collectorconfig.BaseConfig{}
	}

	checkers := append([]capabilities.CapabilityChecker{PostgresCapabilityChecker(pool, prepareCtx)}, extraCheckers...)

	entries := []entry{
		// Simple collectors
		{AutovacuumCountCollector(pool, prepareCtx), simpleBase(AutovacuumCountName)},
		{CapabilitiesCollector(logger, checkers...), simpleBase(CapabilitiesName)},
		{ConnectionStatsCollector(pool, prepareCtx), simpleBase(ConnectionStatsName)},
		{DDLCollector(pool, prepareCtx), simpleBase(DDLCollectorName)},
		{DatabaseSizeCollector(pool, prepareCtx), simpleBase(DatabaseSizeName)},
		{PgAttributeCollector(pool, prepareCtx), simpleBase(PgAttributeName)},
		{PgConstraintCollector(pool, prepareCtx), simpleBase(PgConstraintName)},
		{PgDatabaseCollector(pool, prepareCtx), simpleBase(PgDatabaseName)},
		{PgIndexCollector(pool, prepareCtx), simpleBase(PgIndexName)},
		{PgIndexInventoryCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgIndexInventoryName)},
		{PgLocksCollector(pool, prepareCtx), simpleBase(PgLocksName)},
		{PgPreparedXactsCollector(pool, prepareCtx), simpleBase(PgPreparedXactsName)},
		{PgReplicationSlotsCollector(pool, prepareCtx), simpleBase(PgReplicationSlotsName)},
		{PgTypeCollector(pool, prepareCtx), simpleBase(PgTypeName)},
		{PgStatActivityCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatActivityName)},
		{PgStatArchiverCollector(pool, prepareCtx), simpleBase(PgStatArchiverName)},
		{PgStatBgwriterCollector(pool, prepareCtx), simpleBase(PgStatBgwriterName)},
		{PgStatCheckpointerCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatCheckpointerName)},
		{PgStatDatabaseCollector(pool, prepareCtx), simpleBase(PgStatDatabaseName)},
		{PgStatDatabaseConflictsCollector(pool, prepareCtx), simpleBase(PgStatDatabaseConflictsName)},
		{PgStatIOCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatIOName)},
		{PgStatProgressAnalyzeCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatProgressAnalyzeName)},
		{PgStatProgressCreateIndexCollector(pool, prepareCtx), simpleBase(PgStatProgressCreateIndexName)},
		{PgStatProgressVacuumCollector(pool, prepareCtx), simpleBase(PgStatProgressVacuumName)},
		{PgStatRecoveryPrefetchCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatRecoveryPrefetchName)},
		{PgStatReplicationCollector(pool, prepareCtx), simpleBase(PgStatReplicationName)},
		{PgStatReplicationSlotsCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatReplicationSlotsName)},
		{PgStatSlruCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatSlruName)},
		{PgStatSubscriptionCollector(pool, prepareCtx), simpleBase(PgStatSubscriptionName)},
		{PgStatSubscriptionStatsCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatSubscriptionStatsName)},
		{PgStatUserFunctionsCollector(pool, prepareCtx), simpleBase(PgStatUserFunctionsName)},
		{PgStatWalCollector(pool, prepareCtx, pgMajorVersion), simpleBase(PgStatWalName)},
		{PgStatWalReceiverCollector(pool, prepareCtx), simpleBase(PgStatWalReceiverName)},
		{PgStatioUserTablesCollector(pool, prepareCtx), simpleBase(PgStatioUserTablesName)},
		{TransactionCommitsCollector(pool, prepareCtx, pgMajorVersion), simpleBase(TransactionCommitsName)},
		{UptimeMinutesCollector(pool, prepareCtx, logger), simpleBase(UptimeMinutesName)},
		{WaitEventsCollector(pool, prepareCtx), simpleBase(WaitEventsName)},

		// Typed collectors
		{PgStatStatementsCollector(pool, prepareCtx, cfg.PgStatStatements.Extra, logger), cfg.PgStatStatements.Base},
		{PgClassCollector(pool, prepareCtx, cfg.PgClass.Extra, pgMajorVersion), cfg.PgClass.Base},
		{PgStatsCollector(pool, prepareCtx, cfg.PgStats.Extra), cfg.PgStats.Base},
		{PgStatUserTablesCollector(pool, prepareCtx, cfg.PgStatUserTables.Extra), cfg.PgStatUserTables.Base},
		{PgStatUserIndexesCollector(pool, prepareCtx, cfg.PgStatUserIndexes.Extra), cfg.PgStatUserIndexes.Base},
		{PgStatioUserIndexesCollector(pool, prepareCtx, cfg.PgStatioUserIndexes.Extra), cfg.PgStatioUserIndexes.Base},
	}

	result := make([]Collector, 0, len(entries))
	for _, e := range entries {
		c := e.collector
		keep, err := applyBaseConfig(&c, e.base)
		if err != nil {
			return nil, fmt.Errorf("collector %q: %w", c.Name, err)
		}
		if keep {
			result = append(result, c)
		}
	}
	return result, nil
}

// applyBaseConfig checks if a collector is enabled and applies interval overrides.
// Returns (false, nil) if the collector should be skipped, or a non-nil error
// if the configured interval is invalid.
func applyBaseConfig(c *Collector, base collectorconfig.BaseConfig) (bool, error) {
	if !base.IsEnabled() {
		return false, nil
	}
	interval, err := base.IntervalOr(c.Interval)
	if err != nil {
		return false, err
	}
	c.Interval = interval
	return true, nil
}

// StandardCollectors loads CollectorsConfig from viper and builds the
// default catalog collector set for the given pool and PG version.
// Use CollectorsForVersion when you need to mutate the config first.
//
// extraCheckers are the adapter's own capability checks (cloud API
// permissions, ...), run by the capabilities collector after the Postgres one.
func StandardCollectors(pool *pgxpool.Pool, pgVersion pg.Version, logger *logrus.Logger, extraCheckers ...capabilities.CapabilityChecker) ([]Collector, error) {
	cfg, err := CollectorsConfigFromViper(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse collectors config: %w", err)
	}
	return CollectorsForVersion(pool, pgVersion, cfg, logger, extraCheckers...)
}

// CollectorsForVersion builds the catalog collector set with a
// pre-loaded/adjusted CollectorsConfig.
func CollectorsForVersion(pool *pgxpool.Pool, pgVersion pg.Version, cfg CollectorsConfig, logger *logrus.Logger, extraCheckers ...capabilities.CapabilityChecker) ([]Collector, error) {
	return BuildCollectors(pool, pgVersion.Major, cfg, nil, logger, extraCheckers...)
}
