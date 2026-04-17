package pg

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

// noopPrepareCtx is a PrepareCtx that passes through unchanged.
func noopPrepareCtx(ctx context.Context) (context.Context, error) { return ctx, nil }

// BuildCatalogCollectors creates the full set of catalog collectors, applying
// per-collector config from CollectorsConfig (enabled filter, interval overrides).
// A nil pool is allowed for construction-only callers (e.g. tests); the pointer
// is only dereferenced when a collector's Collect is invoked.
func BuildCatalogCollectors(
	pool *pgxpool.Pool,
	pgMajorVersion int,
	cfg CollectorsConfig,
	prepareCtx queries.PrepareCtx,
) ([]queries.CatalogCollector, error) {
	if prepareCtx == nil {
		prepareCtx = noopPrepareCtx
	}

	type entry struct {
		collector queries.CatalogCollector
		base      collectorconfig.BaseConfig
	}

	simpleBase := func(name string) collectorconfig.BaseConfig {
		if b, ok := cfg.Simple[name]; ok {
			return b
		}
		return collectorconfig.BaseConfig{}
	}

	entries := []entry{
		// Simple collectors
		{queries.AutovacuumCountCollector(pool, prepareCtx), simpleBase(queries.AutovacuumCountName)},
		{queries.ConnectionStatsCollector(pool, prepareCtx), simpleBase(queries.ConnectionStatsName)},
		{queries.DDLCollector(pool, prepareCtx), simpleBase(queries.DDLCollectorName)},
		{queries.DatabaseSizeCollector(pool, prepareCtx), simpleBase(queries.DatabaseSizeName)},
		{queries.PgAttributeCollector(pool, prepareCtx), simpleBase(queries.PgAttributeName)},
		{queries.PgConstraintCollector(pool, prepareCtx), simpleBase(queries.PgConstraintName)},
		{queries.PgDatabaseCollector(pool, prepareCtx), simpleBase(queries.PgDatabaseName)},
		{queries.PgIndexCollector(pool, prepareCtx), simpleBase(queries.PgIndexName)},
		{queries.PgLocksCollector(pool, prepareCtx), simpleBase(queries.PgLocksName)},
		{queries.PgPreparedXactsCollector(pool, prepareCtx), simpleBase(queries.PgPreparedXactsName)},
		{queries.PgReplicationSlotsCollector(pool, prepareCtx), simpleBase(queries.PgReplicationSlotsName)},
		{queries.PgTypeCollector(pool, prepareCtx), simpleBase(queries.PgTypeName)},
		{queries.PgStatActivityCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatActivityName)},
		{queries.PgStatArchiverCollector(pool, prepareCtx), simpleBase(queries.PgStatArchiverName)},
		{queries.PgStatBgwriterCollector(pool, prepareCtx), simpleBase(queries.PgStatBgwriterName)},
		{queries.PgStatCheckpointerCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatCheckpointerName)},
		{queries.PgStatDatabaseCollector(pool, prepareCtx), simpleBase(queries.PgStatDatabaseName)},
		{queries.PgStatDatabaseConflictsCollector(pool, prepareCtx), simpleBase(queries.PgStatDatabaseConflictsName)},
		{queries.PgStatIOCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatIOName)},
		{queries.PgStatProgressAnalyzeCollector(pool, prepareCtx), simpleBase(queries.PgStatProgressAnalyzeName)},
		{queries.PgStatProgressCreateIndexCollector(pool, prepareCtx), simpleBase(queries.PgStatProgressCreateIndexName)},
		{queries.PgStatProgressVacuumCollector(pool, prepareCtx), simpleBase(queries.PgStatProgressVacuumName)},
		{queries.PgStatRecoveryPrefetchCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatRecoveryPrefetchName)},
		{queries.PgStatReplicationCollector(pool, prepareCtx), simpleBase(queries.PgStatReplicationName)},
		{queries.PgStatReplicationSlotsCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatReplicationSlotsName)},
		{queries.PgStatSlruCollector(pool, prepareCtx), simpleBase(queries.PgStatSlruName)},
		{queries.PgStatSubscriptionCollector(pool, prepareCtx), simpleBase(queries.PgStatSubscriptionName)},
		{queries.PgStatSubscriptionStatsCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatSubscriptionStatsName)},
		{queries.PgStatUserFunctionsCollector(pool, prepareCtx), simpleBase(queries.PgStatUserFunctionsName)},
		{queries.PgStatWalCollector(pool, prepareCtx, pgMajorVersion), simpleBase(queries.PgStatWalName)},
		{queries.PgStatWalReceiverCollector(pool, prepareCtx), simpleBase(queries.PgStatWalReceiverName)},
		{queries.PgStatioUserTablesCollector(pool, prepareCtx), simpleBase(queries.PgStatioUserTablesName)},
		{queries.TransactionCommitsCollector(pool, prepareCtx), simpleBase(queries.TransactionCommitsName)},
		{queries.UptimeMinutesCollector(pool, prepareCtx), simpleBase(queries.UptimeMinutesName)},
		{queries.WaitEventsCollector(pool, prepareCtx), simpleBase(queries.WaitEventsName)},

		// Typed collectors
		{queries.PgStatStatementsCollector(pool, prepareCtx, cfg.PgStatStatements.Extra, pgMajorVersion), cfg.PgStatStatements.Base},
		{queries.PgClassCollector(pool, prepareCtx, cfg.PgClass.Extra), cfg.PgClass.Base},
		{queries.PgStatsCollector(pool, prepareCtx, cfg.PgStats.Extra), cfg.PgStats.Base},
		{queries.PgStatUserTablesCollector(pool, prepareCtx, cfg.PgStatUserTables.Extra), cfg.PgStatUserTables.Base},
		{queries.PgStatUserIndexesCollector(pool, prepareCtx, cfg.PgStatUserIndexes.Extra), cfg.PgStatUserIndexes.Base},
		{queries.PgStatioUserIndexesCollector(pool, prepareCtx, cfg.PgStatioUserIndexes.Extra), cfg.PgStatioUserIndexes.Base},
	}

	result := make([]queries.CatalogCollector, 0, len(entries))
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
func applyBaseConfig(c *queries.CatalogCollector, base collectorconfig.BaseConfig) (bool, error) {
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
