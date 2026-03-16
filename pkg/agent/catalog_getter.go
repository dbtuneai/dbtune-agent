package agent

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CatalogGetter provides all the catalog collection methods for pg catalog views.
// Embed this in adapter structs to satisfy the corresponding AgentLooper methods.
// Note: GetActiveConfig is NOT included because it requires a logger;
// adapters should implement it directly.
//
// PrepareContext is an optional hook called before each catalog query. It allows
// adapters (e.g. CNPG, Patroni) to perform failover checks and replace the
// context with an operations-scoped context. If nil, the original context is used.
type CatalogGetter struct {
	PGPool           *pgxpool.Pool
	PGMajorVersion   int
	PGConfig         pg.Config
	CollectorsConfig pg.CollectorsConfig
	PrepareContext   func(ctx context.Context) (context.Context, error)
	HealthGate       *pg.HealthGate
}

func (g *CatalogGetter) prepareCtx(ctx context.Context) (context.Context, error) {
	if g.HealthGate != nil {
		if err := g.HealthGate.Check(); err != nil {
			return nil, err
		}
	}
	if g.PrepareContext != nil {
		return g.PrepareContext(ctx)
	}
	return ctx, nil
}

// collectorCfg returns the override config for a named collector.
func (g *CatalogGetter) collectorCfg(name string) pg.CollectorOverride {
	if g.CollectorsConfig == nil {
		return pg.CollectorOverride{}
	}
	return g.CollectorsConfig[name]
}

// StartHealthGate starts the health gate's ping goroutine lifecycle.
// Adapters inherit this via embedding, so no per-adapter method is needed.
func (g *CatalogGetter) StartHealthGate(ctx context.Context) {
	if g.HealthGate != nil {
		g.HealthGate.Start(ctx)
	}
}

// CatalogCollectors returns all catalog collection tasks.
func (g *CatalogGetter) CatalogCollectors() []CatalogCollector {
	p := queries.PrepareCtx(g.prepareCtx)
	pool := g.PGPool

	// Resolve per-collector params for collectors with extra config.
	stmtCfg := g.collectorCfg(queries.PgStatStatementsName)
	includeQueries := pg.BoolOr(stmtCfg.IncludeQueries, g.PGConfig.IncludeQueries)
	maxQueryLen := pg.IntOr(stmtCfg.MaxQueryTextLength, g.PGConfig.MaximumQueryTextLength)
	if maxQueryLen > pg.MaxQueryTextLength {
		maxQueryLen = pg.MaxQueryTextLength
	}
	diffLimit := pg.IntOr(stmtCfg.DiffLimit, queries.PgStatStatementsDiffLimit)

	statsCfg := g.collectorCfg(queries.PgStatsName)
	statsBatchSize := pg.IntOr(statsCfg.BackfillBatchSize, queries.PgStatsBackfillBatchSize)
	includeTableData := pg.BoolOr(statsCfg.IncludeTableData, true)

	classCfg := g.collectorCfg(queries.PgClassName)
	classBatchSize := pg.IntOr(classCfg.BackfillBatchSize, queries.PgClassBackfillBatchSize)

	userTablesCfg := g.collectorCfg(queries.PgStatUserTablesName)
	userTablesCatLimit := pg.IntOr(userTablesCfg.CategoryLimit, queries.PgStatUserTablesCategoryLimit)

	userIndexesCfg := g.collectorCfg(queries.PgStatUserIndexesName)
	userIndexesCatLimit := pg.IntOr(userIndexesCfg.CategoryLimit, queries.PgStatUserIndexesCategoryLimit)

	statioIndexesCfg := g.collectorCfg(queries.PgStatioUserIndexesName)
	statioIndexesCatLimit := pg.IntOr(statioIndexesCfg.CategoryLimit, queries.PgStatioUserIndexesCategoryLimit)

	allCollectors := []CatalogCollector{
		{
			Name:          "ddl",
			Interval:      1 * time.Minute,
			SkipUnchanged: true,
			Collect: func(ctx context.Context) (any, error) {
				ctx, err := p(ctx)
				if err != nil {
					return nil, err
				}
				ddl, err := queries.CollectDDL(pool, ctx)
				if err != nil {
					return nil, err
				}
				return &DDLPayload{DDL: ddl, Hash: queries.HashDDL(ddl)}, nil
			},
		},
		queries.PgStatsCollector(pool, p, statsBatchSize, includeTableData),
		queries.PgStatUserTablesCollector(pool, p, userTablesCatLimit),
		queries.PgClassCollector(pool, p, classBatchSize),
		queries.PgStatActivityCollector(pool, p),
		queries.PgStatDatabaseCollector(pool, p),
		queries.PgStatDatabaseConflictsCollector(pool, p),
		queries.PgStatArchiverCollector(pool, p),
		queries.PgStatBgwriterCollector(pool, p),
		queries.PgStatCheckpointerCollector(pool, p, g.PGMajorVersion),
		queries.PgStatWalCollector(pool, p, g.PGMajorVersion),
		queries.PgStatIOCollector(pool, p, g.PGMajorVersion),
		queries.PgStatReplicationCollector(pool, p),
		queries.PgStatReplicationSlotsCollector(pool, p, g.PGMajorVersion),
		queries.PgStatSlruCollector(pool, p),
		queries.PgStatUserIndexesCollector(pool, p, userIndexesCatLimit),
		queries.PgStatioUserTablesCollector(pool, p),
		queries.PgStatioUserIndexesCollector(pool, p, statioIndexesCatLimit),
		queries.PgStatUserFunctionsCollector(pool, p),
		queries.PgLocksCollector(pool, p),
		queries.PgStatProgressVacuumCollector(pool, p),
		queries.PgStatProgressAnalyzeCollector(pool, p),
		queries.PgStatProgressCreateIndexCollector(pool, p),
		queries.PgPreparedXactsCollector(pool, p),
		queries.PgReplicationSlotsCollector(pool, p),
		queries.PgIndexCollector(pool, p),
		queries.PgStatWalReceiverCollector(pool, p),
		queries.PgStatRecoveryPrefetchCollector(pool, p, g.PGMajorVersion),
		queries.PgStatSubscriptionCollector(pool, p),
		queries.PgStatSubscriptionStatsCollector(pool, p, g.PGMajorVersion),
		queries.WaitEventsCollector(pool, p),
		queries.AutovacuumCountCollector(pool, p),
		queries.UptimeMinutesCollector(pool, p),
		queries.ConnectionStatsCollector(pool, p),
		queries.DatabaseSizeCollector(pool, p),
		queries.TransactionCommitsCollector(pool, p),
		queries.PgStatStatementsCollector(pool, p, includeQueries, maxQueryLen, diffLimit, g.PGMajorVersion),
	}

	// Post-construction: apply interval overrides + enabled filter.
	// IntervalOr enforces floor: configured value is clamped to >= compiled default.
	// Users can only make collection LESS frequent, never more frequent.
	collectors := make([]CatalogCollector, 0, len(allCollectors))
	for _, c := range allCollectors {
		cfg := g.collectorCfg(c.Name)
		if !cfg.IsEnabled() {
			continue
		}
		c.Interval = cfg.IntervalOr(c.Interval)
		collectors = append(collectors, c)
	}

	// Wrap each collector's Collect func to report errors to the health gate.
	if g.HealthGate != nil {
		for i := range collectors {
			orig := collectors[i].Collect
			collectors[i].Collect = func(ctx context.Context) (any, error) {
				data, err := orig(ctx)
				if err != nil {
					g.HealthGate.ReportError(err)
				}
				return data, err
			}
		}
	}

	return collectors
}
