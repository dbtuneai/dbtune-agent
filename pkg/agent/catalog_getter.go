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
	PGPool         *pgxpool.Pool
	PGMajorVersion int
	PGConfig       pg.Config
	PrepareContext func(ctx context.Context) (context.Context, error)
	HealthGate     *pg.HealthGate
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
	collectors := []CatalogCollector{
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
		queries.PgStatsCollector(g.PGPool, p),
		queries.PgStatUserTablesCollector(g.PGPool, p),
		queries.PgClassCollector(g.PGPool, p),
		queries.PgStatActivityCollector(g.PGPool, p),
		queries.PgStatDatabaseCollector(g.PGPool, p),
		queries.PgStatDatabaseConflictsCollector(g.PGPool, p),
		queries.PgStatArchiverCollector(g.PGPool, p),
		queries.PgStatBgwriterCollector(g.PGPool, p),
		queries.PgStatCheckpointerCollector(g.PGPool, p, g.PGMajorVersion),
		queries.PgStatWalCollector(g.PGPool, p, g.PGMajorVersion),
		queries.PgStatIOCollector(g.PGPool, p, g.PGMajorVersion),
		queries.PgStatReplicationCollector(g.PGPool, p),
		queries.PgStatReplicationSlotsCollector(g.PGPool, p, g.PGMajorVersion),
		queries.PgStatSlruCollector(g.PGPool, p),
		queries.PgStatUserIndexesCollector(g.PGPool, p),
		queries.PgStatioUserTablesCollector(g.PGPool, p),
		queries.PgStatioUserIndexesCollector(g.PGPool, p),
		queries.PgStatUserFunctionsCollector(g.PGPool, p),
		queries.PgLocksCollector(g.PGPool, p),
		queries.PgStatProgressVacuumCollector(g.PGPool, p),
		queries.PgStatProgressAnalyzeCollector(g.PGPool, p),
		queries.PgStatProgressCreateIndexCollector(g.PGPool, p),
		queries.PgPreparedXactsCollector(g.PGPool, p),
		queries.PgReplicationSlotsCollector(g.PGPool, p),
		queries.PgIndexCollector(g.PGPool, p),
		queries.PgStatWalReceiverCollector(g.PGPool, p),
		queries.PgStatRecoveryPrefetchCollector(g.PGPool, p, g.PGMajorVersion),
		queries.PgStatSubscriptionCollector(g.PGPool, p),
		queries.PgStatSubscriptionStatsCollector(g.PGPool, p, g.PGMajorVersion),
		queries.WaitEventsCollector(g.PGPool, p),
		queries.AutovacuumCountCollector(g.PGPool, p),
		queries.UptimeMinutesCollector(g.PGPool, p),
		queries.ConnectionStatsCollector(g.PGPool, p),
		queries.DatabaseSizeCollector(g.PGPool, p),
		queries.TransactionCommitsCollector(g.PGPool, p),
		queries.PgStatStatementsCollector(g.PGPool, p, g.PGConfig.IncludeQueries, g.PGConfig.MaximumQueryTextLength, g.PGMajorVersion),
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
