package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg/catalog"
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
	PrepareContext func(ctx context.Context) (context.Context, error)
	HealthGate     *HealthGate
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

func (g *CatalogGetter) GetDDL(ctx context.Context) (*agent.DDLPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	ddl, err := CollectDDL(g.PGPool, ctx)
	if err != nil {
		if g.HealthGate != nil {
			g.HealthGate.ReportError(err)
		}
		return nil, err
	}
	return &agent.DDLPayload{DDL: ddl, Hash: HashDDL(ddl)}, nil
}

// CatalogCollectors returns all catalog collection tasks.
func (g *CatalogGetter) CatalogCollectors() []agent.CatalogCollector {
	p := catalog.PrepareCtx(g.prepareCtx)

	collectors := []agent.CatalogCollector{
		catalog.NewPgStatsCollector(g.PGPool, p),
		catalog.NewPgStatUserTablesCollector(g.PGPool, p),
		catalog.NewPgClassCollector(g.PGPool, p),
		catalog.NewPgStatActivityCollector(g.PGPool, p),
		catalog.NewPgStatDatabaseCollector(g.PGPool, p),
		catalog.NewPgStatDatabaseConflictsCollector(g.PGPool, p),
		catalog.NewPgStatArchiverCollector(g.PGPool, p),
		catalog.NewPgStatBgwriterCollector(g.PGPool, p),
		catalog.NewPgStatCheckpointerCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewPgStatWalCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewPgStatIOCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewPgStatReplicationCollector(g.PGPool, p),
		catalog.NewPgStatReplicationSlotsCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewPgStatSlruCollector(g.PGPool, p),
		catalog.NewPgStatUserIndexesCollector(g.PGPool, p),
		catalog.NewPgStatioUserTablesCollector(g.PGPool, p),
		catalog.NewPgStatioUserIndexesCollector(g.PGPool, p),
		catalog.NewPgStatUserFunctionsCollector(g.PGPool, p),
		catalog.NewPgLocksCollector(g.PGPool, p),
		catalog.NewPgStatProgressVacuumCollector(g.PGPool, p),
		catalog.NewPgStatProgressAnalyzeCollector(g.PGPool, p),
		catalog.NewPgStatProgressCreateIndexCollector(g.PGPool, p),
		catalog.NewPgPreparedXactsCollector(g.PGPool, p),
		catalog.NewPgReplicationSlotsCollector(g.PGPool, p),
		catalog.NewPgIndexCollector(g.PGPool, p),
		catalog.NewPgStatWalReceiverCollector(g.PGPool, p),
		catalog.NewPgStatRecoveryPrefetchCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewPgStatSubscriptionCollector(g.PGPool, p),
		catalog.NewPgStatSubscriptionStatsCollector(g.PGPool, p, g.PGMajorVersion),
		catalog.NewWaitEventsCollector(g.PGPool, p),
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
