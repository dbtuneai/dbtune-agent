package pg

import (
	"context"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CatalogGetter provides all the Get* methods for pg catalog views.
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
}

func (g *CatalogGetter) prepareCtx(ctx context.Context) (context.Context, error) {
	if g.PrepareContext != nil {
		return g.PrepareContext(ctx)
	}
	return ctx, nil
}

func (g *CatalogGetter) GetDDL(ctx context.Context) (*agent.DDLPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	ddl, err := CollectDDL(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.DDLPayload{DDL: ddl, Hash: HashDDL(ddl)}, nil
}

func (g *CatalogGetter) GetPgStatistic(ctx context.Context) (*agent.PgStatisticPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatistic(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatisticPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatUserTables(ctx context.Context) (*agent.PgStatUserTablePayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatUserTables(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatUserTablePayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgClass(ctx context.Context) (*agent.PgClassPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgClass(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgClassPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatActivity(ctx context.Context) (*agent.PgStatActivityPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatActivity(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatActivityPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatDatabaseAll(ctx context.Context) (*agent.PgStatDatabasePayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatDatabase(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatDatabasePayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatDatabaseConflicts(ctx context.Context) (*agent.PgStatDatabaseConflictsPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatDatabaseConflicts(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatDatabaseConflictsPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatArchiver(ctx context.Context) (*agent.PgStatArchiverPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatArchiver(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatArchiverPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatBgwriterAll(ctx context.Context) (*agent.PgStatBgwriterPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatBgwriter(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatBgwriterPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatCheckpointerAll(ctx context.Context) (*agent.PgStatCheckpointerPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatCheckpointer(g.PGPool, ctx, g.PGMajorVersion)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatCheckpointerPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatWalAll(ctx context.Context) (*agent.PgStatWalPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatWal(g.PGPool, ctx, g.PGMajorVersion)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatWalPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatIO(ctx context.Context) (*agent.PgStatIOPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatIO(g.PGPool, ctx, g.PGMajorVersion)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatIOPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatReplication(ctx context.Context) (*agent.PgStatReplicationPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatReplication(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatReplicationPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatReplicationSlots(ctx context.Context) (*agent.PgStatReplicationSlotsPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatReplicationSlots(g.PGPool, ctx, g.PGMajorVersion)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatReplicationSlotsPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatSlru(ctx context.Context) (*agent.PgStatSlruPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatSlru(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatSlruPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatUserIndexes(ctx context.Context) (*agent.PgStatUserIndexesPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatUserIndexes(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatUserIndexesPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatioUserTables(ctx context.Context) (*agent.PgStatioUserTablesPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatioUserTables(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatioUserTablesPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatioUserIndexes(ctx context.Context) (*agent.PgStatioUserIndexesPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatioUserIndexes(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatioUserIndexesPayload{Rows: rows}, nil
}

func (g *CatalogGetter) GetPgStatUserFunctions(ctx context.Context) (*agent.PgStatUserFunctionsPayload, error) {
	ctx, err := g.prepareCtx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := CollectPgStatUserFunctions(g.PGPool, ctx)
	if err != nil {
		return nil, err
	}
	return &agent.PgStatUserFunctionsPayload{Rows: rows}, nil
}
