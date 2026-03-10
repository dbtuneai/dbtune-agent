package pg

import (
	"context"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
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

// CatalogCollectors returns all catalog collection tasks.
func (g *CatalogGetter) CatalogCollectors() []agent.CatalogCollector {
	return []agent.CatalogCollector{
		{Name: "pg_statistic", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatistic(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatisticPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_user_tables", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserTables(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserTablePayload{Rows: rows}, nil
		}},
		{Name: "pg_class", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgClass(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgClassPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_activity", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatActivity(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatActivityPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_database", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabase(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatDatabasePayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_database_conflicts", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatDatabaseConflicts(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatDatabaseConflictsPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_archiver", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatArchiver(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatArchiverPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_bgwriter", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatBgwriter(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatBgwriterPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_checkpointer", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatCheckpointer(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatCheckpointerPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_wal", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatWal(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatWalPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_io", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatIO(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatIOPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_replication", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatReplication(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatReplicationPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_replication_slots", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatReplicationSlots(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatReplicationSlotsPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_slru", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatSlru(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatSlruPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_user_indexes", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserIndexes(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserIndexesPayload{Rows: rows}, nil
		}},
		{Name: "pg_statio_user_tables", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserTables(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatioUserTablesPayload{Rows: rows}, nil
		}},
		{Name: "pg_statio_user_indexes", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatioUserIndexes(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatioUserIndexesPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_user_functions", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectPgStatUserFunctions(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserFunctionsPayload{Rows: rows}, nil
		}},
	}
}
