package pg

import (
	"context"
	"time"

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
		{Name: "pg_stats", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStats(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatsPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_user_tables", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatUserTables(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgClass(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatActivity(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatDatabase(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatDatabaseConflicts(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatArchiver(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatBgwriter(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatCheckpointer(g.PGPool, ctx, g.PGMajorVersion)
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
			rows, err := catalog.CollectPgStatWal(g.PGPool, ctx, g.PGMajorVersion)
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
			rows, err := catalog.CollectPgStatIO(g.PGPool, ctx, g.PGMajorVersion)
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
			rows, err := catalog.CollectPgStatReplication(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatReplicationSlots(g.PGPool, ctx, g.PGMajorVersion)
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
			rows, err := catalog.CollectPgStatSlru(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatUserIndexes(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatioUserTables(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatioUserIndexes(g.PGPool, ctx)
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
			rows, err := catalog.CollectPgStatUserFunctions(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatUserFunctionsPayload{Rows: rows}, nil
		}},
		{Name: "pg_locks", Interval: 30 * time.Second, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgLocks(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgLocksPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_progress_vacuum", Interval: 30 * time.Second, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatProgressVacuum(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressVacuumPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_progress_analyze", Interval: 30 * time.Second, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatProgressAnalyze(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressAnalyzePayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_progress_create_index", Interval: 30 * time.Second, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatProgressCreateIndex(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatProgressCreateIndexPayload{Rows: rows}, nil
		}},
		{Name: "pg_prepared_xacts", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgPreparedXacts(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgPreparedXactsPayload{Rows: rows}, nil
		}},
		{Name: "pg_replication_slots", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgReplicationSlots(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgReplicationSlotsPayload{Rows: rows}, nil
		}},
		{Name: "pg_index", Interval: 5 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgIndex(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgIndexPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_wal_receiver", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatWalReceiver(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatWalReceiverPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_recovery_prefetch", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatRecoveryPrefetch(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatRecoveryPrefetchPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_subscription", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatSubscription(g.PGPool, ctx)
			if err != nil {
				return nil, err
			}
			return &agent.PgStatSubscriptionPayload{Rows: rows}, nil
		}},
		{Name: "pg_stat_subscription_stats", Interval: 1 * time.Minute, Collect: func(ctx context.Context) (any, error) {
			ctx, err := g.prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := catalog.CollectPgStatSubscriptionStats(g.PGPool, ctx, g.PGMajorVersion)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				return nil, nil
			}
			return &agent.PgStatSubscriptionStatsPayload{Rows: rows}, nil
		}},
	}
}
