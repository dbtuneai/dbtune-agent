package pg

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PostgreSQLRoleQuery = `
/*dbtune*/
SELECT DISTINCT
    CASE
        WHEN b.sender=0 AND c.receiver=0 THEN
            'Standalone'
        WHEN b.sender>0 AND c.receiver=0 THEN
            'Primary'
        WHEN b.sender=0 AND c.receiver>0 THEN
            'Replica'
        WHEN b.sender>0 AND c.receiver>0 THEN
            'Primary+Replica'
    END AS pgrole
FROM
    pg_database a,
    (
        SELECT COUNT(*) AS sender
        FROM pg_stat_replication
    ) b,
    (
        SELECT COUNT(*) AS receiver
        FROM pg_stat_wal_receiver
    ) c
WHERE
    NOT a.datistemplate;
`

// PostgreSQLRole collects the role of the PostgreSQL instance.
// Possible values: 'Standalone', 'Primary', 'Replica', or 'Primary+Replica' (cascading replication).
// This is useful for high-availability setups where instances can failover between roles.
// Works with all PostgreSQL adapters: CNPG, Docker with streaming replication, RDS read replicas, etc.
func PostgreSQLRole(pgPool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		var role string
		err := pgPool.QueryRow(ctx, PostgreSQLRoleQuery).Scan(&role)
		if err != nil {
			return fmt.Errorf("error getting PostgreSQL role: %v", err)
		}

		err = EmitMetric(state, metrics.PGRole, role)
		if err != nil {
			return fmt.Errorf("error emitting PostgreSQL role metric: %v", err)
		}

		return nil
	}
}
