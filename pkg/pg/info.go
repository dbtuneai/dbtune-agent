package pg

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxConnectionsQuery = `
SELECT setting::integer FROM pg_settings WHERE  name = 'max_connections';
`

func MaxConnections(pgPool *pgxpool.Pool) (uint32, error) {
	var maxConnections uint32
	err := QueryRowWithPrefix(pgPool, context.Background(), MaxConnectionsQuery).Scan(&maxConnections)
	if err != nil {
		return 0, fmt.Errorf("error getting max connections: %w", err)
	}

	return maxConnections, nil
}

// CurrentDatabaseQuery returns the name of the database the agent is connected to.
const CurrentDatabaseQuery = `
SELECT current_database();
`

// CurrentDatabase returns the name of the database the agent is connected to
// (i.e. the database being tuned).
func CurrentDatabase(pgPool *pgxpool.Pool) (string, error) {
	var currentDatabase string
	err := QueryRowWithPrefix(pgPool, context.Background(), CurrentDatabaseQuery).Scan(&currentDatabase)
	if err != nil {
		return "", fmt.Errorf("error getting current database: %w", err)
	}

	return currentDatabase, nil
}

// UserDatabaseCountQuery counts the non-template, connectable databases in the
// cluster. A count greater than 1 indicates the tuned database shares the
// cluster with other user databases.
const UserDatabaseCountQuery = `
SELECT count(*)::integer FROM pg_database WHERE NOT datistemplate AND datallowconn;
`

// UserDatabaseCount returns the number of non-template, connectable databases in
// the cluster.
func UserDatabaseCount(pgPool *pgxpool.Pool) (int, error) {
	var count int
	err := QueryRowWithPrefix(pgPool, context.Background(), UserDatabaseCountQuery).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error getting user database count: %w", err)
	}

	return count, nil
}

// DatabaseSystemInfo returns the system-info metrics describing which database
// the agent is tuning (pg_current_database) and how many user databases share
// the cluster (pg_database_count). This lets us tell whether the tuned database
// runs standalone or alongside other databases in the same cluster.
func DatabaseSystemInfo(pgPool *pgxpool.Pool) ([]metrics.FlatValue, error) {
	currentDatabase, err := CurrentDatabase(pgPool)
	if err != nil {
		return nil, err
	}

	databaseCount, err := UserDatabaseCount(pgPool)
	if err != nil {
		return nil, err
	}

	currentDatabaseMetric, err := metrics.PGCurrentDatabase.AsFlatValue(currentDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to create current database metric: %w", err)
	}

	databaseCountMetric, err := metrics.PGDatabaseCount.AsFlatValue(databaseCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create database count metric: %w", err)
	}

	return []metrics.FlatValue{currentDatabaseMetric, databaseCountMetric}, nil
}

const CheckPGStatStatementsQuery = `
SELECT COUNT(*) FROM public.pg_stat_statements(false);
`

func CheckPGStatStatements(pgPool *pgxpool.Pool) error {
	_, err := ExecWithPrefix(pgPool, context.Background(), CheckPGStatStatementsQuery)
	return err
}
