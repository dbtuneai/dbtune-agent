package rds

import (
	"context"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
)

func (adapter *RDSAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Info("Collecting system info")

	if err := adapter.refreshDBInfo(ctx); err != nil {
		return nil, err
	}

	// Get the RDSDB specific info
	info, err := adapter.State.DBInfo.TryIntoFlatValuesSlice()
	if err != nil {
		return nil, err
	}

	// PGVersion
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion.String())
	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL version metric: %v", err)
		return nil, err
	}
	info = append(info, version)

	// MaxConnections
	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)

	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL max connections metric: %v", err)
		return nil, err
	}
	info = append(info, maxConnectionsMetric)

	// Current database and user-database count
	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Errorf("Failed to create database system info metrics: %v", err)
		return nil, err
	}
	info = append(info, databaseInfo...)

	return info, nil
}
