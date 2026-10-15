package pg

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// StandardCatalogCollectors loads CollectorsConfig from viper and builds the
// default catalog collector set for the given pool and PG version.
// Use CatalogCollectorsForVersion when you need to mutate the config first.
func StandardCatalogCollectors(pool *pgxpool.Pool, pgVersion Version, logger *logrus.Logger) ([]queries.CatalogCollector, error) {
	cfg, err := CollectorsConfigFromViper(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse collectors config: %w", err)
	}
	return CatalogCollectorsForVersion(pool, pgVersion, cfg, logger)
}

// CatalogCollectorsForVersion builds the catalog collector set with a
// pre-loaded/adjusted CollectorsConfig.
func CatalogCollectorsForVersion(pool *pgxpool.Pool, pgVersion Version, cfg CollectorsConfig, logger *logrus.Logger) ([]queries.CatalogCollector, error) {
	return BuildCatalogCollectors(pool, pgVersion.Major, cfg, nil, logger)
}
