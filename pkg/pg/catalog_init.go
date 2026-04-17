package pg

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StandardCatalogCollectors loads CollectorsConfig from viper and builds the
// default catalog collector set for the given pool and PG version string.
// Use CatalogCollectorsForVersion when you need to mutate the config first.
func StandardCatalogCollectors(pool *pgxpool.Pool, pgVersion string) ([]queries.CatalogCollector, error) {
	cfg, err := CollectorsConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to parse collectors config: %w", err)
	}
	return CatalogCollectorsForVersion(pool, pgVersion, cfg)
}

// CatalogCollectorsForVersion builds the catalog collector set with a
// pre-loaded/adjusted CollectorsConfig.
func CatalogCollectorsForVersion(pool *pgxpool.Pool, pgVersion string, cfg CollectorsConfig) ([]queries.CatalogCollector, error) {
	major, err := PGMajorVersion(pgVersion)
	if err != nil {
		return nil, err
	}
	return BuildCatalogCollectors(pool, major, cfg, nil)
}
